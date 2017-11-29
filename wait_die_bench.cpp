#include "thread_util.hpp"
#include "random.hpp"
#include <unistd.h>
#include "cpuid.hpp"
#include "measure_util.hpp"
#include "arch.hpp"

#include "wait_die.hpp"
#include "tx_util.hpp"

using Lock = cybozu::wait_die::WaitDieLock;
using Mutex = Lock::Mutex;
using Mode = Lock::Mode;

const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);

EpochGenerator epochGen_;


struct Shared
{
    std::vector<Mutex> muV;
    size_t longTxSize;
    size_t nrOp;
    size_t nrWr;
    int shortTxMode;
    int longTxMode;
    bool usesBackOff;

    GlobalTxIdGenerator globalTxIdGen;
    SimpleTxIdGenerator simpleTxIdGen;

    Shared() : globalTxIdGen(5, 10) {}
    //Shared() : globalTxIdGen(7, 5) {}
};


template <int txIdGenType>
Result worker2(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, Shared& shared)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    std::vector<Mutex>& muV = shared.muV;
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t nrWr = shared.nrWr;
    const int shortTxMode = shared.shortTxMode;
    const int longTxMode = shared.longTxMode;

    Result res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    cybozu::wait_die::LockSet lockSet;
    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    PriorityIdGenerator<12> priIdGen;
    priIdGen.init(idx + 1);
    TxIdGenerator localTxIdGen(&shared.globalTxIdGen);

    // USE_MIX_TX
    std::vector<bool> isWriteV(nrOp);
    std::vector<size_t> tmpV2; // for fillModeVec.

    // USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);


    const bool isLongTx = longTxSize != 0 && idx == 0; // starvation setting.
    const size_t realNrOp = isLongTx ? longTxSize : nrOp;
    if (!isLongTx && shortTxMode == USE_MIX_TX) {
        isWriteV.resize(nrOp);
    }
#if 0
    GetModeFunc<decltype(rand), Mode>
        getMode(boolRand, isWriteV, isLongTx,
                shortTxMode, longTxMode, realNrOp, nrWr);
#endif

    while (!start) _mm_pause();
    size_t count = 0; unused(count);
    while (!quit) {
        if (!isLongTx && shortTxMode == USE_MIX_TX) {
            fillModeVec(isWriteV, rand, nrWr, tmpV2);
        }

        uint64_t txId;
        if (txIdGenType == SCALABLE_TXID_GEN) {
            txId = priIdGen.get(isLongTx ? 0 : 1);
        } else if (txIdGenType == BULK_TXID_GEN) {
            txId = localTxIdGen.get();
        } else if (txIdGenType == SIMPLE_TXID_GEN) {
            txId = shared.simpleTxIdGen.get();
        } else {
            throw cybozu::Exception("bad txIdGenType") << txIdGenType;
        }
        lockSet.setTxId(txId);
        size_t firstRecIdx;
        uint64_t t0;
        if (shared.usesBackOff) t0 = cybozu::time::rdtscp();
        auto randState = rand.getState();
        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            assert(lockSet.empty());
            bool abort = false;
            rand.setState(randState);
            for (size_t i = 0; i < realNrOp; i++) {
#if 0
                Mode mode = getMode(i);
#else
                Mode mode = getMode<decltype(rand), Mode>(
                    boolRand, isWriteV, isLongTx, shortTxMode, longTxMode,
                    realNrOp, nrWr, i);
#endif
                size_t key = getRecordIdx(
                    rand, isLongTx, shortTxMode, longTxMode,
                    muV.size(), realNrOp, i, firstRecIdx);
                Mutex& mutex = muV[key];
                if (!lockSet.lock(mutex, mode)) {
                    abort = true;
                    break;
                }
            }
            if (abort) {
                lockSet.clear();
                res.incAbort(isLongTx);
                if (shared.usesBackOff) backOff(t0, retry, rand);
                continue;
            }

            res.incCommit(isLongTx);
            lockSet.clear();
            res.addRetryCount(isLongTx, retry);
            break; // retry is not required.
        }
    }
    return res;
}


struct Result2
{
    struct Data {
        size_t txSize;

        size_t nrCommit;
        size_t nrAbort;

        Data() : nrCommit(0), nrAbort(0) {
        }

        void operator+=(const Data& rhs) {
            nrCommit += rhs.nrCommit;
            nrAbort += rhs.nrAbort;
        }
    };

    using Umap = std::unordered_map<size_t, Data>;
    Umap umap_;  // key: txSize

    void incCommit(size_t txSize) {
        umap_[txSize].nrCommit++;
    }

    void incAbort(size_t txSize) {
        umap_[txSize].nrAbort++;
    }

    void addRetryCount(size_t txSize, size_t nrRetry) {
        unused(txSize);
        unused(nrRetry);
        // not implemented yet.
    }

    size_t nrCommit() const {
        size_t total = 0;
        for (const Umap::value_type &p : umap_) {
            total += p.second.nrCommit;
        }
        return total;
    }

    void operator+=(const Result2& res) {
        for (const Umap::value_type &p : res.umap_) {
            umap_[p.first] += p.second;
        }
    }

    std::string str() const {
        std::vector<Data> v;
        v.reserve(umap_.size());
        for (const Umap::value_type &p : umap_) {
            v.push_back(p.second);
            v.back().txSize = p.first;
        }
        std::sort(v.begin(), v.end(), [](const Data &a, const Data &b) {
                return a.txSize < b.txSize;
            });

        std::stringstream ss;
        for (const Data& d : v) {
            ss << " " << "nrCommit_" << d.txSize << ":" << d.nrCommit;
            ss << " " << "nrAbort_" << d.txSize << ":" << d.nrAbort;
        }
        return ss.str();
    }
};


/**
 * Long transactions with several transaction sizes.
 */
Result2 worker3(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, Shared& shared)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    std::vector<Mutex>& muV = shared.muV;

    const size_t txSize = [&]() -> size_t {
        if (idx == 0) {
            return std::max<size_t>(muV.size() / 2, 10);
        } else if (idx <= 5) {
            return std::max<size_t>(muV.size() / 10, 10);
        } else {
            return 10;
        }
    }();

    Result2 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    cybozu::wait_die::LockSet lockSet;
    std::vector<size_t> tmpV; // for fillMuIdVecArray.

#if 0
    TxIdGenerator localTxIdGen(&shared.globalTxIdGen);
#else
    EpochTxIdGenerator<8, 2> epochTxIdGen(idx + 1, epochGen_);
#if 1
    if (idx == 0) {
        epochTxIdGen.setOrderId(0);
    } else if (idx <= 5) {
        epochTxIdGen.setOrderId(1);
    } else {
        epochTxIdGen.setOrderId(3);
    }
#endif
#if 0
    if (idx == 0) {
        epochTxIdGen.boost(20);
    } else if (idx <= 5) {
        epochTxIdGen.boost(10);
    }
#endif
#endif

    BoolRandom<decltype(rand)> boolRand(rand);

    const size_t realNrOp = txSize;

    while (!start) _mm_pause();
    size_t count = 0; unused(count);
    while (!quit) {
#if 0
        const uint64_t txId = localTxIdGen.get();
#else
        const uint64_t txId = epochTxIdGen.get();
#endif
        lockSet.setTxId(txId);
        uint64_t t0;
        if (shared.usesBackOff) t0 = cybozu::time::rdtscp();
        auto randState = rand.getState();
        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            assert(lockSet.empty());
            bool abort = false;
            rand.setState(randState);
            boolRand.reset();
            for (size_t i = 0; i < realNrOp; i++) {
                Mode mode = boolRand() ? Mode::X : Mode::S;
                const size_t key = rand() % muV.size();
                Mutex& mutex = muV[key];
                if (!lockSet.lock(mutex, mode)) {
                    abort = true;
                    break;
                }
            }
            if (abort) {
                lockSet.clear();
                res.incAbort(txSize);
                if (shared.usesBackOff) backOff(t0, retry, rand);
                continue;
            }

            res.incCommit(txSize);
            lockSet.clear();
            res.addRetryCount(txSize, retry);
            break; // retry is not required.
        }
    }
    return res;
}


void runTest()
{
#if 0
    //for (size_t nrResPerTh : {40}) {
    //for (size_t nrResPerTh : {4, 4000}) {
    for (size_t nrResPerTh : {4000}) {
        for (size_t nrTh : {32}) {
            //for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
            if (nrTh > 2 && nrTh % 2 != 0) continue;
            for (size_t i = 0; i < 10; i++) {
                bool verbose = false;
                runExec(nrResPerTh * nrTh, nrTh, 10, verbose, 0, 4, 2);
                //sleepMs(1000);
            }
        }
    }
#endif
#if 0
    // high-contention expr.
    for (size_t nrMutex : {40}) {
        //for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
        for (size_t nrTh : {8, 16, 24, 32}) {
            if (nrTh > 16 && nrTh % 2 != 0) continue;
            const size_t nrOp = 10;
            for (size_t nrWr = 0; nrWr <= nrOp; nrWr++) {
                for (size_t i = 0; i < 10; i++) {
                    bool verbose = false;
                    runExec(nrMutex, nrTh, 10, verbose, 0, nrOp, nrWr);
                }
            }
        }
    }
#endif
#if 0
    runExec(5000, 8, 10, true);
#endif
#if 0
    // starvation expr.
    //const size_t nrMutex = 400 * 1000 * 1000;
    //const size_t nrTh = 16;
    const size_t nrMutex = 40 * 1000;
    const size_t nrTh = 8;
    //for (size_t longTxPct : {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60}) {
    for (size_t longTxPml : {1, 2, 3, 4, 5, 6, 7, 8, 9,
                10, 20, 30, 40, 50, 60, 70, 80, 90,
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}) {
        const size_t longTxSize = longTxPml * nrMutex / 1000;
        for (size_t i = 0; i < 10; i++) {
            bool verbose = false;
            //size_t maxSec = longTxPml >= 50 ? 50000 : 100;
            size_t maxSec = 100;
            runExec(nrMutex, nrTh, maxSec, verbose, longTxSize);
            sleepMs(1000);
        }
    }
#endif
}

struct CmdLineOptionPlus : CmdLineOption
{
    using base = CmdLineOption;

    int txIdGenType;
    int usesBackOff; // 0 or 1.

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendOpt(&txIdGenType, 0, "txid-gen", "[id]: txid gen method (0:sclable, 1:bulk, 2:simple)");
        appendOpt(&usesBackOff, 0, "backoff", "[0 or 1]: backoff 0:off 1:on");
    }
    std::string str() const {
        return cybozu::util::formatString(
            "mode:wait-die %s txidGenType:%d backoff:%d"
            , base::str().c_str(), txIdGenType, usesBackOff ? 1 : 0);
    }
};


void dispatch1(CmdLineOptionPlus& opt, Shared& shared)
{
    Result res;
    switch (opt.txIdGenType) {
    case SCALABLE_TXID_GEN:
        runExec(opt, shared, worker2<SCALABLE_TXID_GEN>, res);
        break;
    case BULK_TXID_GEN:
        runExec(opt, shared, worker2<BULK_TXID_GEN>, res);
        break;
    case SIMPLE_TXID_GEN:
        runExec(opt, shared, worker2<SIMPLE_TXID_GEN>, res);
        break;
    default:
        throw cybozu::Exception("bad txIdGenType") << opt.txIdGenType;
    }
}


int main(int argc, char *argv[]) try
{
    CmdLineOptionPlus opt("wait_die_bench: benchmark with wait-die lock.");
    opt.parse(argc, argv);

    if (opt.workload == "custom") {
        Shared shared;
        shared.muV.resize(opt.getNrMu());
        shared.longTxSize = opt.longTxSize;
        shared.nrOp = opt.nrOp;
        shared.nrWr = opt.nrWr;
        shared.shortTxMode = opt.shortTxMode;
        shared.longTxMode = opt.longTxMode;
        shared.usesBackOff = opt.usesBackOff ? 1 : 0;
        for (size_t i = 0; i < opt.nrLoop; i++) {
            dispatch1(opt, shared);
        }
    } else if (opt.workload == "custom3") {
        Shared shared;
        shared.muV.resize(opt.getNrMu());
        for (size_t i = 0; i < opt.nrLoop; i++) {
            Result2 res;
            runExec(opt, shared, worker3, res);
        }
    } else {
        throw cybozu::Exception("bad workload.") << opt.workload;
    }
} catch (std::exception& e) {
    ::fprintf(::stderr, "exeption: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
