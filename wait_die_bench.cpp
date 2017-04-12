#include "thread_util.hpp"
#include "random.hpp"
#include <immintrin.h>
#include <unistd.h>
#include "cpuid.hpp"
#include "measure_util.hpp"

#include "wait_die.hpp"
#include "tx_util.hpp"

using Lock = cybozu::wait_die::WaitDieLock;
using Mutex = Lock::Mutex;
using Mode = Lock::Mode;

const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);


struct Shared
{
    std::string workload;
    int shortMode;
    int txIdGenType;
    size_t nrMuPerTh;
    std::vector<Mutex> muV;
    size_t longTxSize;
    size_t nrOp;
    size_t nrWr;

    GlobalTxIdGenerator globalTxIdGen;
    SimpleTxIdGenerator simpleTxIdGen;

    Shared() : globalTxIdGen(5, 10) {}

    std::string str() const {
        return cybozu::util::formatString(
            "mode:%s workload:%s longTxSize:%zu nrMutex:%zu nrMutexPerTh:%zu "
            "nrOp:%zu nrWr:%zu shortMode:%d txidGenType:%d"
            , "wait-die", workload.c_str(), longTxSize, muV.size(), nrMuPerTh
            , nrOp, nrWr, shortMode, txIdGenType);
    }
};


template <int shortMode, int txIdGenType>
Result worker(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, Shared& shared)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    const size_t nrOp = shared.nrOp;
    const size_t nrWr = shared.nrWr;
    std::vector<Mutex>& muV = shared.muV;
    const size_t longTxSize = shared.longTxSize;

    Result res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    std::vector<size_t> muIdV(nrOp);
    std::vector<Lock> lockV;
    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    PriorityIdGenerator<12> priIdGen;
    priIdGen.init(idx + 1);
    TxIdGenerator localTxIdGen(&shared.globalTxIdGen);

    // USE_MIX_TX
    std::vector<bool> isWriteV(nrOp);
    std::vector<size_t> tmpV2; // for fillModeVec.

    // USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);

    while (!start) _mm_pause();
    size_t count = 0; unused(count);
    while (!quit) {
        const bool isLongTx = longTxSize != 0 && idx == 0; // starvation setting.
        if (isLongTx) {
            muIdV.resize(longTxSize);
            if (longTxSize > muV.size() * 5 / 1000) { // over 0.5%
                fillMuIdVecArray(muIdV, rand, muV.size(), tmpV);
            } else {
                fillMuIdVecLoop(muIdV, rand, muV.size());
            }
        } else {
            muIdV.resize(nrOp);
            fillMuIdVecLoop(muIdV, rand, muV.size());
            if (shortMode == USE_MIX_TX) {
                isWriteV.resize(nrOp);
                fillModeVec(isWriteV, rand, nrWr, tmpV2);
            }
        }

        // Do not sort with noWait mode.
        //std::sort(muIdV.begin(), muIdV.end());

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

        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            assert(lockV.empty());
            bool abort = false;
            const size_t sz = muIdV.size();
            unused(sz);
            for (size_t i = 0; i < muIdV.size(); i++) {
                //Mode mode = (rand() % 2 == 0 ? Mode::X : Mode::S);
                Mode mode;
                if (shortMode == USE_LONG_TX_2) {
                    mode = boolRand() ? Mode::X : Mode::S;
                } else if (shortMode == USE_READONLY_TX) {
                    mode = Mode::S;
                } else if (shortMode == USE_WRITEONLY_TX) {
                    mode = Mode::X;
                } else if (shortMode == USE_MIX_TX) {
                    mode = isWriteV[i] ? Mode::X : Mode::S;
                } else {
                    assert(shortMode == USE_R2W2);
                    // Last two will be write op.
                    if (i == sz - 1 || i == sz - 2) {
                        mode = Mode::X;
                    } else {
                        mode = Mode::S;
                    }
                }

                lockV.emplace_back();
                if (!lockV.back().lock(&muV[muIdV[i]], mode, txId)) {
                    res.incAbort(isLongTx);
                    abort = true;
                    break;
                }
            }
            if (abort) {
                lockV.clear();
                continue;
            }

            res.incCommit(isLongTx);
            lockV.clear();
            res.addRetryCount(isLongTx, retry);
            break; // retry is not required.
        }

#if 0
        // This is starvation expr only.
        count++;
        if (isLongTx && (longTxSize >= 5 * muV.size() / 100) && count >= 5) {
            shouldQuit = true;
            break;
        }
#endif
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

    size_t nrMuPerTh;
    std::string workload;
    int shortMode;
    int txIdGenType;
    size_t longTxSize;

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendMust(&nrMuPerTh, "mu", "[num]: number of mutexes per thread.");
        appendMust(&workload, "w", "[workload]: workload type in 'shortlong', 'high-contention', 'high-conflicts'.");
        appendOpt(&shortMode, 0, "sm", "[id]: short workload mode (0:r2w2, 1:long2, 2:ro, 3:wo, 4:mix)");
        appendOpt(&txIdGenType, 0, "txid-gen", "[id]: txid gen method (0:sclable, 1:bulk, 2:simple)");
        appendOpt(&longTxSize, 0, "long-tx-size", "[size]: long tx size. 0 means no long tx.");
    }
    void parse(int argc, char *argv[]) {
        base::parse(argc, argv);
        if (nrMuPerTh == 0) {
            throw cybozu::Exception("nrMuPerTh must not be 0.");
        }
        if (longTxSize > nrMuPerTh * nrTh) {
            throw cybozu::Exception("longTxSize is too large: up to nrMuPerTh * nrTh.");
        }
    }
};

template <int shortMode>
void dispatch2(CmdLineOptionPlus& opt, Shared& shared)
{
    switch (opt.txIdGenType) {
    case SCALABLE_TXID_GEN:
        runExec(opt, shared, worker<shortMode, SCALABLE_TXID_GEN>);
        break;
    case BULK_TXID_GEN:
        runExec(opt, shared, worker<shortMode, BULK_TXID_GEN>);
        break;
    case SIMPLE_TXID_GEN:
        runExec(opt, shared, worker<shortMode, SIMPLE_TXID_GEN>);
        break;
    default:
        throw cybozu::Exception("bad txIdGenType") << opt.txIdGenType;
    }
}

void dispatch1(CmdLineOptionPlus& opt, Shared& shared)
{
    switch (opt.shortMode) {
    case USE_R2W2:
        dispatch2<USE_R2W2>(opt, shared);
        break;
    case USE_LONG_TX_2:
        dispatch2<USE_LONG_TX_2>(opt, shared);
        break;
    case USE_READONLY_TX:
        dispatch2<USE_READONLY_TX>(opt, shared);
        break;
    case USE_WRITEONLY_TX:
        dispatch2<USE_WRITEONLY_TX>(opt, shared);
        break;
    case USE_MIX_TX:
        dispatch2<USE_MIX_TX>(opt, shared);
        break;
    default:
        throw cybozu::Exception("bad shortMode") << opt.shortMode;
    }
}

int main(int argc, char *argv[]) try
{
    CmdLineOptionPlus opt("wait_die_bench: benchmark with wait-die lock.");
    opt.parse(argc, argv);

    if (opt.workload == "shortlong") {
        Shared shared;
        shared.workload = opt.workload;
        shared.nrMuPerTh = opt.nrMuPerTh;
        shared.muV.resize(opt.nrMuPerTh * opt.nrTh);
        shared.longTxSize = opt.longTxSize;
        shared.nrOp = 4;
        shared.nrWr = 2;
        shared.shortMode = opt.shortMode;
        shared.txIdGenType = opt.txIdGenType;
        for (size_t i = 0; i < opt.nrLoop; i++) {
            dispatch1(opt, shared);
        }
    } else {
        throw cybozu::Exception("bad workload.") << opt.workload;
    }
} catch (std::exception& e) {
    ::fprintf(::stderr, "exeption: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
