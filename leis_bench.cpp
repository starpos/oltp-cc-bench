#include <unistd.h>
#include "thread_util.hpp"
#include "random.hpp"
#include "cpuid.hpp"
#include "measure_util.hpp"
#include "leis_lock.hpp"
#include "arch.hpp"
#include "vector_payload.hpp"
#include "cache_line_size.hpp"
#include "zipf.hpp"
#include "workload_util.hpp"


#ifdef USE_PARTITION
#include "partitioned.hpp"
#endif


std::vector<uint> CpuId_;


template <typename LeisLockType>
struct Shared
{
    using Mutex = typename LeisLockType::Mutex;
#ifdef USE_PARTITION
    PartitionedVectorWithPayload<Mutex> recV;
#else
    VectorWithPayload<Mutex> recV;
#endif
    size_t longTxSize;
    size_t nrOp;
    double wrRatio;
    size_t nrWr4Long;
    TxMode shortTxMode;
    TxMode longTxMode;
    size_t nrTh4LongTx;
    size_t payload;
    bool usesRMW;
    bool usesZipf;
    double zipfTheta;
    double zipfZetan;
};


template <bool UseMap, typename LeisLockType>
Result1 worker(size_t idx, uint8_t& ready, const bool& start, const bool& quit, bool& shouldQuit, Shared<LeisLockType>& shared)
{
    using Mutex = typename LeisLockType::Mutex;
    using Mode = typename Mutex::Mode;

    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    auto& recV = shared.recV;
#ifdef USE_PARTITION
    recV.allocate(idx);
    recV.checkAndWait();
#endif
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t wrRatio = size_t(shared.wrRatio * (double)SIZE_MAX);
    const TxMode shortTxMode = shared.shortTxMode;
    const TxMode longTxMode = shared.longTxMode;

    Result1 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0), idx);
    FastZipf fastZipf(rand, shared.zipfTheta, recV.size(), shared.zipfZetan);

    cybozu::lock::LeisLockSet<UseMap, LeisLockType> llSet;
    std::vector<uint8_t> value(shared.payload);

    const bool isLongTx = longTxSize != 0 && idx < shared.nrTh4LongTx; // starvation setting.
    const size_t realNrOp = isLongTx ? longTxSize : nrOp;
    const size_t realNrWr = isLongTx ? shared.nrWr4Long : size_t((double)nrOp * shared.wrRatio);
    auto getMode = selectGetModeFunc<decltype(rand), Mode>(isLongTx, shortTxMode, longTxMode);
    auto getRecordIdx = selectGetRecordIdx<decltype(rand)>(isLongTx, shortTxMode, longTxMode, shared.usesZipf);

    llSet.init(shared.payload, realNrOp);

    storeRelease(ready, 1);
    while (!loadAcquire(start)) _mm_pause();
    size_t count = 0; unused(count);
    while (!loadAcquire(quit)) {
        size_t firstRecIdx;
        assert(llSet.empty());
        auto randState = rand.getState();
        for (size_t retry = 0;; retry++) {
            if (loadAcquire(quit)) break; // to quit under starvation.
            rand.setState(randState); // Retries will reproduce the same access pattern.
            for (size_t i = 0; i < realNrOp; i++) {
                Mode mode = getMode(rand, realNrOp, realNrWr, wrRatio, i);
                size_t key = getRecordIdx(rand, fastZipf, recV.size(), realNrOp, i, firstRecIdx);
                auto& item = recV[key];
                Mutex& mutex = item.value;
                if (mode == Mode::S) {
                    if (!llSet.read(mutex, item.payload, &value[0])) goto abort;
                } else {
                    assert(mode == Mode::X);
                    if (shared.usesRMW) {
                        if (!llSet.readForUpdate(mutex, item.payload, &value[0])) goto abort;
                        if (!llSet.write(mutex, item.payload, &value[0])) goto abort;
                    } else {
                        if (!llSet.write(mutex, item.payload, &value[0])) goto abort;
                    }
                }
            }
            if (!llSet.blindWriteLockAll()) goto abort;
            llSet.updateAndUnlock();
            res.incCommit(isLongTx);
            res.addRetryCount(isLongTx, retry);
            break; // retry is not required.

          abort:
            llSet.recover();
            res.incAbort(isLongTx);
            // continue
        }

#if 0
        // This is startvation expr only.
        count++;
        //if (isLongTx && (longTxSize >= 5 * muV.size() / 100) && count >= 10) {
        if (isLongTx && count >= 5) {
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
    //for (size_t nrResPerTh : {4}) {
    for (size_t nrResPerTh : {4000}) {
    //for (size_t nrResPerTh : {4, 4000}) {
        //for (size_t nrTh : {32}) {
        for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
            if (nrTh > 2 && nrTh % 2 != 0) continue;
            for (size_t i = 0; i < 10; i++) {
                bool verbose = false;
                runExec(nrResPerTh, nrTh, 10, verbose, 0, 4, 2);
                //sleepMs(1000);
            }
        }
    }
#endif
#if 0
    runExec(5000, 8, 10, true);
#endif
#if 0
    // high-contention expr.
    for (size_t nrMutex : {40}) {
        //for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
        //for (size_t nrTh : {32}) {
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
    // starvation expr.
#if 0
    const size_t nrMutex = 400 * 1000 * 1000;
    const size_t nrTh = 16;
#else
    const size_t nrMutex = 40 * 1000;
    const size_t nrTh = 8;
#endif
    //for (size_t longTxPct : {1}) {
    for (size_t longTxPml : {1, 2, 3, 4, 5, 6, 7, 8, 9,
                10, 20, 30, 40, 50, 60, 70, 80, 90,
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}) {
        const size_t longTxSize = longTxPml * nrMutex / 1000;
        for (size_t i = 0; i < 10; i++) {
            bool verbose = false;
            //size_t maxSec = longTxPct >= 5 ? 20000 : 100;
            //size_t maxSec = 30000;
            size_t maxSec = 100;
            runExec(nrMutex / nrTh, nrTh, maxSec, verbose, longTxSize);
            sleepMs(1000);
        }
    }
#endif
}

struct CmdLineOptionPlus : CmdLineOption
{
    using base = CmdLineOption;

    int useVector;
    int leisLockType;
    int usesRMW; // 0 or 1.

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendOpt(&useVector, 0, "vector", "[0 or 1]: use vector instead of map. (default:0)");
        appendOpt(&leisLockType, 0, "lock", "[id]: leis lock type (0:spin, 1:withmcs, 2:sxql, default:0)");
        appendOpt(&usesRMW, 1, "rmw", "[0 or 1]: use read-modify-write or normal write 0:w 1:rmw (default: 1)");
    }
    std::string str() const {
        return cybozu::util::formatString(
            "mode:leis %s vector:%d lockType:%d rmw:%d"
            , base::str().c_str(), useVector != 0, leisLockType, usesRMW ? 1 : 0);
    }
};


enum LockLockTypeType
{
    USE_LEIS_SPIN = 0,
    USE_LEIS_WITHMCS = 1,
    USE_LEIS_SXQL = 2,
};



template <typename Lock>
void dispatch1(const CmdLineOptionPlus& opt)
{
    Shared<Lock> shared;
    initRecordVector(shared.recV, opt);
    shared.longTxSize = opt.longTxSize;
    shared.nrOp = opt.nrOp;
    shared.wrRatio = opt.wrRatio;
    shared.nrWr4Long = opt.nrWr4Long;
    shared.shortTxMode = TxMode(opt.shortTxMode);
    shared.longTxMode = TxMode(opt.longTxMode);
    shared.nrTh4LongTx = opt.nrTh4LongTx;
    shared.payload = opt.payload;
    shared.usesRMW = opt.usesRMW ? 1 : 0;
    shared.usesZipf = opt.usesZipf;
    shared.zipfTheta = opt.zipfTheta;
    if (shared.usesZipf) {
        shared.zipfZetan = FastZipf::zeta(opt.getNrMu(), shared.zipfTheta);
    } else {
        shared.zipfZetan = 1.0;
    }
    for (size_t i = 0; i < opt.nrLoop; i++) {
        Result1 res;
        if (opt.useVector != 0) {
            runExec(opt, shared, worker<0, Lock>, res);
        } else {
            runExec(opt, shared, worker<1, Lock>, res);
        }
    }
}


void dispatch0(const CmdLineOptionPlus& opt)
{
    switch (opt.leisLockType) {
    case USE_LEIS_SPIN:
        dispatch1<cybozu::lock::XSLock>(opt);
        break;
    case USE_LEIS_WITHMCS:
        dispatch1<cybozu::lock::LockWithMcs>(opt);
        break;
    case USE_LEIS_SXQL:
        dispatch1<cybozu::lock::SXQLock>(opt);
        break;
    default:
        throw cybozu::Exception("bad leisLockType") << opt.leisLockType;
    }
}


int main(int argc, char *argv[]) try
{
    CmdLineOptionPlus opt("leis_lock_bench: benchmark with leis lock.");
    opt.parse(argc, argv);
    setCpuAffinityModeVec(opt.amode, CpuId_);

#ifdef NO_PAYLOAD
    if (opt.payload != 0) throw cybozu::Exception("payload not supported");
#endif

    if (opt.workload != "custom") {
        throw cybozu::Exception("bad workload.") << opt.workload;
    }
    dispatch0(opt);

} catch (std::exception& e) {
    ::fprintf(::stderr, "exeption: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
