#include <ctime>
#include <vector>
#include <chrono>
#include <unistd.h>
#include "occ.hpp"
#include "thread_util.hpp"
#include "random.hpp"
#include "measure_util.hpp"
#include "cpuid.hpp"
#include "vector_payload.hpp"
#include "cache_line_size.hpp"
#include "zipf.hpp"

#ifdef USE_PARTITION
#include "partitioned.hpp"
#endif

using Mutex = cybozu::occ::OccLock::Mutex;

std::vector<uint> CpuId_;


struct Shared
{
#ifdef USE_PARTITION
    PartitionedVectorWithPayload<Mutex> recV;
#else
    VectorWithPayload<Mutex> recV;
#endif
    size_t longTxSize;
    size_t nrOp;
    size_t nrWr;
    size_t nrWr4Long;
    int shortTxMode;
    int longTxMode;
    bool usesBackOff;
    bool usesRMW;
    bool nowait;
    size_t nrTh4LongTx;
    size_t payload;
    size_t nrMuPerTh;
    bool usesZipf;
    double zipfTheta;
    double zipfZetan;
};


enum class Mode : bool { S = false, X = true, };


Result1 worker2(size_t idx, uint8_t& ready, const bool& start, const bool& quit, bool& shouldQuit, Shared& shared)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    auto& recV = shared.recV;
#ifdef USE_PARTITION
    recV.allocate(idx);
    recV.checkAndWait();
#endif
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t nrWr = shared.nrWr;
    const int shortTxMode = shared.shortTxMode;
    const int longTxMode = shared.longTxMode;

    Result1 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0), idx);
    FastZipf fastZipf(rand, shared.zipfTheta, recV.size(), shared.zipfZetan);

    std::vector<uint8_t> value(shared.payload);
    cybozu::occ::LockSet lockSet;

    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    // USE_MIX_TX
    std::vector<bool> isWriteV(nrOp);
    std::vector<size_t> tmpV2; // for fillModeVec.

    // USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);

    const bool isLongTx = longTxSize != 0 && idx < shared.nrTh4LongTx; // starvation setting.
    const size_t realNrOp = isLongTx ? longTxSize : nrOp;
    const size_t realNrWr = isLongTx ? shared.nrWr4Long : nrWr;
    if (!isLongTx && shortTxMode == USE_MIX_TX) {
        isWriteV.resize(nrOp);
    }
    lockSet.init(shared.payload, realNrOp);

#if 0
    GetModeFunc<decltype(rand), Mode>
        getMode(boolRand, isWriteV, isLongTx,
                shortTxMode, longTxMode, realNrOp, nrWr);
#endif

    storeRelease(ready, 1);
    while (!loadAcquire(start)) _mm_pause();
    while (!loadAcquire(quit)) {
        if (!isLongTx && shortTxMode == USE_MIX_TX) {
            fillModeVec(isWriteV, rand, nrWr, tmpV2);
        }
        size_t firstRecIdx = 0;
        uint64_t t0 = 0;
        if (shared.usesBackOff) t0 = cybozu::time::rdtscp();
        auto randState = rand.getState();
        for (size_t retry = 0;; retry++) {
            if (loadAcquire(quit)) break; // to quit under starvation.
            // Try to run transaction.
            assert(lockSet.empty());
            rand.setState(randState);
            for (size_t i = 0; i < realNrOp; i++) {
#if 0
                const bool isWrite = bool(getMode(i));
#else
                const bool isWrite = bool(
                    getMode<decltype(rand), Mode>(
                        rand, boolRand, isWriteV, isLongTx, shortTxMode, longTxMode,
                        realNrOp, realNrWr, i));
#endif
                const size_t key = getRecordIdx(rand, isLongTx, shortTxMode, longTxMode,
                                                recV.size(), realNrOp, i, firstRecIdx,
                                                shared.usesZipf, fastZipf);
                auto& item = recV[key];
                Mutex& mutex = item.value;
                void *payload = item.payload;
                if (shared.usesRMW || !isWrite) {
                    lockSet.read(mutex, payload, &value[0]);
                }
                if (isWrite) {
                    lockSet.write(mutex, payload, &value[0]);
                }
            }

            // commit phase.
            if (shared.nowait) {
                if (!lockSet.tryLock()) goto abort;
            } else {
                lockSet.lock();
            }
            if (!lockSet.verify()) goto abort;
            lockSet.updateAndUnlock();
            res.incCommit(isLongTx);
            res.addRetryCount(isLongTx, retry);
            break;
        abort:
            lockSet.clear();
            res.incAbort(isLongTx);
            if (shared.usesBackOff) backOff(t0, retry, rand);
            // continue
        }
    }
    return res;
}


Result1 worker3(size_t idx, uint8_t& ready, const bool& start, const bool& quit, bool& shouldQuit, Shared& shared)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    auto& recV = shared.recV;
#ifdef USE_PARTITION
    recV.allocate(idx);
    recV.checkAndWait();
#endif
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t nrWr = shared.nrWr;
    const int shortTxMode = shared.shortTxMode;
    const int longTxMode = shared.longTxMode;

    Result1 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0), idx);

    std::vector<uint8_t> value(shared.payload);
    cybozu::occ::LockSet lockSet;

    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    // USE_MIX_TX
    std::vector<bool> isWriteV(nrOp);
    std::vector<size_t> tmpV2; // for fillModeVec.

    // USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);

    const bool isLongTx = longTxSize != 0 && idx < shared.nrTh4LongTx; // starvation setting.
    const size_t realNrOp = isLongTx ? longTxSize : nrOp;
    const size_t realNrWr = isLongTx ? shared.nrWr4Long : nrWr;
    if (!isLongTx && shortTxMode == USE_MIX_TX) {
        isWriteV.resize(nrOp);
    }
    lockSet.init(shared.payload, realNrOp);

    const size_t keyBase = shared.nrMuPerTh * idx;

    storeRelease(ready, 1);
    while (!loadAcquire(start)) _mm_pause();
    while (!loadAcquire(quit)) {
        if (!isLongTx && shortTxMode == USE_MIX_TX) {
            fillModeVec(isWriteV, rand, nrWr, tmpV2);
        }
        //size_t firstRecIdx = 0;
        uint64_t t0 = 0;
        if (shared.usesBackOff) t0 = cybozu::time::rdtscp();
        auto randState = rand.getState();
        for (size_t retry = 0;; retry++) {
            if (loadAcquire(quit)) break; // to quit under starvation.
            // Try to run transaction.
            assert(lockSet.empty());
            rand.setState(randState);
            for (size_t i = 0; i < realNrOp; i++) {
                const bool isWrite = bool(
                    getMode<decltype(rand), Mode>(
                        rand, boolRand, isWriteV, isLongTx, shortTxMode, longTxMode,
                        realNrOp, realNrWr, i));

#if 0
                const size_t key = getRecordIdx(rand, isLongTx, shortTxMode, longTxMode,
                                                recV.size(), realNrOp, i, firstRecIdx);
#else
                // Access to local area only.
                const size_t key = keyBase + rand() % shared.nrMuPerTh;
#endif
                auto& item = recV[key];
                Mutex& mutex = item.value;
                void *payload = item.payload;
                if (shared.usesRMW || !isWrite) {
                    lockSet.read(mutex, payload, &value[0]);
                }
                if (isWrite) {
                    lockSet.write(mutex, payload, &value[0]);
                }
            }

            // commit phase.
            if (shared.nowait) {
                if (!lockSet.tryLock()) goto abort;
            } else {
                lockSet.lock();
            }
            if (!lockSet.verify()) goto abort;
            lockSet.updateAndUnlock();
            res.incCommit(isLongTx);
            res.addRetryCount(isLongTx, retry);
            break;
        abort:
            lockSet.clear();
            res.incAbort(isLongTx);
            if (shared.usesBackOff) backOff(t0, retry, rand);
            // continue;
        }
    }
    return res;
}


void runTest()
{
#if 0
    for (size_t nrResPerTh : {4000}) {
    //for (size_t nrResPerTh : {4, 4000}) {
        //for (size_t nrTh : {32}) {
        for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
            if (nrTh > 2 && nrTh % 2 != 0) continue;
            for(size_t i = 0; i < 10; i++) {
                bool verbose = false;
                runExec(nrResPerTh * nrTh, nrTh, 10, verbose, 0, 4, 0);
                //sleepMs(1000);
            }
        }
    }
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
    for (size_t longTxPml : {1, 2, 3, 4, 5, 6, 7, 8, 9,
                10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                20, 30, 40, 50, 60, 70, 80, 90,
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}) {
        const size_t longTxSize = longTxPml * nrMutex / 1000;
        for (size_t i = 0; i < 10; i++) {
            bool verbose = false;
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

    int usesBackOff; // 0 or 1.
    int usesRMW; // 0 or 1.
    int nowait; // 0 or 1.

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendOpt(&usesBackOff, 0, "backoff", "[0 or 1]: backoff (0:off, 1:on)");
        appendOpt(&usesRMW, 1, "rmw", "[0 or 1]: use read-modify-write or normal write (0:w, 1:rmw, default:1)");
        appendOpt(&nowait, 0, "nowait", "[0 or 1]: use nowait optimization.");
    }
    std::string str() const {
        return cybozu::util::formatString(
            "mode:silo-occ %s backoff:%d rmw:%d nowait:%d"
            , base::str().c_str(), usesBackOff ? 1 : 0, usesRMW ? 1 : 0, nowait ? 1 : 0);
    }
};


template <typename Opt>
void initShared(Shared& shared, const Opt& opt)
{
    initRecordVector(shared.recV, opt);
    shared.longTxSize = opt.longTxSize;
    shared.nrOp = opt.nrOp;
    shared.nrWr = opt.nrWr;
    shared.nrWr4Long = opt.nrWr4Long;
    shared.shortTxMode = opt.shortTxMode;
    shared.longTxMode = opt.longTxMode;
    shared.usesBackOff = opt.usesBackOff ? 1 : 0;
    shared.usesRMW = opt.usesRMW ? 1 : 0;
    shared.nowait = opt.nowait ? 1 : 0;
    shared.nrTh4LongTx = opt.nrTh4LongTx;
    shared.payload = opt.payload;
    shared.nrMuPerTh = opt.getNrMuPerTh();
    shared.usesZipf = opt.usesZipf;
    shared.zipfTheta = opt.zipfTheta;
    if (opt.usesZipf) {
        shared.zipfZetan = FastZipf::zeta(opt.getNrMu(), shared.zipfTheta);
    } else {
        shared.zipfZetan = 1.0;
    }
}


int main(int argc, char *argv[]) try
{
    CmdLineOptionPlus opt("occ_bench: benchmark with silo-occ.");
    opt.parse(argc, argv);
    setCpuAffinityModeVec(opt.amode, CpuId_);

#ifdef NO_PAYLOAD
    if (opt.payload != 0) throw cybozu::Exception("payload not supported");
#endif

    if (opt.workload == "custom") {
        Shared shared;
        initShared(shared, opt);
        for (size_t i = 0; i < opt.nrLoop; i++) {
            Result1 res;
            runExec(opt, shared, worker2, res);
        }
    } else if (opt.workload == "local") {
        Shared shared;
        initShared(shared, opt);
        for (size_t i = 0; i < opt.nrLoop; i++) {
            Result1 res;
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
