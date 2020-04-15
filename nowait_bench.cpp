#include "thread_util.hpp"
#include "random.hpp"
#include <unistd.h>
#include "cpuid.hpp"
#include "measure_util.hpp"
#include "lock.hpp"
#include "arch.hpp"
#include "vector_payload.hpp"
#include "cache_line_size.hpp"
#include "nowait.hpp"
#include "zipf.hpp"
#include "workload_util.hpp"


#ifdef USE_PARTITION
#include "partitioned.hpp"
#endif


using Mutex = cybozu::lock::XSMutex;
using Lock = cybozu::lock::XSLock;
using Mode = Mutex::Mode;

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
    double wrRatio;
    size_t nrWr4Long;
    TxMode shortTxMode;
    TxMode longTxMode;
    bool usesBackOff;
    size_t nrTh4LongTx;
    size_t payload;
    bool usesRMW;
    bool usesZipf;
    double zipfTheta;
    double zipfZetan;
};

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
    const size_t wrRatio = size_t(shared.wrRatio * (double)SIZE_MAX);
    const TxMode shortTxMode = shared.shortTxMode;
    const TxMode longTxMode = shared.longTxMode;

    Result1 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0), idx);
    FastZipf fastZipf(rand, shared.zipfTheta, recV.size(), shared.zipfZetan);
    cybozu::lock::NoWaitLockSet lockSet;
    std::vector<uint8_t> value(shared.payload);

    const bool isLongTx = longTxSize != 0 && idx < shared.nrTh4LongTx; // starvation setting.
    const size_t realNrOp = isLongTx ? longTxSize : nrOp;
    const size_t realNrWr = isLongTx ? shared.nrWr4Long : size_t(shared.wrRatio * (double)nrOp);
    auto getMode = selectGetModeFunc<decltype(rand), Mode>(isLongTx, shortTxMode, longTxMode);
    auto getRecordIdx = selectGetRecordIdx<decltype(rand)>(isLongTx, shortTxMode, longTxMode, shared.usesZipf);
    lockSet.init(shared.payload, realNrOp);

    storeRelease(ready, 1);
    while (!loadAcquire(start)) _mm_pause();
    size_t count = 0; unused(count);
    while (!loadAcquire(quit)) {
        size_t firstRecIdx = 0;
        uint64_t t0 = -1, t1 = -1, t2 = -1;
        log_timestamp_if_necessary_on_tx_start(t0, shared.usesBackOff);
        auto randState = rand.getState();
        for (size_t retry = 0;; retry++) {
            if (loadAcquire(quit)) break; // to quit under starvation.
            assert(lockSet.empty());
            rand.setState(randState);
            log_timestamp_if_necessary_on_trial_start(t0, t1, t2, retry, shared.usesBackOff);
            for (size_t i = 0; i < realNrOp; i++) {
                size_t key = getRecordIdx(rand, fastZipf, recV.size(), realNrOp, i, firstRecIdx);
                Mode mode = getMode(rand, realNrOp, realNrWr, wrRatio, i);

                auto& item = recV[key];
                Mutex& mutex = item.value;

                if (mode == Mode::S) {
                    if (!lockSet.read(mutex, item.payload, &value[0])) goto abort;
                } else {
                    assert(mode == Mode::X);
                    if (shared.usesRMW) {
                        if (!lockSet.readForUpdate(mutex, item.payload, &value[0])) goto abort;
                        if (!lockSet.write(mutex, item.payload, &value[0])) goto abort;
                    } else {
                        if (!lockSet.write(mutex, item.payload, &value[0])) goto abort;
                    }
                }
            }
            if (!lockSet.blindWriteLockAll()) goto abort;
            lockSet.updateAndUnlock();
            log_timestamp_if_necessary_on_commit(res, t0, t1, t2);
            res.incCommit(isLongTx);
            res.addRetryCount(isLongTx, retry);
            break; // retry is not required.

          abort:
            lockSet.unlock();
            log_timestamp_if_necessary_on_abort(res, t1, t2);
            res.incAbort(isLongTx);
            if (shared.usesBackOff) backOff(t0, retry, rand);
            // continue
        }
    }
    return res;
}


void runTest()
{
#if 0
    runExec(10000, 12, 10, true);
#endif
#if 0
    //for (bool noWait : {false, true}) {
    for (bool noWait : {true}) {
        //for (size_t nrResPerTh : {4000}) {
        for (size_t nrResPerTh : {4, 4000}) {
            //for (size_t nrTh : {16}) {
            for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
                if (nrTh > 16 && nrTh % 2 != 0) continue;
                for (size_t i = 0; i < 20; i++) {
                    bool verbose = false;
                    runExec(nrResPerTh * nrTh, nrTh, 10, verbose, noWait, 0);
                    //sleep_ms(1000);
                }
            }
        }
    }
#endif
#if 0
    // high-contention expr.
    //for (bool noWait : {true, false}) {
    for (bool noWait : {true, false}) {
        for (size_t nrMutex : {40}) {
            //for (size_t nrTh : {32}) {
            //for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
            for (size_t nrTh : {8, 16, 24, 32}) {
                if (nrTh > 16 && nrTh % 2 != 0) continue;
                const size_t nrOp = 10;
                for (size_t nrWr = 0; nrWr <= nrOp; nrWr++) {
                    for (size_t i = 0; i < 10; i++) {
                        bool verbose = false;
                        runExec(nrMutex, nrTh, 10, verbose, noWait, 0, nrOp, nrWr);
                    }
                }
            }
        }
    }
#endif
#if 0
    // starvation expr.
    for (bool noWait : {true, false}) {
#if 0
        const size_t nrMutex = 400 * 1000 * 1000;
        const size_t nrTh = 16;
#else
        const size_t nrMutex = 40 * 1000;
        const size_t nrTh = 8;
#endif
        std::initializer_list<size_t> longTxPmlV;
        longTxPmlV = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                    20, 30, 40, 50, 60, 70, 80, 90, 100,
                    200, 300, 400, 500, 600, 700, 800, 900, 1000};
        for (size_t longTxPml : longTxPmlV) {
            const size_t longTxSize = longTxPml * nrMutex / 1000;
            for (size_t i = 0; i < 10; i++) {
                bool verbose = false;
                //size_t maxSec = longTxPct >= 5 ? 50000 : 100;
                size_t maxSec = 100;
                runExec(nrMutex, nrTh, maxSec, verbose, noWait, longTxSize);
                //sleep_ms(1000);
            }
        }
    }
#endif
}


struct CmdLineOptionPlus : CmdLineOption
{
    using base = CmdLineOption;

    int usesBackOff; // 0 or 1.
    int usesRMW; // 0 or 1.

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendOpt(&usesBackOff, 0, "backoff", "[0 or 1]: backoff 0:off 1:on");
        appendOpt(&usesRMW, 1, "rmw", "[0 or 1]: use read-modify-write or normal write 0:w 1:rmw (default: 1)");
    }
    std::string str() const {
        return cybozu::util::formatString(
            "mode:nowait %s backoff:%d rmw:%d"
            , base::str().c_str(), usesBackOff ? 1 : 0, usesRMW ? 1 : 0);
    }
};


int main(int argc, char *argv[]) try
{
    CmdLineOptionPlus opt("nowait_bench: benchmark with nowait lock.");
    opt.parse(argc, argv);
    setCpuAffinityModeVec(opt.amode, CpuId_);

#ifdef NO_PAYLOAD
    if (opt.payload != 0) throw cybozu::Exception("payload not supported");
#endif

    if (opt.workload == "custom") {
        Shared shared;
        initRecordVector(shared.recV, opt);
        shared.longTxSize = opt.longTxSize;
        shared.nrOp = opt.nrOp;
        shared.wrRatio = opt.wrRatio;
        shared.nrWr4Long = opt.nrWr4Long;
        shared.shortTxMode = TxMode(opt.shortTxMode);
        shared.longTxMode = TxMode(opt.longTxMode);
        shared.usesBackOff = opt.usesBackOff ? 1 : 0;
        shared.nrTh4LongTx = opt.nrTh4LongTx;
        shared.payload = opt.payload;
        shared.usesRMW = opt.usesRMW != 0;
        shared.usesZipf = opt.usesZipf;
        shared.zipfTheta = opt.zipfTheta;
        if (shared.usesZipf) {
            shared.zipfZetan = FastZipf::zeta(opt.getNrMu(), shared.zipfTheta);
        } else {
            shared.zipfZetan = 1.0;
        }
        for (size_t i = 0; i < opt.nrLoop; i++) {
            Result1 res;
            runExec(opt, shared, worker2, res);
        }
    } else {
        throw cybozu::Exception("bad workload.") << opt.workload;
    }
} catch (std::exception& e) {
    ::fprintf(::stderr, "exeption: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
