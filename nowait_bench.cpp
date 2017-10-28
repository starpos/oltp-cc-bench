#include "thread_util.hpp"
#include "random.hpp"
#include <unistd.h>
#include "cpuid.hpp"
#include "measure_util.hpp"
#include "lock.hpp"
#include "arch.hpp"


using Mutex = cybozu::lock::XSMutex;
using Lock = cybozu::lock::XSLock;
using Mode = Mutex::Mode;

const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);


struct Shared
{
    std::vector<Mutex> muV;
    size_t longTxSize;
    size_t nrOp;
    size_t nrWr;
    int shortTxMode;
    int longTxMode;
    bool usesBackOff;
};

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
    cybozu::lock::NoWaitLockSet lockSet;
    std::vector<size_t> tmpV; // for fillMuIdVecArray.

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
                const size_t key = getRecordIdx(
                    rand, isLongTx, shortTxMode, longTxMode,
                    muV.size(), realNrOp, i, firstRecIdx);
                Mutex& mutex = muV[key];
                if (!lockSet.lock(mutex, mode)) {
                    abort = true;
                    break;
                }
            }
            if (abort) {
                res.incAbort(isLongTx);
                lockSet.clear();
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
                    //sleepMs(1000);
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
                //sleepMs(1000);
            }
        }
    }
#endif
}


struct CmdLineOptionPlus : CmdLineOption
{
    using base = CmdLineOption;

    int usesBackOff; // 0 or 1.

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendOpt(&usesBackOff, 0, "backoff", "[0 or 1]: backoff 0:off 1:on");
    }
    std::string str() const {
        return cybozu::util::formatString(
            "mode:nowait %s backoff:%d"
            , base::str().c_str(), usesBackOff ? 1 : 0);
    }
};


int main(int argc, char *argv[]) try
{
    CmdLineOptionPlus opt("nowait_bench: benchmark with nowait lock.");
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
            runExec(opt, shared, worker2);
        }
    } else {
        throw cybozu::Exception("bad workload.") << opt.workload;
    }
} catch (std::exception& e) {
    ::fprintf(::stderr, "exeption: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
