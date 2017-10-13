#include <unistd.h>
#include <immintrin.h>
#include "thread_util.hpp"
#include "random.hpp"
#include "cpuid.hpp"
#include "measure_util.hpp"
#include "leis_lock.hpp"


template <bool UseMap>
using LeisLockSet  = cybozu::lock::LeisLockSet<UseMap>;

// Mutex and Node must be the same among LeisLockSet<0> and LeisLockSet(1).
using Mutex = LeisLockSet<0>::Mutex;
using Mode = LeisLockSet<0>::Mode;

const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);


struct Shared
{
    std::vector<Mutex> muV;
    size_t longTxSize;
    size_t nrOp;
    size_t nrWr;
    int shortTxMode;
    int longTxMode;
};

template <bool UseMap>
Result worker(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, Shared& shared)
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
    LeisLockSet<UseMap> llSet;
    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    // USE_MIX_TX
    std::vector<bool> isWriteV;
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
        if (isLongTx && shortTxMode == USE_MIX_TX) {
            fillModeVec(isWriteV, rand, nrWr, tmpV2);
        }

        size_t firstRecIdx;
        assert(llSet.empty());
        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            bool abort = false;
            for (size_t i = 0; i < realNrOp; i++) {
#if 0
                Mode mode = getMode(i);
#else
                Mode mode = getMode<decltype(rand), Mode>(
                    boolRand, isWriteV, isLongTx, shortTxMode, longTxMode,
                    realNrOp, nrWr, i);
#endif
                size_t key = getRecordIdx(rand, isLongTx, shortTxMode, longTxMode, muV.size(), realNrOp, i, firstRecIdx);
                Mutex& mutex = muV[key];
                if (!llSet.lock(&mutex, mode)) {
                    res.incAbort(isLongTx);
                    abort = true;
                    break;
                }
            }
            if (abort) {
                llSet.recover();
                continue;
            }

            res.incCommit(isLongTx);
            llSet.unlock();
            res.addRetryCount(isLongTx, retry);
            break; // retry is not required.
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

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendOpt(&useVector, 0, "vector", "[0 or 1]: use vector instead of map.");
    }
    std::string str() const {
        return cybozu::util::formatString("mode:leis ") +
            base::str() +
            cybozu::util::formatString(" vector:%d", useVector != 0);
    }
};


int main(int argc, char *argv[]) try
{
    CmdLineOptionPlus opt("leis_lock_bench: benchmark with leis lock.");
    opt.parse(argc, argv);

    if (opt.workload == "custom") {
        Shared shared;
        shared.muV.resize(opt.getNrMu());
        shared.longTxSize = opt.longTxSize;
        shared.nrOp = opt.nrOp;
        shared.nrWr = opt.nrWr;
        shared.shortTxMode = opt.shortTxMode;
        shared.longTxMode = opt.longTxMode;
        for (size_t i = 0; i < opt.nrLoop; i++) {
            if (opt.useVector != 0) {
                runExec(opt, shared, worker<0>);
            } else {
                runExec(opt, shared, worker<1>);
            }
        }
    } else {
        throw cybozu::Exception("bad workload.") << opt.workload;
    }
} catch (std::exception& e) {
    ::fprintf(::stderr, "exeption: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
