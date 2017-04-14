#include "thread_util.hpp"
#include "random.hpp"
#include <immintrin.h>
#include <unistd.h>
#include "cpuid.hpp"
#include "measure_util.hpp"
#include "lock.hpp"


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
};

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
    std::vector<size_t> muIdV(nrOp);
    std::vector<Lock> lockV;
    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    // USE_MIX_TX
    std::vector<bool> isWriteV(nrOp);
    std::vector<size_t> tmpV2; // for fillModeVec.

    // USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);


    const bool isLongTx = longTxSize != 0 && idx == 0; // starvation setting.
    if (isLongTx) {
        muIdV.resize(longTxSize);
    } else {
        muIdV.resize(nrOp);
        if (shortTxMode == USE_MIX_TX) {
            isWriteV.resize(nrOp);
        }
    }
    GetModeFunc<decltype(rand), Mode>
        getMode(boolRand, isWriteV, isLongTx,
                shortTxMode, longTxMode, muIdV.size(), nrWr);


    while (!start) _mm_pause();
    size_t count = 0; unused(count);
    while (!quit) {
        if (isLongTx) {
            if (longTxSize > muV.size() * 5 / 1000) {
                fillMuIdVecArray(muIdV, rand, muV.size(), tmpV);
            } else {
                fillMuIdVecLoop(muIdV, rand, muV.size());
            }
        } else {
            fillMuIdVecLoop(muIdV, rand, muV.size());
            if (shortTxMode == USE_MIX_TX) {
                fillModeVec(isWriteV, rand, nrWr, tmpV2);
            }
        }

        // Do not sort with noWait mode.
        //std::sort(muIdV.begin(), muIdV.end());

        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            assert(lockV.empty());
            bool abort = false;
            const size_t sz = muIdV.size();
            unused(sz);
            for (size_t i = 0; i < muIdV.size(); i++) {
                Mode mode = getMode(i);
                lockV.emplace_back();
                if (!lockV.back().tryLock(&muV[muIdV[i]], mode)) {
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
        if (isLongTx && (longTxSize >= 5 * muV.size() / 100) && count >= 10) {
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

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
    }
    std::string str() const {
        return cybozu::util::formatString("mode:nowait ") + base::str();
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
        for (size_t i = 0; i < opt.nrLoop; i++) {
            runExec(opt, shared, worker);
        }
    } else {
        throw cybozu::Exception("bad workload.") << opt.workload;
    }
} catch (std::exception& e) {
    ::fprintf(::stderr, "exeption: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
