#include <unistd.h>
#include <immintrin.h>
#include "thread_util.hpp"
#include "random.hpp"
#include "cpuid.hpp"
#include "measure_util.hpp"
#include "leis_lock.hpp"


//#define USE_LONG_TX_2
#undef USE_LONG_TX_2

//#define USE_READONLY_TX
#undef USE_READONLY_TX

//#define USE_WRITEONLY_TX
#undef USE_WRITEONLY_TX

//#define USE_MIX_TX
#undef USE_MIX_TX



using LeisLockSet  = cybozu::lock::LeisLockSet;
using Mutex = LeisLockSet::Mutex;
using Mode = LeisLockSet::Mode;

const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);


Result worker(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, std::vector<Mutex>& muV, size_t longTxSize, size_t nrOp, size_t nrWr)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);
    Result res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    std::vector<size_t> muIdV(nrOp);
    LeisLockSet llSet;
    std::vector<size_t> tmpV; // for fillMuIdVecArray.
#ifdef USE_MIX_TX
    std::vector<bool> isWriteV(nrOp);
    std::vector<size_t> tmpV2; // for fillModeVec.
#endif
#ifdef USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);
#endif

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
#ifdef USE_MIX_TX
            isWriteV.resize(nrOp);
            fillModeVec(isWriteV, rand, nrWr, tmpV2);
#endif
        }

#if 0
        // leis-lock will get best performance with total-ordered accesses.
        std::sort(muIdV.begin(), muIdV.end());
#endif

        assert(llSet.empty());
        for (size_t retry = 0;; retry++) {
            bool abort = false;
            const size_t sz = muIdV.size();
            unused(sz);
            for (size_t i = 0; i < muIdV.size(); i++) {
                //Mode mode = (rand() % 2 == 0 ? Mode::X : Mode::S);
                Mode mode;
#if defined(USE_LONG_TX_2)
                mode = boolRand() ? Mode::X : Mode::S;
#elif defined(USE_READONLY_TX)
                mode = Mode::S;
#elif defined(USE_WRITEONLY_TX)
                mode = Mode::X;
#elif defined(USE_MIX_TX)
                mode = isWriteV[i] ? Mode::X : Mode::S;
#else
                // Last two will be write op.
                if (i == sz - 1 || i == sz - 2) {
                    mode = Mode::X;
                } else {
                    mode = Mode::S;
                }
#endif
                if (!llSet.lock(&muV[muIdV[i]], mode)) {
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

void runExec(size_t nrResPerTh, size_t nrTh, size_t runSec, bool verbose, size_t longTxSize, size_t nrOp, size_t nrWr)
{
    std::vector<Mutex> muV(nrResPerTh * nrTh);
    bool start = false;
    bool quit = false;
    bool shouldQuit = false;

    cybozu::thread::ThreadRunnerSet thS;
    std::vector<Result> resV(nrTh);
    for (size_t i = 0; i < nrTh; i++) {
        thS.add([&,i]() {
                resV[i] = worker(i, start, quit, shouldQuit, muV, longTxSize, nrOp, nrWr);
            });
    }
    thS.start();
    start = true;
    size_t sec = 0;
    for (size_t i = 0; i < runSec; i++) {
        if (verbose) ::printf("%zu\n", i);
        sleepMs(1000);
        sec++;
        if (shouldQuit) break;
    }
    quit = true;
    thS.join();
    Result res;
    for (size_t i = 0; i < nrTh; i++) {
        if (verbose) {
            ::printf("worker %zu  %s", i, resV[i].str().c_str());
        }
        res += resV[i];
    }
    ::printf("mode:%s longTxSize:%zu nrResPerTh:%zu concurrency:%zu nrOp:%zu nrWr:%zu sec:%zu  tps:%.03f %s\n"
             , "leis-lock"
             , longTxSize, nrResPerTh, nrTh, nrOp, nrWr, sec, res.nrCommit() / (double)sec
             , res.str().c_str());
    ::fflush(::stdout);
}


int main()
{
#if 1
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
