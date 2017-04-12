#include <ctime>
#include <vector>
#include <chrono>
#include <unistd.h>
#include "tictoc.hpp"
#include "thread_util.hpp"
#include "random.hpp"
#include "measure_util.hpp"
#include "cpuid.hpp"

//#define USE_LONG_TX_2
#undef USE_LONG_TX_2

//#define USE_READONLY_TX
#undef USE_READONLY_TX

//#define USE_WRITEONLY_TX
#undef USE_WRITEONLY_TX

//#define USE_MIX_TX
#undef USE_MIX_TX


using Mutex = cybozu::tictoc::Mutex;

const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);


Result worker(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, std::vector<Mutex>& muV, uint32_t& txIdGen, size_t longTxSize, size_t nrOp, size_t nrWr)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);
    Result res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    std::vector<size_t> muIdV(nrOp);
    cybozu::tictoc::WriteSet ws;
    cybozu::tictoc::ReadSet rs;
    cybozu::tictoc::LockSet ls;
    cybozu::tictoc::Flags flags;
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
        //const bool isLongTx = rand() % 1000 == 0; // 0.1% long transaction.
        const bool isLongTx = longTxSize != 0 && idx == 0; // starvation setting.
        if (isLongTx) {
            muIdV.resize(longTxSize);
            if (longTxSize > muV.size() * 5 / 1000) {
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
        const uint32_t txId = __atomic_fetch_add(&txIdGen, 1, __ATOMIC_RELAXED);
        unused(txId);
#else
        unused(txIdGen);
#endif
        for (size_t retry = 0;; retry++) {
            // Try to run transaction.
            const size_t sz = muIdV.size();
            unused(sz);
            for (size_t i = 0; i < muIdV.size(); i++) {
                bool isWrite;
#if defined(USE_LONG_TX_2)
                isWrite = boolRand();
#elif defined(USE_READONLY_TX)
                isWrite = false;
#elif defined(USE_WRITEONLY_TX)
                isWrite = true;
#elif defined(USE_MIX_TX)
                isWrite = isWriteV[i];
#else
                isWrite = (i == sz - 1 || i == sz - 2);
#endif
                // read
                cybozu::tictoc::Reader r;
                r.prepare(&muV[muIdV[i]]);
                for (;;) {
                    // should read data here.
                    r.readFence();
                    if (r.isReadSucceeded()) break;
                    r.prepareRetry();
                }
                rs.push_back(std::move(r));
                if (isWrite) {
                    // write
                    ws.emplace_back(&muV[muIdV[i]]);
                }
            }
            if (!cybozu::tictoc::preCommit(rs, ws, ls, flags)) {
                res.incAbort(isLongTx);
                continue;
            }
            res.incCommit(isLongTx);
            res.addRetryCount(isLongTx, retry);
            break;
        }

#if 0
        // This is starvation expr only
        count++;
        if (isLongTx && (longTxSize >= 1 * muV.size() / 100) && count >= 5) {
            shouldQuit = true;
            break;
        }
#endif
    }
    return res;
}


void runExec(size_t nrMutex, size_t nrTh, size_t runSec, bool verbose, size_t longTxSize, size_t nrOp, size_t nrWr)
{
    std::vector<Mutex> muV(nrMutex);
    bool start = false;
    bool quit = false;
    bool shouldQuit = false;
    uint32_t txIdGen = 0;
    cybozu::thread::ThreadRunnerSet thS;
    std::vector<Result> resV(nrTh);
    for (size_t i = 0; i < nrTh; i++) {
        thS.add([&,i]() { resV[i] = worker(i, start, quit, shouldQuit, muV, txIdGen, longTxSize, nrOp, nrWr); });
    }
    thS.start();
    start = true;
    size_t sec = 0;
    for (size_t i = 0; i < runSec; i++) {
        if (verbose) {
            ::printf("%zu %u\n", i, __atomic_load_n(&txIdGen, __ATOMIC_RELAXED));
        }
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
    ::printf("mode:tictoc longTxSize:%zu nrMutex:%zu concurrency:%zu nrOp:%zu nrWr:%zu sec:%zu tps:%.03f %s\n"
             , longTxSize, nrMutex, nrTh, nrOp, nrWr, sec, res.nrCommit() / (double)sec
             , res.str().c_str());
    ::fflush(::stdout);
}


int main()
{
#if 0
    runExec(4000 * 12, 12, 10, true, 0);
#endif
#if 1
    size_t nrOp = 4;
    //for (size_t nrResPerTh : {4, 4000}) {
    for (size_t nrResPerTh : {4000}) {
        for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
        //for (size_t nrTh : {32}) {
            if (nrTh > 2 && nrTh % 2 != 0) continue;
            for(size_t i = 0; i < 10; i++) {
                runExec(nrResPerTh * nrTh, nrTh, 10, false, 0, nrOp, 0);
                //sleepMs(1000);
            }
        }
    }
#endif
#if 0
    // high-contention expr.
    for (size_t nrMutex : {40}) {
        //for (size_t nrTh : {32}) {
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
    // starvation expr.
#if 0
    const size_t nrMutex = 400 * 1000 * 1000;
    const size_t nrTh = 16;
#else
    const size_t nrMutex = 40 * 1000;
    const size_t nrTh = 8;
#endif
    // for (size_t longTxPml : {20, 30, 40, 50, 60, 70, 80, 90,
    //             100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}) {
    for (size_t longTxPml : {1, 2, 3, 4, 5, 6, 7, 8, 9,
                10, 20, 30, 40, 50, 60, 70, 80, 90,
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}) {
        const size_t longTxSize = longTxPml * nrMutex / 1000;
        for (size_t i = 0; i < 10; i++) {
            bool verbose = false;
            //size_t maxSec = longTxPml >= 10 ? 20000 : 100;
            size_t maxSec = 100;
            runExec(nrMutex, nrTh, maxSec, verbose, longTxSize);
        }
    }
#endif
}
