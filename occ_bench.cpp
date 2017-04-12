#include <ctime>
#include <vector>
#include <chrono>
#include <unistd.h>
#include "occ.hpp"
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


using Mutex = cybozu::occ::OccLock::Mutex;

const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);


Result worker(size_t idx, bool& start, bool& quit, std::vector<Mutex>& muV, uint32_t& txIdGen, size_t longTxSize, size_t nrOp, size_t nrWr)
{
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);
    Result res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    std::vector<size_t> muIdV(nrOp);

    std::vector<Mutex*> writeSet;
    std::vector<cybozu::occ::OccReader> readSet;
    std::vector<cybozu::occ::OccLock> lockV;

    std::vector<size_t> tmpV; // for fillMuIdVecArray.

#ifdef USE_MIX_TX
    std::vector<bool> isWriteV(nrOp);
    std::vector<size_t> tmpV2; // for fillModeVec.
#endif
#ifdef USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);
#endif

    while (!start) _mm_pause();
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
            bool abort = false;
            assert(writeSet.empty());
            assert(readSet.empty());
            assert(lockV.empty());

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
                for (;;) {
                    cybozu::occ::OccReader r;
                    r.prepare(&muV[muIdV[i]]);
                    // should read resource here.
                    r.readFence();
                    if (r.verifyAll()) {
                        readSet.push_back(std::move(r));
                        break;
                    }
                }
                if (isWrite) {
                    // write
                    writeSet.push_back(&muV[muIdV[i]]);
                }
            }
#if 0
            // delay
            if (isLongTx) ::usleep(1);
#endif

            // commit phase.
            std::sort(writeSet.begin(), writeSet.end());
            for (Mutex* mutex : writeSet) {
                lockV.emplace_back(mutex);
            }
            __atomic_thread_fence(__ATOMIC_ACQ_REL);
            for (cybozu::occ::OccReader& r : readSet) {
                const Mutex *p = reinterpret_cast<Mutex*>(r.getId());
                const bool ret =
                    std::binary_search(writeSet.begin(), writeSet.end(), p)
                    ? r.verifyVersion() : r.verifyAll();
                if (!ret) {
                    abort = true;
                    break;
                }
            }
            if (abort) {
                lockV.clear();
                writeSet.clear();
                readSet.clear();
                res.incAbort(isLongTx);
                continue;
            }
            // We can commit.
            for (cybozu::occ::OccLock& lk : lockV) {
                // should write resource here.
                lk.update();
                lk.writeFence();
                lk.unlock();
            }
            lockV.clear();
            writeSet.clear();
            readSet.clear();
            res.incCommit(isLongTx);
            res.addRetryCount(isLongTx, retry);
            break;
        }
    }
    return res;
}


void runExec(size_t nrMutex, size_t nrTh, size_t runSec, bool verbose, size_t longTxSize, size_t nrOp, size_t nrWr)
{
    std::vector<Mutex> muV(nrMutex);
    bool start = false;
    bool quit = false;
    uint32_t txIdGen = 0; // QQQ
    cybozu::thread::ThreadRunnerSet thS;
    std::vector<Result> resV(nrTh);
    for (size_t i = 0; i < nrTh; i++) {
        thS.add([&,i]() { resV[i] = worker(i, start, quit, muV, txIdGen, longTxSize, nrOp, nrWr); });
    }
    thS.start();
    start = true;
    for (size_t i = 0; i < runSec; i++) {
        if (verbose) {
            ::printf("%zu %u\n", i, __atomic_load_n(&txIdGen, __ATOMIC_RELAXED));
        }
        sleepMs(1000);
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
    ::printf("mode:%s longTxSize:%zu nrMutex:%zu concurrency:%zu nrOp:%zu nrWr:%zu sec:%zu tps:%.03f %s\n"
#ifdef USE_OCC_MCS
             , "silo-occ"
#else
             , "silo-occ-nonmcs"
#endif
             , longTxSize, nrMutex, nrTh, nrOp, nrWr, runSec, res.nrCommit() / (double)runSec
             , res.str().c_str());
    ::fflush(::stdout);
}


int main()
{
#if 1
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
