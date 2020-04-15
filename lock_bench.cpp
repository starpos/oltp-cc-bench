#include "thread_util.hpp"
#include "random.hpp"
#include <unistd.h>
#include "cpuid.hpp"
#include "measure_util.hpp"
#include "lock.hpp"
#include "arch.hpp"
#include "workload_util.hpp"



using Mutex = cybozu::lock::XSMutex;
using Lock = cybozu::lock::XSLock;
using Mode = Mutex::Mode;


const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);



Result1 lockWorker(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, std::vector<Mutex>& muV, size_t longTxSize, size_t nrOp, size_t nrWr)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);
    Result1 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0), idx);

    // currently zipfian workload is not supported.
    const double theta = 0.0;
    FastZipf fastZipf(rand, theta, muV.size(), FastZipf::zeta(muV.size(), theta));
    const bool usesZipf = false;

    const bool isLongTx = (longTxSize != 0 && idx == 0); // starvation setting.
    const size_t wrRatio = size_t((double)nrWr / (double)nrOp * (double)SIZE_MAX);
    const TxMode shortTxMode = USE_MIX_TX;
    const TxMode longTxMode = USE_MIX_TX;
    auto getMode = selectGetModeFunc<decltype(rand), Mode>(isLongTx, shortTxMode, longTxMode);
    auto getRecordIdx = selectGetRecordIdx<decltype(rand)>(isLongTx, shortTxMode, longTxMode, usesZipf);

    AccessInfoVec aiV;
    aiV.resize(isLongTx ? longTxSize : nrOp);

    std::vector<Lock> lockV;

    lockV.reserve(nrOp);
    while (!start) _mm_pause();
    size_t count = 0; unused(count);
    while (!load_acquire(quit)) {
        fillAccessInfoVec(rand, fastZipf, getMode, getRecordIdx, muV.size(), wrRatio, aiV);

        // Sort must be requried to avoid dead-lock.
        std::sort(aiV.begin(), aiV.end());

#if 0 // debug
        for (const auto& ai : aiV) {
            ::printf("%s ", ai.str().c_str());
        }
        ::printf("\n");
#endif

        size_t prevKey = aiV.size() != 0 ? aiV.begin()->key + 1 : 0; // different.

        assert(lockV.empty());
        for (size_t i = 0; i < aiV.size(); i++) {
            const AccessInfo& ai = aiV[i];
            if (ai.key == prevKey) continue;
            lockV.emplace_back(&muV[ai.key], ai.is_write ? Mode::X : Mode::S);
            prevKey = ai.key;
        }
        res.incCommit(isLongTx);
        lockV.clear();

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


void runExec(size_t nrMutex, size_t nrTh, size_t runSec, bool verbose, size_t longTxSize, size_t nrOp, size_t nrWr)
{
    std::vector<Mutex> muV(nrMutex);
    bool start = false;
    bool quit = false;
    bool shouldQuit = false;

    cybozu::thread::ThreadRunnerSet thS;
    std::vector<Result1> resV(nrTh);
    for (size_t i = 0; i < nrTh; i++) {
        thS.add([&,i]() {
                resV[i] = lockWorker(i, start, quit, shouldQuit, muV, longTxSize, nrOp, nrWr);
            });
    }
    thS.start();
    start = true;
    size_t sec = 0;
    for (size_t i = 0; i < runSec; i++) {
        if (verbose) ::printf("%zu\n", i);
        sleep_ms(1000);
        sec++;
        if (shouldQuit) break;
    }
    store_release(quit, true);
    thS.join();
    Result1 res;
    for (size_t i = 0; i < nrTh; i++) {
        if (verbose) {
            ::printf("worker %zu  %s\n", i, resV[i].str().c_str());
        }
        res += resV[i];
    }
    ::printf("mode:%s longTxSize:%zu nrMutex:%zu concurrency:%zu nrOp:%zu nrWr:%zu sec:%zu tps:%.03f %s\n"
             , "lock"
             , longTxSize, nrMutex, nrTh, nrOp, nrWr, sec, res.nrCommit() / (double)sec
             , res.str().c_str());
    ::fflush(::stdout);
}


int main()
{
#if 1
    runExec(256 * 1000, 256, 10, true, 0, 10, 2);
    // runExec(256 * 1000, 1, 1, true, 0, 10, 2);
#endif
#if 0
    //for (size_t nrResPerTh : {4000}) {
    for (size_t nrResPerTh : {4, 4000}) {
        //for (size_t nrTh : {16}) {
        for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
            if (nrTh > 16 && nrTh % 2 != 0) continue;
            for (size_t i = 0; i < 20; i++) {
                bool verbose = false;
                runExec(nrResPerTh * nrTh, nrTh, 10, verbose, 0);
                //sleep_ms(1000);
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
            runExec(nrMutex, nrTh, maxSec, verbose, longTxSize);
            //sleep_ms(1000);
        }
    }
#endif
}
