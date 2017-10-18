#include <immintrin.h>
#include <unistd.h>
#include <vector>
#include "thread_util.hpp"
#include "random.hpp"
#include "cpuid.hpp"
#include "measure_util.hpp"
#include "tx_util.hpp"
#include "lock.hpp"
#include "pqlock.hpp"


enum class LockType : uint8_t
{
    PQSpin, PQPosix, PQMcs1, PQMcs2, PQMcs3, PQ1993, PQ1997,
};


const char *getPQLockName(LockType lkType)
{
    std::pair<LockType, const char *> table[] = {
        {LockType::PQSpin,  "pqspin"},
        {LockType::PQPosix, "pqposix"},
        {LockType::PQMcs1,  "pqmcs1"},
        {LockType::PQMcs2,  "pqmcs2"},
        {LockType::PQMcs3,  "pqmcs3"},
        {LockType::PQ1993,  "pq1993"},
        {LockType::PQ1997,  "pq1997"},
    };
    const size_t nr = sizeof(table) / sizeof(table[0]);

    for (size_t i = 0; i < nr; i++) {
        if (lkType == table[i].first) return table[i].second;
    }
    return "unknown";
}


const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);


struct Resource
{
    struct CacheLine {
        union {
            alignas(CACHE_LINE_SIZE)
            char bytes[CACHE_LINE_SIZE];
            uint64_t count;
        };
    };
    std::vector<CacheLine> vec;
    void resize(size_t s) { vec.resize(s); }
    void update() {
        for (CacheLine& cl : vec) cl.count++;
    }
};


template <typename PQLock, typename TxIdGen>
size_t worker(size_t idx, bool& start, bool& quit, std::vector<typename PQLock::Mutex>& muV,
              std::vector<Resource>& resV, TxIdGen& txIdGen)
{
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
#if 0
    std::vector<size_t> muIdV(4);
#else
    std::vector<size_t> muIdV(1);
#endif
#if 0
    std::vector<PQLock> lockV;
#endif
    size_t c = 0;

    while (!start) _mm_pause();
    while (!quit) {
        fillMuIdVecLoop(muIdV, rand, muV.size());
        std::sort(muIdV.begin(), muIdV.end());

#if 0
        const uint32_t txId = txIdGen.get();
#else
        unused(txIdGen);
        const uint32_t txId = rand();
#endif

#if 0
        for (size_t i = 0; i < muIdV.size(); i++) {
            lockV.emplace_back();
            lockV.back().lock(&muV[muIdV[i]], txId);
        }
#else
        PQLock lk0(&muV[muIdV[0]], txId);
#if 0
        PQLock lk1(&muV[muIdV[1]], txId);
        PQLock lk2(&muV[muIdV[2]], txId);
        PQLock lk3(&muV[muIdV[3]], txId);
#endif
#endif

#if 0
        sleepMs(500);
#endif
        unused(resV);
#if 0
        resV[muIdV[0]].update();
#if 0
        resV[muIdV[1]].update();
        resV[muIdV[2]].update();
        resV[muIdV[3]].update();
#endif
#endif

        c++;
#if 0
        lockV.clear();
#else
#endif
    }
    return c;
}


template <typename PQLock>
void runExecT(size_t nrRes, size_t nrTh, size_t runSec, bool verbose, LockType lkType)
{
    std::vector<typename PQLock::Mutex> muV(nrRes);
    std::vector<Resource> resV(nrRes);
    for (Resource& r : resV) {
        r.resize(128);
    }
#if 1
    GlobalTxIdGenerator txIdGen(6, 12);
#else
    SimpleTxIdGenerator txIdGen;
#endif
    bool start = false, quit = false;
    cybozu::thread::ThreadRunnerSet thS;
    std::vector<size_t> cV(nrTh);
    for (size_t i = 0; i < nrTh; i++) {
        thS.add([&,i]() {
#if 1
            TxIdGenerator localGen(&txIdGen);
#else
            SimpleTxIdGenerator &localGen = txIdGen;
#endif
            cV[i] = worker<PQLock>(i, start, quit, muV, resV, localGen);
        });
    }
    thS.start();
    start = true;
    for (size_t i = 0; i < runSec; i++) {
        if (verbose) {
            ::printf("%zu %u\n", i, txIdGen.sniff());
        }
        sleepMs(1000);
    }
    quit = true;
    thS.join();
#if 0
    size_t dummyAlloc = 0, dummyFree = 0;
    for (const PQLock::Mutex& mu : muV) {
        dummyAlloc += mu.dummyAlloc;
        dummyFree += mu.dummyFree;
    }
    ::printf("total dummy alloc %zu free %zu\n", dummyAlloc, dummyFree);
#endif
    size_t total = 0;
    for (size_t i = 0; i < nrTh; i++) {
        if (verbose) {
            ::printf("worker %zu count %zu\n", i, cV[i]);
        }
        total += cV[i];
    }
    ::printf("mode:%s mutex:%zu  concurrency:%zu  ops:%.03f  total:%zu\n"
             , getPQLockName(lkType), nrRes, nrTh, total / (double)runSec, total);
    ::fflush(::stdout);
}


void runExec(size_t nrRes, size_t nrTh, size_t runSec, bool verbose, LockType lkType)
{
    switch (lkType) {
    case LockType::PQSpin:
        runExecT<cybozu::lock::PQSpinLock>(nrRes, nrTh, runSec, verbose, lkType);
        break;
    case LockType::PQPosix:
        runExecT<cybozu::lock::PQPosixLock>(nrRes, nrTh, runSec, verbose, lkType);
        break;
    case LockType::PQMcs1:
        runExecT<cybozu::lock::PQMcsLock>(nrRes, nrTh, runSec, verbose, lkType);
        break;
    case LockType::PQMcs2:
        runExecT<cybozu::lock::PQMcsLock2>(nrRes, nrTh, runSec, verbose, lkType);
        break;
    case LockType::PQMcs3:
        runExecT<cybozu::lock::PQMcsLock3>(nrRes, nrTh, runSec, verbose, lkType);
        break;
    case LockType::PQ1993:
        runExecT<cybozu::lock::PQ1993Lock>(nrRes, nrTh, runSec, verbose, lkType);
        break;
    case LockType::PQ1997:
        runExecT<cybozu::lock::PQ1997Lock>(nrRes, nrTh, runSec, verbose, lkType);
        break;
    default:
        throw std::runtime_error("no such lock type.");
    }
}


int main() try
{
#if 1
    //for (LockType lkType : {LockType::PQSpin, LockType::PQPosix,
    //LockType::PQMcs1, LockType::PQMcs2, LockType::PQMcs3, LockType::PQ1993, LockType::PQ1997}) {
    for (LockType lkType : {LockType::PQSpin, LockType::PQPosix,
                LockType::PQMcs1, LockType::PQMcs2, LockType::PQMcs3, LockType::PQ1993}) {
        //for (LockType lkType : {LockType::PQMcs1, LockType::PQMcs2, LockType::PQMcs3}) {
        //for (LockType lkType : {LockType::PQMcs3}) {
        //for (size_t nrRes : {1, 10, 100, 1000, 10000}) {
        for (size_t nrRes : {1, 2, 4, 1024}) {
            //for (size_t nrTh : {1, 2, 4, 8, 16, 24, 32}) {
            //for (size_t nrTh : {2}) {
            //for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
            for (size_t nrTh : {1, 2, 4, 6, 8, 12, 16, 20, 24, 28, 32}) {
                size_t nrLoop = 10;
                for (size_t i = 0; i < nrLoop; i++) {
                    size_t periodSec = 10;
                    runExec(nrRes, nrTh, periodSec, false, lkType);
                }
            }
        }
    }
#endif
#if 0
    for (size_t nrRes : {1}) {
        for (size_t nrTh : {24}) {
            //runExec(nrRes, nrTh, 10, true, LockType::PQMcs2);
            runExec(nrRes, nrTh, 30, true, LockType::PQ1997);
        }
    }
#endif
} catch (std::exception& e) {
    ::fprintf(::stderr, "Error: %s\n", e.what());
}
