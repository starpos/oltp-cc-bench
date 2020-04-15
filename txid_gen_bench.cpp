#include <ctime>
#include <vector>
#include <chrono>
#include "thread_util.hpp"
#include "random.hpp"
#include "tx_util.hpp"
#include "measure_util.hpp"
#include "cpuid.hpp"
#include "arch.hpp"


const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);


template <typename TxIdGen>
size_t worker(size_t idx, bool& start, bool& quit, std::vector<TxIdGen>& txIdGen)
{
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);
    size_t c = 0;
    size_t total = 0;
    while (!start) _mm_pause();
    while (!quit) {
        for (size_t i = 0; i < txIdGen.size(); i++) {
            total += txIdGen[i].get();
        }
        c++;
    }
    return c;
}


void runExec(size_t nrTh, uint8_t allocBits, size_t nrTxIdGen, size_t runSec, bool verbose)
{
    bool start = false;
    bool quit = false;
#if 0
    std::vector<GlobalTxIdGenerator> txIdGen(nrTxIdGen);
    for (GlobalTxIdGenerator& g : txIdGen) g.init(5, allocBits);
#else
    std::vector<SimpleTxIdGenerator> txIdGen(nrTxIdGen);
#endif

    cybozu::thread::ThreadRunnerSet thS;
    std::vector<size_t> cV(nrTh);
    for (size_t i = 0; i < nrTh; i++) {
        thS.add([&,i]() {
#if 0
                std::vector<TxIdGenerator> localGen(nrTxIdGen);
                for (size_t j = 0; j < nrTxIdGen; j++) localGen[j].init(&txIdGen[j]);
#else
                std::vector<SimpleTxIdGenerator>& localGen = txIdGen;
#endif
                cV[i] = worker(i, start, quit, localGen);
        });
    }
    thS.start();
    start = true;
    for (size_t i = 0; i < runSec; i++) {
        if (verbose) {
            ::printf("%zu %u\n", i, txIdGen[0].sniff());
        }
        sleep_ms(1000);
    }
    quit = true;
    thS.join();
    size_t total = 0;
    for (size_t i = 0; i < nrTh; i++) {
        if (verbose) {
            ::printf("worker %zu count %zu\n", i, cV[i]);
        }
        total += cV[i];
    }
    ::printf("concurrency %zu  txidbulk %zu  total %zu  throughput %.03f tps\n"
             , nrTh, uint64_t(1) << allocBits, total, total / (double)runSec);
    ::fflush(::stdout);
}


size_t worker2(size_t idx, bool& start, bool& quit)
{
    unused(idx);
    uint32_t txIdGen = 0;
    size_t c = 0;
    while (!start) _mm_pause();
    while (!quit) {
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
        uint32_t txId = txIdGen++;
        unused(txId);
        c++;
    }
    return c;
}


void runExec2(size_t nrTh, size_t runSec, bool verbose)
{
    bool start = false;
    bool quit = false;

    cybozu::thread::ThreadRunnerSet thS;
    std::vector<size_t> cV(nrTh);
    for (size_t i = 0; i < nrTh; i++) {
        thS.add([&,i]() {
                cV[i] = worker2(i, start, quit);
            });
    }
    thS.start();
    start = true;
    for (size_t i = 0; i < runSec; i++) {
        if (verbose) {
            ::printf("%zu\n", i);
        }
        sleep_ms(1000);
    }
    quit = true;
    thS.join();
    size_t total = 0;
    for (size_t i = 0; i < nrTh; i++) {
        if (verbose) {
            ::printf("worker %zu count %zu\n", i, cV[i]);
        }
        total += cV[i];
    }
    ::printf("concurrency %zu  total %zu  throughput %.03f tps\n"
             , nrTh, total, total / (double)runSec);
    ::fflush(::stdout);
}


int main()
{
#if 1
    for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
        for (size_t i = 0; i < 10; i++) {
            runExec(nrTh, 0, 1, 10, false);
        }
    }
#endif
#if 0
    for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
        for (uint8_t allocBits : {2, 6, 10, 14}) {
            for(size_t i = 0; i < 10; i++) {
                runExec(nrTh, allocBits, 1, 10, false);
                sleep_ms(1000);
            }
        }
    }
#endif
#if 0
    for (size_t nrTh = 1; nrTh <= 24; nrTh++) {
        for(size_t i = 0; i < 10; i++) {
            runExec(nrTh, 0, 10, false);
            sleep_ms(1000);
        }
    }
#endif
#if 0
    runExec(12, 12, 10, true);
#endif


#if 0
    for (size_t nrTh = 1; nrTh <= 24; nrTh++) {
        for(size_t i = 0; i < 10; i++) {
            runExec2(nrTh, 10, false);
            sleep_ms(1000);
        }
    }
#endif
#if 0
    runExec2(12, 10, true);
#endif
}
