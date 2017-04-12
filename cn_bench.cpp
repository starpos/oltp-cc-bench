#include "thread_util.hpp"
#include "random.hpp"
#include <immintrin.h>
#include <unistd.h>
#include "cpuid.hpp"
#include "measure_util.hpp"
#include "counting_network.hpp"


const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);
//using CN = cybozu::util::CountingNetwork4;
using CN = cybozu::util::CountingNetwork8;

size_t worker(size_t idx, const bool& start, const bool& quit, CN& cn)
{
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);
    cybozu::util::XorShift128 rand(::time(0) + idx);
    size_t count = 0;
    uint64_t total = 0;
    while (!start) _mm_pause();
    while (!quit) {
        total += cn.get(idx);
        count++;
    }
    return count;
}


void runExec(size_t nrTh, size_t runSec, bool verbose)
{
    CN cn;
    bool start = false;
    bool quit = false;

    cybozu::thread::ThreadRunnerSet thS;
    std::vector<size_t> cV(nrTh);
    for (size_t i = 0; i < nrTh; i++) {
        thS.add([&,i]() {
                cV[i] = worker(i, start, quit, cn);
            });
    }
    thS.start();
    start = true;
    for (size_t i = 0; i < runSec; i++) {
        if (verbose) ::printf("%zu\n", i);
        sleepMs(1000);
    }
    quit = true;
    thS.join();
    size_t total = 0;
    for (size_t i = 0; i < nrTh; i++) {
        if (verbose) ::printf("worker %zu  %zu", i, cV[i]);
        total += cV[i];
    }
    ::printf("concurrency %zu  sec %5zu  throughput %.03f ops\n"
             , nrTh, runSec, total / (double)runSec);
    ::fflush(::stdout);
}


int main()
{
#if 1
    for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
        for (size_t i = 0; i < 10; i++) {
            bool verbose = false;
            runExec(nrTh, 10, verbose);
            sleepMs(1000);
        }
    }
#endif
#if 0
    runExec(10000, 12, 10, true);
#endif
}
