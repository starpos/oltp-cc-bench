#include <cstdio>
#include <chrono>
#include <vector>
#include "thread_util.hpp"
#include "arch.hpp"

template <typename T>
void unused(T&)
{
}

size_t worker(size_t idx, bool& start, bool&quit, uint64_t& val)
{
    unused(idx);
    size_t c = 0;
    while (!start) _mm_pause();
    while (!quit) {
        __atomic_fetch_add(&val, 1, __ATOMIC_RELAXED);
        c++;
    }
    return c;
}

void runExpr(size_t nrTh, size_t runSec, bool verbose)
{
    uint64_t val = 0;
    bool start = false;
    bool quit = false;
    cybozu::thread::ThreadRunnerSet thS;
    std::vector<size_t> cV(nrTh);
    for (size_t i = 0; i < nrTh; i++) {
        thS.add([&,i]() { cV[i] = worker(i, start, quit, val); });
    }
    thS.start();
    start = true;
    for (size_t i = 0; i < runSec; i++) {
        if (verbose) {
            ::printf("%03zu  %zu\n", i, val);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    quit = true;
    thS.join();
    size_t total = 0;
    for (size_t i = 0; i < nrTh; i++) {
        if (verbose) {
            ::printf("worker %zu  count %zu\n", i, cV[i]);
        }
        total += cV[i];
    }
    //::printf("total count %zu\n", total);
    ::printf("atomicInc  concurrency %zu  throughput %.3f op/sec  total %zu\n"
             , nrTh, total / (double)runSec, total);
    ::fflush(::stdout);
}

int main()
{
    for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
        for (size_t i = 0; i < 10; i++) {
            runExpr(nrTh, 10, false);
        }
    }
}
