#include <cstdio>
#include <chrono>
#include <vector>
#include "thread_util.hpp"
#include "arch.hpp"
#include "util.hpp"
#include "atomic_wrapper.hpp"
#include "cache_line_size.hpp"


size_t worker(size_t idx, bool& start, bool& quit, uint64_t& val)
{
    unused(idx);
    size_t c = 0;
    while (!load_acquire(start)) _mm_pause();
    while (!load_acquire(quit)) {
        fetch_add(val, 1);
        c++;
    }
    return c;
}


void run_expr(size_t nrTh, size_t runSec, bool verbose)
{
    alignas(CACHE_LINE_SIZE)
    uint64_t val = 0;
    alignas(CACHE_LINE_SIZE)
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
    for (size_t nrTh = 1; nrTh <= 16; nrTh++) {
        for (size_t i = 0; i < 3; i++) {
            run_expr(nrTh, 10, false);
        }
    }
}
