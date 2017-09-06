#include "thread_util.hpp"
#include "measure_util.hpp"
#include <vector>
#include <immintrin.h>
#include "atomic_x86_64.hpp"
#include "cpuid.hpp"


const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);


using uint128_t = __uint128_t;

alignas(16)
uint128_t value_ = 0;


void worker0(size_t id, size_t& success, size_t& failure, const bool &start, const bool &quit)
{
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[id]);

    size_t success0 = 0;
    size_t failure0 = 0;

    unused(id);

    while (!start) {
        _mm_pause();
    }

    uint128_t v0;
    __atomic_load(&value_, &v0, __ATOMIC_SEQ_CST);
    while (!quit) {
        uint128_t v1 = v0 + 1;
        if (__atomic_compare_exchange(&value_, &v0, &v1, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
            success0++;
            v0 = v1;
        } else {
            failure0++;
        }
    }
    //::printf("%02zu success %10zu failure %10zu\n", id, success, failure);
    success = success0;
    failure = failure0;
}


template <size_t atomic_kind>
void worker1(size_t id, size_t& success, size_t& failure, const bool &start, const bool &quit)
{
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[id]);

    size_t success0 = 0;
    size_t failure0 = 0;

    unused(id);

    while (!start) {
        _mm_pause();
    }

    uint128_t v0;
    my_atomic_load_16(value_, v0);
    while (!quit) {
        uint128_t v1 = v0 + 1;
        if (my_atomic_compare_exchange_16<atomic_kind>(value_, v0, v1)) {
            success0++;
            v0 = v1;
        } else {
            failure0++;
        }
    }
    //::printf("%02zu success %10zu failure %10zu\n", id, success, failure);
    success = success0;
    failure = failure0;
}


template <typename Worker>
void runBenchmark(Worker &&worker, const char *name, size_t nrTh, size_t nrSec)
{
    cybozu::thread::ThreadRunnerSet thSet;
    bool start = false, quit = false;
    std::vector<size_t> successV(nrTh), failureV(nrTh);

    for (size_t i = 0; i < nrTh; i++) {
        thSet.add([&,i]() {
                worker(i, successV[i], failureV[i], start, quit);
            });
    }
    thSet.start();
    start = true;
    sleepMs(1000 * nrSec);
    quit = true;
    thSet.join();

    size_t success = 0, failure = 0;
    for (size_t i = 0; i < nrTh; i++) {
        success += successV[i];
        failure += failureV[i];
        //::printf("%02zu success %10zu failure %10zu\n", i, successV[i], failureV[i]);
    }
    ::printf("%s success/s:%010zu failure/s:%010zu\n", name, success / nrSec, failure/ nrSec);
    ::fflush(::stdout);
}


int main()
{
    const size_t nrTh = 16;
    const size_t nrSec = 10;
    const size_t nrLoop = 10;
#if 0
    for (size_t i = 0; i < 3; i++) {
        runBenchmark(worker0, "worker0", nrTh, nrSec);
    }
#endif
    for (size_t atomic_kind = 0; atomic_kind < 4; atomic_kind++) {
        for (size_t i = 0; i < nrLoop; i++) {
            switch (atomic_kind) {
            case 0:
                runBenchmark(worker1<0>, "mode:atomic", nrTh, nrSec);
                break;
            case 1:
                runBenchmark(worker1<1>, "mode:sync  ", nrTh, nrSec);
                break;
            case 2:
                runBenchmark(worker1<2>, "mode:asm   ", nrTh, nrSec);
                break;
            default:
                ::exit(1);
            }
        }
    }
    return 0;
}
