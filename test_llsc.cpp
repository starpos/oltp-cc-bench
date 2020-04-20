#include <thread>
#include <cinttypes>
#include <vector>
#include "arch.hpp"
#include "sleep.hpp"
#include "atomic_wrapper.hpp"
#include "cache_line_size.hpp"
#include "cybozu/test.hpp"


#if defined(__aarch64__)


thread_local size_t failed_;


template <typename UInt>
void spin_lock(UInt& mutex)
{
    for (;;) {
        aarch64_sevl();
        for (;;) {
            aarch64_wfe();
            if (aarch64_ldaxr(mutex) == 0) break;
        }
        if (aarch64_stlxr(mutex, 1)) break;

        failed_++;
    }
}


template <typename UInt>
void spin_unlock(UInt& mutex)
{
    store_release(mutex, 0);
}


uint64_t total_ = 0;


void worker(const bool& quit, uint32_t& mutex, uint64_t& counter)
{
    failed_ = 0;
    uint64_t local_counter = 0;
    while (!load_acquire(quit)) {
        spin_lock(mutex);
        counter++;
        spin_unlock(mutex);
        local_counter++;
    }
    fetch_add(total_, local_counter);

    ::printf("failed %zu\n", failed_);
}


template <typename UInt>
void run_test(size_t nr_threads)
{
    alignas(CACHE_LINE_SIZE) uint64_t counter = 0;
    alignas(CACHE_LINE_SIZE) uint32_t mutex = 0;
    alignas(CACHE_LINE_SIZE) bool quit = false;
    total_ = 0;

    std::vector<std::thread> th_v;
    for (size_t i = 0; i < nr_threads; i++) {
        th_v.emplace_back(worker, std::ref(quit), std::ref(mutex), std::ref(counter));
    }
    sleep_ms(100);
    store_release(quit, true);
    for (auto& th : th_v) th.join();

    ::printf("total   %" PRIu64 "\n", total_);
    ::printf("counter %" PRIu64 "\n", counter);

    CYBOZU_TEST_EQUAL(total_, counter);
}


CYBOZU_TEST_AUTO(test_llsc)
{
    constexpr size_t nr_threads = 8;

    run_test<uint64_t>(nr_threads);
    run_test<uint32_t>(nr_threads);
    run_test<uint16_t>(nr_threads);
    run_test<uint8_t>(nr_threads);
    run_test<bool>(nr_threads);
}


#else


CYBOZU_TEST_AUTO(test_llic)
{
    // do nothing.
}


#endif
