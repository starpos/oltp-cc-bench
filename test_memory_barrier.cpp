/**
 * Test of memory barrier behavior.
 */
#include "atomic_wrapper.hpp"
#include "cache_line_size.hpp"
#include "arch.hpp"
#include "sleep.hpp"
#include "util.hpp"
#include "thread_util.hpp"
#include "random.hpp"
#include "process.hpp"

#include <thread>
#include <vector>
#include <cinttypes>
#include <cstdio>


struct Shared
{
    alignas(CACHE_LINE_SIZE)
    bool ready;
    alignas(CACHE_LINE_SIZE)
    size_t nr_ready;
    alignas(CACHE_LINE_SIZE)
    bool done;
    alignas(CACHE_LINE_SIZE)
    size_t nr_done;
    alignas(CACHE_LINE_SIZE)
    bool quit;

    alignas(CACHE_LINE_SIZE)
    uint64_t x;
    alignas(CACHE_LINE_SIZE)
    uint64_t y;

    Shared(): ready(false), nr_ready(0), done(false), nr_done(0)
            , quit(false), x(0), y(0) {
    }
};


struct Local
{
    alignas(CACHE_LINE_SIZE)
    uint64_t observed[2];

    Local() : observed() {}
};



void wait_for_value(const bool& value)
{
    while (!load_acquire(value)) _mm_pause();
}



INLINE void do_store_load(uint64_t& x, const uint64_t& y, uint64_t& z0)
{
#if 0
    __atomic_thread_fence(__ATOMIC_SEQ_CST); // dmb ish
    acq_rel_fence(); // dmb ish
    acquire_fence(); // dmb ishld
    release_fence(); // dmb ish
    __asm__ volatile ("dmb ish\n\t" :::);   // any-any barrier.
    __asm__ volatile ("dmb ishld\n\t" :::); // load-load and load-store barrier.
    __asm__ volatile ("dmb ishst\n\t" :::); // store-store barrier.
#endif
    store(x, 1);
    z0 = load(y);
}


INLINE void do_load_store(const uint64_t& x, uint64_t& y, uint64_t& z0)
{
    z0 = load(x);
    store(y, 1);
}


INLINE void do_store_store(uint64_t& x, uint64_t& y)
{
    store(x, 1);
    //__asm__ volatile ("dmb ishst\n\t" :::); // store-store barrier.
    //__asm__ volatile ("dmb ish\n\t" :::);   // any-any barrier.
    //__asm__ volatile ("dmb ishld\n\t" :::); // load-load and load-store barrier.
    store(y, 1);
}


INLINE void do_load_load(const uint64_t& x, const uint64_t& y, uint64_t& z0, uint64_t& z1)
{
    z0 = load(x);
    z1 = load(y);
}


void set_random_affinity()
{
    size_t nr_cpu = cybozu::process::get_nr_processors();
    size_t affinity_nr = cybozu::util::Random<size_t>(0, nr_cpu - 1)();
    cybozu::thread::setThreadAffinity(::pthread_self(), affinity_nr);
}



enum class ExprType
{
    StoreLoad,
    LoadStore,
    StoreStore, // worker0 StoreStore, worekr1 LoadLoad.
};



template <ExprType expr_type>
void worker0(size_t i, Shared& shared, Local& local)
{
    set_random_affinity();

    unused(i);
    while (!load_acquire(shared.quit)) {
        fetch_add(shared.nr_ready, 1);
        wait_for_value(shared.ready);

        uint64_t z0 = 0, z1 = 0;
        if (expr_type == ExprType::StoreLoad) {
            do_store_load(shared.x, shared.y, z0);
        } else if (expr_type == ExprType::LoadStore) {
            do_load_store(shared.x, shared.y, z0);
        } else {
            do_store_store(shared.x, shared.y);
        }

        //sleep_us(1);
        local.observed[0] = z0;
        local.observed[1] = z1;
        fetch_add(shared.nr_done, 1);
        wait_for_value(shared.done);
    }
}

template <ExprType expr_type>
void worker1(size_t i, Shared& shared, Local& local)
{
    set_random_affinity();

    unused(i);
    while (!load_acquire(shared.quit)) {
        fetch_add(shared.nr_ready, 1);
        wait_for_value(shared.ready);

        uint64_t z0 = 0, z1 = 0;
        if (expr_type == ExprType::StoreLoad) {
            do_store_load(shared.y, shared.x, z0);
        } else if (expr_type == ExprType::LoadStore) {
            do_load_store(shared.y, shared.x, z0);
        } else {
            do_load_load(shared.y, shared.x, z1, z0);
        }

        //sleep_us(1);
        local.observed[0] = z0;
        local.observed[1] = z1;
        fetch_add(shared.nr_done, 1);
        wait_for_value(shared.done);
    }
}


void wait_for_value(const size_t& value, size_t count)
{
    while (load_acquire(value) < count) _mm_pause();
}


template <ExprType expr_type>
void expr(size_t nr_loop, std::vector<size_t>& observed_count)
{
    std::vector<std::thread> th_v;

    Shared shared;
    std::vector<Local> local_v(2);
    th_v.emplace_back(worker0<expr_type>, 0, std::ref(shared), std::ref(local_v[0]));
    th_v.emplace_back(worker1<expr_type>, 1, std::ref(shared), std::ref(local_v[1]));

    for (size_t i = 0; i < nr_loop; i++) {
        wait_for_value(shared.nr_ready, 2); // wait
        //sleep_us(1);
        store_release(shared.nr_ready, 0);
        store_release(shared.done, false);
        store_release(shared.ready, true); // notify
        // worker critical section.
        wait_for_value(shared.nr_done, 2); // wait
        store_release(shared.nr_done, 0);
        store_release(shared.ready, false);
        store_release(shared.x, 0);
        store_release(shared.y, 0);

        /* get results.
         *
         * storeload: 00 (idx == 0) impossible unless the reorder occurs.
         * loadstore: 11 (idx == 3) impossible unless the reorder occurs.
         *
         * storestore+loadload: 01 (idx == 1) impossible unless reorder occurs.
         */
        size_t idx;
        if (expr_type != ExprType::StoreStore) {
            idx = local_v[0].observed[0] * 2 + local_v[1].observed[0];
        } else {
            idx = local_v[1].observed[0] * 2 + local_v[1].observed[1];
        }
        observed_count[idx]++;
        if (idx >= 4) {
            ::printf("error idx %zu\n", idx);
            ::exit(1);
        }

        if (i == nr_loop - 1) store_release(shared.quit, true);
        store_release(shared.done, true); // notify
    }
    for (auto& th: th_v) th.join();
}


template <ExprType expr_type>
void print_observed_count(const std::vector<size_t>& observed_count)
{
    ::printf("00 %zu\n"
             "01 %zu\n"
             "10 %zu\n"
             "11 %zu\n"
             , observed_count[0]
             , observed_count[1]
             , observed_count[2]
             , observed_count[3]);

    size_t idx;
    if (expr_type == ExprType::StoreLoad) idx = 0;
    else if (expr_type == ExprType::LoadStore) idx = 3;
    else idx = 1;

    ::printf("reorder observed: %zu\n", size_t(observed_count[idx] != 0));
}

int main()
{
    constexpr ExprType expr_type = ExprType::StoreLoad;

    // 00 --> 0, 01 --> 1, 10 --> 2, 11 --> 3
    std::vector<size_t> observed_count(4);

    for (size_t i = 0; i < 10; i++) {
        ::printf("expr %zu\n", i);
        expr<expr_type>(1000000, observed_count);
        print_observed_count<expr_type>(observed_count);
    }
}
