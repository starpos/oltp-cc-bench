/**
 * Test acquire/release semantics.
 * Pingpong code.
 */
#include <thread>
#include <cinttypes>
#include "atomic_wrapper.hpp"
#include "arch.hpp"
#include "cache_line_size.hpp"
#include "util.hpp"


template <typename... Args>
void print(Args&&... args)
{
    unused(args...);
#if 0
    ::printf(args...);
#endif
}


struct A
{
    alignas(CACHE_LINE_SIZE)
    uint64_t val;
};


void worker1(A* a, bool& quit)
{
    uint64_t c = 2;
    for (;;) {
        print("worker1 store %" PRIu64 "\n", c);
        store(a[0].val, c);
        store_release(a[1].val, c);

        if (load(quit)) break;
        c++;

        print("worker1 try to load %" PRIu64 "\n", c);
        while (load_acquire(a[3].val) == c - 2) _mm_pause();
        uint64_t x = load(a[2].val);
        if (x != c) {
            print("worker1 expected %" PRIu64 " but %" PRIu64 "\n", c, x);
            ::exit(1);
        }
        print("worker1 load %" PRIu64 "\n", c);

        c++;
    }

    ::printf("worker1 %" PRIu64 "\n", c);
}


void worker2(A* a, bool& quit)
{
    uint64_t c = 2;
    while (!load(quit)) {

        print("worker2 try to load %" PRIu64 "\n", c);
        while (load_acquire(a[1].val) == c - 2) _mm_pause();
        uint64_t x = load(a[0].val);
        if (x != c) {
            print("worker2 expected %" PRIu64 " but %" PRIu64 "\n", c, x);
            ::exit(1);
        }
        print("worker2 load %" PRIu64 "\n", c);

        c++;

        print("worker2 store %" PRIu64 "\n", c);
        store(a[2].val, c);
        store_release(a[3].val, c);

        c++;
    }

    ::printf("worker2 %" PRIu64 "\n", c);
}


int main()
{
    const size_t nr_loop = 100;
    const size_t run_period_ms = 5000;

    for (size_t i = 0; i < nr_loop; i++) {
        ::printf("loop %zu\n", i);
        A a[4];
        a[0].val = 0;
        a[1].val = 0;
        a[2].val = 1;
        a[3].val = 1;
        alignas(64) bool quit = false;

        std::thread th1(worker1, std::ref(a), std::ref(quit));
        std::thread th2(worker2, std::ref(a), std::ref(quit));
        std::this_thread::sleep_for(std::chrono::milliseconds(run_period_ms));
        store_release(quit, true);
        th1.join();
        th2.join();
    }
}
