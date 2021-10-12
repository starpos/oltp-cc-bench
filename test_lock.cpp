#include "atomic_wrapper.hpp"
#include "arch.hpp"
#include <atomic>
#include <cinttypes>


void lock(uint8_t& mutex)
{
    while (exchange(mutex, 1)) _mm_pause();
}


void lock2(uint8_t& mutex)
{
    while (test_and_set(mutex)) _mm_pause();
}


void lock3(std::atomic<uint8_t>& mutex)
{
    while (mutex.exchange(1, std::memory_order_acquire)) _mm_pause();
}


int main() { return 0; }
