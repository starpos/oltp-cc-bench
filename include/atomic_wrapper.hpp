#pragma once

// Thin wrappers of atomic builtin.

template <typename Int>
Int load(Int& m)
{
    return __atomic_load_n(&m, __ATOMIC_RELAXED);
}


template <typename Int>
Int load_acquire(Int& m)
{
    return __atomic_load_n(&m, __ATOMIC_ACQUIRE);
}


template <typename Int0, typename Int1>
void store(Int0& m, Int1 v)
{
    __atomic_store_n(&m, (Int0)v, __ATOMIC_RELAXED);
}


template <typename Int0, typename Int1>
void store_release(Int0& m, Int1 v)
{
    __atomic_store_n(&m, (Int0)v, __ATOMIC_RELEASE);
}


template <typename Int0, typename Int1>
Int0 exchange(Int0& m, Int1 v, int mode = __ATOMIC_ACQ_REL) {
    return __atomic_exchange_n(&m, v, mode);
}


#define exchange_acquire(m, v) exchange(m, v, __AOTMIC_ACQUIRE)
#define exchange_release(m, v) exchange(m, v, __AOTMIC_RELEASE)


template <typename Int0, typename Int1>
bool compare_exchange(Int0& m, Int0& before, Int1 after, int mode = __ATOMIC_ACQ_REL, int fail_mode = __ATOMIC_ACQUIRE) {
    return __atomic_compare_exchange_n(&m, &before, (Int0)after, false, mode, fail_mode);
}


#define compare_exchange_acquire(m, b, a) compare_exchange(m, b, a, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)
#define compare_exchange_release(m, b, a) compare_exchange(m, b, a, __ATOMIC_RELEASE, __ATOMIC_RELAXED)


template <typename Int0, typename Int1>
Int0 fetch_add(Int0& m, Int1 v, int mode = __ATOMIC_ACQ_REL)
{
    return __atomic_fetch_add(&m, static_cast<Int0>(v), mode);
}


template <typename Int0, typename Int1>
Int0 fetch_sub(Int0& m, Int1 v, int mode = __ATOMIC_ACQ_REL)
{
    return __atomic_fetch_sub(&m, static_cast<Int0>(v), mode);
}


void acq_rel_memory_barrier() {
    __atomic_thread_fence(__ATOMIC_ACQ_REL);
}


void release_memory_barrier() {
    __atomic_thread_fence(__ATOMIC_RELEASE);
}


void acquire_memory_barrier() {
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
}


/**
 * For compatibility.
 */
#define storeRelease store_release
#define loadAcquire load_acquire
#define compareExchange compare_exchange
#define fetchAdd fetch_add
#define fetchSub fetch_sub
#define memoryBarrier memory_barrier
#define releaseMemoryBarrier release_memory_barrier
#define acquireMemoryBarrier acquire_memory_barrier



#define COMPILER_FENCE() __asm__ volatile("" ::: "memory")

/*
 * In x86_64, CAS and load is not reordered.
 * In aarch64, ldar/stlr instructions are not reordered.
 * So in the both architectures,
 * it is not required explicit
 * instruction memory barriers at serialization point
 * for several OCC protocols.
 */
#define SERIALIZATION_POINT_BARRIER() COMPILER_FENCE()
