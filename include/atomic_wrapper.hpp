#pragma once

// Thin wrappers of atomic builtin.

template <typename Int>
Int load(Int& m) {
    return __atomic_load_n(&m, __ATOMIC_RELAXED);
}

template <typename Int>
Int loadAcquire(Int& m) {
    return __atomic_load_n(&m, __ATOMIC_ACQUIRE);
}

template <typename Int0, typename Int1>
void store(Int0& m, Int1 v) {
    __atomic_store_n(&m, (Int0)v, __ATOMIC_RELAXED);
}

template <typename Int0, typename Int1>
void storeRelease(Int0& m, Int1 v) {
    __atomic_store_n(&m, (Int0)v, __ATOMIC_RELEASE);
}

template <typename Int0, typename Int1>
Int0 exchange(Int0& m, Int1 v, int mode) {
    return __atomic_exchange_n(&m, v, mode);
}

template <typename Int0, typename Int1>
bool compareExchange(Int0& m, Int0& before, Int1 after, int mode) {
    return __atomic_compare_exchange_n(&m, &before, (Int0)after, false, mode, __ATOMIC_RELAXED);
}

template <typename Int0, typename Int1>
Int0 fetchAdd(Int0& m, Int1 v, int mode = __ATOMIC_RELAXED)
{
    return __atomic_fetch_add(&m, static_cast<Int0>(v), mode);
}

template <typename Int0, typename Int1>
Int0 fetchSub(Int0& m, Int1 v, int mode = __ATOMIC_RELAXED)
{
    return __atomic_fetch_sub(&m, static_cast<Int0>(v), mode);
}

void memoryBarrier() {
    __atomic_thread_fence(__ATOMIC_ACQ_REL);
}

void releaseMemoryBarrier() {
    __atomic_thread_fence(__ATOMIC_RELEASE);
}

void acquireMemoryBarrier() {
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
}
