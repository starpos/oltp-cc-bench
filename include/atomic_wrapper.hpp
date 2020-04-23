/**
 * Thin wrappers of atomic builtin.
 */
#pragma once
#include <cinttypes>
#include <type_traits>
#include <cstring>
#include "inline.hpp"


/**
 * Convert a trivially_copyable type to the corresponding unsigned type
 * for builtin atomic operations to support non-scalar types.
 */
template <typename T, typename Enable = void>
struct to_uint_type {};

template <typename T>
struct to_uint_type<T, std::enable_if_t<sizeof(T) == 1> > { using type = uint8_t; };

template <typename T>
struct to_uint_type<T, std::enable_if_t
                    <sizeof(T) == 2 && alignof(uint16_t) <= alignof(T)> >
{ using type = uint16_t; };

template <typename T>
struct to_uint_type<T, std::enable_if_t
                    <sizeof(T) == 4 && alignof(uint32_t) <= alignof(T)> >
{ using type = uint32_t; };


template <typename T>
struct to_uint_type<T, std::enable_if_t
                    <sizeof(T) == 8 && alignof(uint64_t) <= alignof(T)> >
{ using type = uint64_t; };

template <typename T>
struct to_uint_type<T, std::enable_if_t
                    <sizeof(T) == 16 && alignof(uint64_t) <= alignof(T)> >
{ using type = __uint128_t; };


template <typename T>
INLINE T load(const T& m, int order = __ATOMIC_RELAXED)
{
    static_assert(std::is_trivially_copyable_v<T>);
    using Int = typename to_uint_type<T>::type;
    Int tmp = __atomic_load_n((Int*)&m, order);
    return *(T*)&tmp;
}


template <typename T>
INLINE T load_acquire(const T& m)
{
    return load(m, __ATOMIC_ACQUIRE);
}


template <typename T0, typename T1>
INLINE void store(T0& m, T1 v, int order = __ATOMIC_RELAXED)
{
    static_assert(std::is_trivially_copyable_v<T0>);
    static_assert(std::is_trivially_copyable_v<T1>);
    using Int = typename to_uint_type<T0>::type;
    __atomic_store_n((Int*)&m, (Int)v, order);
}


template <typename T0, typename T1>
INLINE void store_release(T0& m, T1 v)
{
    store(m, v, __ATOMIC_RELEASE);
}


template <typename T0, typename T1>
INLINE T0 exchange(T0& m, T1 v, int mode = __ATOMIC_ACQ_REL)
{
    static_assert(std::is_trivially_copyable_v<T0>);
    static_assert(std::is_trivially_copyable_v<T1>);
    using Int = typename to_uint_type<T0>::type;
    Int tmp = __atomic_exchange_n((Int*)&m, (Int)v, mode);
    return *(T0*)&tmp;
}


template <typename T0, typename T1>
INLINE T0 exchange_acquire(T0& m, T1 v) { return exchange(m, v, __ATOMIC_ACQUIRE); }

template <typename T0, typename T1>
INLINE T0 exchange_release(T0& m, T1 v) { return exchange(m, v, __ATOMIC_RELEASE); }


template <typename T0, typename T1>
INLINE bool compare_exchange(
    T0& m, T0& before, T1 after,
    int mode = __ATOMIC_ACQ_REL, int fail_mode = __ATOMIC_ACQUIRE)
{
    static_assert(std::is_trivially_copyable_v<T0>);
    static_assert(std::is_trivially_copyable_v<T1>);
    using Int = typename to_uint_type<T0>::type;
    return __atomic_compare_exchange_n(
        (Int*)&m, (Int*)&before, (Int)after, false, mode, fail_mode);
}


template <typename T0, typename T1>
INLINE bool compare_exchange_acquire(T0& m, T0& before, T1 after)
{
    return compare_exchange(m, before, after, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
}


template <typename T0, typename T1>
INLINE bool compare_exchange_release(T0& m, T0& before, T1 after)
{
    return compare_exchange(m, before, after, __ATOMIC_RELEASE, __ATOMIC_RELAXED);
}


template <typename T0, typename T1>
INLINE bool compare_exchange_relaxed(T0& m, T0& before, T1 after)
{
    return compare_exchange(m, before, after, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
}


template <typename T0, typename T1>
INLINE T0 fetch_add(T0& m, T1 v, int mode = __ATOMIC_ACQ_REL)
{
    static_assert(std::is_trivially_copyable_v<T0>);
    static_assert(std::is_trivially_copyable_v<T1>);
    using Int = typename to_uint_type<T0>::type;
    return (T0)__atomic_fetch_add((Int*)&m, (Int)v, mode);
}

template <typename T0, typename T1>
INLINE T0 fetch_add_acq(T0& m, T1 v) { return fetch_add(m, v, __ATOMIC_ACQUIRE); }

template <typename T0, typename T1>
INLINE T0 fetch_add_rel(T0& m, T1 v) { return fetch_add(m, v, __ATOMIC_RELEASE); }

template <typename T0, typename T1>
INLINE T0 fetch_add_relaxed(T0& m, T1 v) { return fetch_add(m, v, __ATOMIC_RELAXED); }


template <typename T0, typename T1>
INLINE T0 fetch_sub(T0& m, T1 v, int mode = __ATOMIC_ACQ_REL)
{
    static_assert(std::is_trivially_copyable_v<T0>);
    static_assert(std::is_trivially_copyable_v<T1>);
    using Int = typename to_uint_type<T0>::type;
    return (T0)__atomic_fetch_sub((Int*)&m, (Int)v, mode);
}

template <typename T0, typename T1>
INLINE T0 fetch_sub_acq(T0& m, T1 v) { return fetch_sub(m, v, __ATOMIC_ACQUIRE); }

template <typename T0, typename T1>
INLINE T0 fetch_sub_rel(T0& m, T1 v) { return fetch_sub(m, v, __ATOMIC_RELEASE); }

template <typename T0, typename T1>
INLINE T0 fetch_sub_relaxed(T0& m, T1 v) { return fetch_sub(m, v, __ATOMIC_RELAXED); }


INLINE void acq_rel_fence() {
    __atomic_thread_fence(__ATOMIC_ACQ_REL);
}


INLINE void release_fence() {
    __atomic_thread_fence(__ATOMIC_RELEASE);
}


INLINE void acquire_fence() {
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
#define acqRelFence acq_rel_fence
#define releaseFence release_fence
#define acquireFence acquire_fence



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
