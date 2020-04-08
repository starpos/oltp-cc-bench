#pragma once
#include <functional>
#include "arch.hpp"
#include "inline.hpp"
#include "atomic_wrapper.hpp"
#include "template_util.hpp"



/**
 * spinwait_until() uses sevl/wfe for aarch64 architecture.
 * Otherwise _mm_pasue() is used.
 */
#if defined(__aarch64__)


/**
 * Predicate: you can choose std::equal_to, std::not_equal_to, std::less, and so on.
 * Your own predicate type must have default constructor.
 */
template <typename UInt1, typename UInt2, typename Predicate>
INLINE void spinwait_until(const UInt1& val, UInt2 expected)
{
    Predicate pred;
    if (pred(load_acquire(val), expected)) return;
    aarch64_sevl();
    do {
        aarch64_wfe();
    } while (!pred(aarch64_ldaxr<UInt1>(val), expected));
}


/**
 * You can pass your own predicate function.
 * The type of the predicate should be
 * bool(const UInt1&, const UInt2&) or bool(UInt1, UInt2).
 */
template <typename UInt1, typename UInt2, typename Predicate>
INLINE void spinwait_until(const UInt1& val, UInt2 expected, Predicate&& pred)
{
    if (pred(load_acquire(val), expected)) return;
    aarch64_sevl();
    do {
        aarch64_wfe();
    } while (!pred(aarch64_ldaxr<UInt1>(val), expected));
}


#else


template <typename UInt1, typename UInt2, typename Predicate> >
INLINE void spinwait_until(const UInt1& val, UInt2 expected)
{
    Predicate pred;
    while (!pred(load_acquire(val), expected)) _mm_pause();
}


template <typename UInt1, typename UInt2, typename Predicate>
INLINE void spinwait_until(const UInt1& val, UInt2 expected, Predicate&& pred)
{
    while (!pred(load_acquire(val), expected)) _mm_pause();
}

#endif


/*
 * Common wrapper functions.
 */


template <typename UInt1, typename UInt2>
INLINE void spinwait_until_equal(const UInt1& val, UInt2 expected)
{
    using EqualTo = std::equal_to<typename get_large_type<UInt1, UInt2>::type>;
    spinwait_until<UInt1, UInt2, EqualTo>(val, expected);
}


template <typename UInt1, typename UInt2>
INLINE void spinwait_until_not_equal(const UInt1& val, UInt2 expected)
{
    using NotEqualTo = std::not_equal_to<typename get_large_type<UInt1, UInt2>::type>;
    spinwait_until<UInt1, UInt2, NotEqualTo>(val, expected);
}
