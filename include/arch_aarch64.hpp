#pragma once
#include <cinttypes>
#include <cassert>
#include "inline.hpp"


/**
 * yield instruction of aarch64 is different from pause instruction of x86_64.
 * so do not use yield instruction here.
 */
INLINE void _mm_pause()
{
    __asm__ volatile ("" ::: "memory");
}



/**
 * From armv8-a, sevl/wfe instructions are available as a hint of spin loops.
 */
INLINE void aarch64_sevl()
{
    __asm__ volatile ("sevl" ::: "memory");
}


INLINE void aarch64_wfe()
{
    __asm__ volatile ("wfe" ::: "memory");
}


/**
 * You can use this as load-link.
 */
template <typename UInt>
INLINE UInt aarch64_ldaxr(const UInt& target)
{
    uint64_t val;
    switch (sizeof(UInt)) {
    case 8:
        __asm__ volatile ("ldaxr %0, [%1]" : "=&r" (val) : "r" (&target) : "memory");
        break;
    case 4:
        __asm__ volatile ("ldaxr %w0, [%1]" : "=&r" (val) : "r" (&target) : "memory");
        break;
    case 2:
        __asm__ volatile ("ldaxrh %w0, [%1]" : "=&r" (val) : "r" (&target) : "memory");
        break;
    case 1:
        __asm__ volatile ("ldaxrb %w0, [%1]" : "=&r" (val) : "r" (&target) : "memory");
        break;
    default:
        assert(true);
    }
    return UInt(val);
}


/**
 * You can use this as store-conditional.
 * return: true if success.
 */
template <typename UInt1, typename UInt2>
INLINE bool aarch64_stlxr(UInt1& target, UInt2 val)
{
    uint32_t status;
    uint64_t val2 = val;
    switch (sizeof(UInt1)) {
    case 8:
        __asm__ volatile ("stlxr %w0, %1, [%2]"
                          : "=&r" (status) : "r" (val2), "r" (&target) : "memory");
        break;
    case 4:
        __asm__ volatile ("stlxr %w0, %w1, [%2]"
                          : "=&r" (status) : "r" (val2), "r" (&target) : "memory");
        break;
    case 2:
        __asm__ volatile ("stlxrh %w0, %w1, [%2]"
                          : "=&r" (status) : "r" (val2), "r" (&target) : "memory");
        break;
    case 1:
        __asm__ volatile ("stlxrb %w0, %w1, [%2]"
                          : "=&r" (status) : "r" (val2), "r" (&target) : "memory");
        break;
    default:
        assert(true);
    }
    return status == 0;
}
