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


template <typename UInt>
INLINE UInt aarch64_ldaxr(const UInt& val)
{
    uint64_t old;
    switch (sizeof(UInt)) {
    case 8:
        __asm__ volatile ("ldaxr %0, [%1]" : "=&r" (old) : "r" (&val) : "memory");
        break;
    case 4:
        __asm__ volatile ("ldaxr %w0, [%1]" : "=&r" (old) : "r" (&val) : "memory");
        break;
    case 2:
        __asm__ volatile ("ldaxrh %w0, [%1]" : "=&r" (old) : "r" (&val) : "memory");
        break;
    case 1:
        __asm__ volatile ("ldaxrb %w0, [%1]" : "=&r" (old) : "r" (&val) : "memory");
        break;
    default:
        assert(true);
    }
    return UInt(old);
}
