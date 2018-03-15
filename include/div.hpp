#pragma once
#include <cinttypes>
#include <cstdlib>

void div(uint64_t x, uint64_t y, uint64_t& quot, uint64_t& rem)
{
#ifdef __x86_64__
    __asm__ volatile ("divq %4" : "=a" (quot), "=d" (rem) : "a" (x), "d" (0), "rm" (y));
#else
    ldiv_t d = ::ldiv(x, y);
    quot = d.quot;
    rem = d.rem;
#endif
}

