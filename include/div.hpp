#pragma once
#include <cinttypes>
#include <cstdlib>

void div(uint64_t x, uint64_t y, uint64_t& quot, uint64_t& rem)
{
#if 0
#ifdef __x86_64__
    // Inline assembler is slower than ::ldiv() call using clang++-6.0.
    __asm__ volatile (
        "cqo\n\t"
        "divq (%3)\n\t"
        : "=a" (quot), "=d" (rem)
        : "a" (x), "r" (&y));
#else
    ldiv_t d = ::ldiv(x, y);
    quot = d.quot;
    rem = d.rem;
#endif
#else
    ldiv_t d = ::ldiv(x, y);
    quot = d.quot;
    rem = d.rem;
#endif
}

