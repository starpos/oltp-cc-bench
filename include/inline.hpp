#pragma once

// DO NOT USE inline keyword in C++17 code. The meaning of it has been changed.
// clang-10 does not do inline expantion with inline keyword with -std=c++17.
// gcc-9.3 puts warnings without inline keyword.
#if defined(__clang__) && (__clang__major__ >= 10)
#define INLINE __attribute__((always_inline))
#else
#define INLINE __attribute__((always_inline)) inline
#endif

#define NOINLINE __attribute__((noinline))
