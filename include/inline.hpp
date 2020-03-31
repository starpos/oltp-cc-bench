#pragma once

// DO NOT USE inline keyword from C++17. The meaning of it has been changed.
#define INLINE __attribute__((always_inline))
#define NOINLINE __attribute__((noinline))
