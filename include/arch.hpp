#pragma once

#if defined(__x86_64__)
#include <immintrin.h>
#elif defined(__aarch64__)
#include "arch_aarch64.hpp"
#else
#error "This architecture is not supported."
#endif
