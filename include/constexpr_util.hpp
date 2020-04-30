#pragma once
#include <cassert>
#include <cstddef>
#include <cinttypes>


constexpr size_t GetMaxValue(size_t bits)
{
    //static_assert(bits <= 64, "bits must be no more than 64.");
    if (bits < 64) {
        return (uint64_t(1) << bits) - 1;
    } else {
        assert(bits == 64);
        return uint64_t(-1);
    }
}
