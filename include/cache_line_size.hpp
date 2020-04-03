#pragma once
#include <cstddef>
#include <type_traits>


constexpr size_t CACHE_LINE_SIZE = 64;


template <typename T, typename Enabled = void>
struct CacheLineAligned
{
    alignas(CACHE_LINE_SIZE)
    T value;
};


template <typename T>
struct CacheLineAligned<T, std::enable_if_t<std::is_scalar_v<T>>>
{
    alignas(CACHE_LINE_SIZE)
    T value;

    CacheLineAligned() = default;

    // For implicit conversion from/to data of type T.
    CacheLineAligned(T value0) noexcept : value(value0) {}
    operator T() const noexcept { return value; }

    T operator++() noexcept { return ++value; }
    T operator++(int) noexcept { return value++; }
    // TODO: overrides of primitive operators if need.
};
