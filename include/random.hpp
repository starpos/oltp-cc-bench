#pragma once
/**
 * @file
 * @brief Random data generator.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <random>
#include <limits>
#include <cassert>

namespace cybozu {
namespace util {

template <typename IntType>
class Random
{
private:
    std::random_device rd_;
    std::mt19937 gen_;
    std::uniform_int_distribution<IntType> dist_;

public:
    using ResultType = IntType;

    Random(IntType minValue = std::numeric_limits<IntType>::min(),
           IntType maxValue = std::numeric_limits<IntType>::max())
        : rd_()
        , gen_(rd_())
        , dist_(minValue, maxValue) {
    }

    IntType operator()() {
        return dist_(gen_);
    }
};

class XorShift128
{
private:
    uint32_t x_, y_, z_, w_;

public:
    explicit XorShift128(uint32_t seed)
        : x_(123456789)
        , y_(362436069)
        , z_(521288629)
        , w_(88675123) {
        x_ ^= seed;
        y_ ^= (seed << 8)  | (seed >> (32 - 8));
        z_ ^= (seed << 16) | (seed >> (32 - 16));
        w_ ^= (seed << 24) | (seed >> (32 - 24));
    }
    using ResultType = uint32_t;

    uint32_t operator()() {
        return get();
    }

    uint32_t get() {
        const uint32_t t = x_ ^ (x_ << 11);
        x_ = y_;
        y_ = z_;
        z_ = w_;
        w_ = (w_ ^ (w_ >> 19)) ^ (t ^ (t >> 8));
        return  w_;
    }

    uint32_t get(uint32_t max) {
        return get() % max;
    }

    uint32_t get(uint32_t min, uint32_t max) {
        assert(min < max);
        return get() % (max - min) + min;
    }
};


/**
 * This algorithm is originally developed
 * by David Blackman and Sebastiano Vigna (vigna@acm.org)
 * http://xorshift.di.unimi.it/splitmix64.c
 */
class SplitMix64
{
    uint64_t x_;
public:
    using ResultType = uint64_t;
    explicit SplitMix64(uint64_t seed) : x_(seed) {}
    uint64_t operator()() {
        uint64_t z = (x_ += UINT64_C(0x9E3779B97F4A7C15));
        z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
        return z ^ (z >> 31);
    }
};


/**
 * This algorithm is originally developed
 * by David Blackman and Sebastiano Vigna (vigna@acm.org)
 * http://xoroshiro.di.unimi.it/xorshift128plus.c
 */
class XorShift128Plus
{
    uint64_t s_[2];
public:
    using ResultType = uint64_t;
    explicit XorShift128Plus(uint64_t seed) {
        s_[0] = seed;
        s_[1] = SplitMix64(seed)();
    }
    uint64_t operator()() {
        uint64_t s1 = s_[0];
        const uint64_t s0 = s_[1];
        const uint64_t res = s0 + s1;
        s_[0] = s0;
        s1 ^= s1 << 23;
        s_[1] = s1 ^ s0 ^ (s1 >> 18) ^ (s0 >> 5);
        return res;
    }
};

/**
 * This algorithm is originally developed
 * by David Blackman and Sebastiano Vigna (vigna@acm.org)
 * http://xoroshiro.di.unimi.it/xoroshiro128plus.c
 */
class Xoroshiro128Plus
{
    uint64_t s_[2];
public:
    using ResultType = uint64_t;
    explicit Xoroshiro128Plus(uint64_t seed) {
        s_[0] = seed;
        s_[1] = SplitMix64(seed)();
    }
    uint64_t operator()() {
        const uint64_t s0 = s_[0];
        uint64_t s1 = s_[1];
        const uint64_t res = s0 + s1;
        s1 ^= s0;
        s_[0] = rotl(s0, 55) ^ s1 ^ (s1 << 14);
        s_[1] = rotl(s1, 36);
        return res;
    }
private:
    uint64_t rotl(const uint64_t x, int k) {
        return (x << k) | (x >> (64 - k));
    }
};

}} //namespace cybozu::util
