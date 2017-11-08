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
#include <cstring>

namespace cybozu {
namespace util {

/**
 * Rand must have operator() with type IntType (*)().
 */
template <typename IntType, typename Rand>
void fillRandom(Rand &rand, void *data, size_t size)
{
    char *p = (char *)data;
    const size_t s = sizeof(IntType);
    while (s <= size) {
        IntType i = rand();
        ::memcpy(p, &i, s);
        p += s;
        size -= s;
    }
    if (0 < size) {
        IntType i = rand();
        ::memcpy(p, &i, size);
    }
}

template <typename IntType>
class Random
{
private:
    std::random_device rd_;
    IntType seed_;
    std::mt19937 gen_;
    std::uniform_int_distribution<IntType> dist_;

public:
    Random(IntType minValue = std::numeric_limits<IntType>::min(),
           IntType maxValue = std::numeric_limits<IntType>::max())
        : rd_()
        , seed_(rd_())
        , gen_(seed_)
        , dist_(minValue, maxValue) {
    }

    void setSeed(IntType seed) {
        seed_ = seed;
        gen_.seed(seed_);
    }
    void setSeed() {
        seed_ = rd_();
        gen_.seed(seed_);
    }
    IntType getSeed() const {
        return seed_;
    }

    IntType operator()() {
        return dist_(gen_);
    }

    void fill(void *data, size_t size) {
        fillRandom<IntType>(*this, data, size);
    }

    template <typename T>
    T get() {
        T value;
        fill(&value, sizeof(value));
        return value;
    }

    uint16_t get16() { return get<uint16_t>(); }
    uint32_t get32() { return get<uint32_t>(); }
    uint64_t get64() { return get<uint64_t>(); }
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

    void fill(void *data, size_t size) {
        fillRandom<uint32_t>(*this, data, size);
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
 * http://xoroshiro.di.unimi.it/xoroshiro128plus.c
 */
class Xoroshiro128Plus
{
public:
    struct State {
        uint64_t s[2];
        const uint64_t& operator[](size_t i) const { return s[i]; }
        uint64_t& operator[](size_t i) { return s[i]; }
        void operator+=(uint64_t v) {
            s[0] += v;
        }
    };
    using ResultType = uint64_t;

private:
    State s_;

public:
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
    void fill(void *data, size_t size) {
        fillRandom<uint64_t>(*this, data, size);
    }
    State getState() const { return s_; }
    void setState(State s) { s_ = s; }
private:
    uint64_t rotl(const uint64_t x, int k) {
        return (x << k) | (x >> (64 - k));
    }
};

}} //namespace cybozu::util
