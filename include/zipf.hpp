#pragma once
/**
 * @file
 * @brief Zipf distribution generator.
 * @author HOSHINO Takashi
 *
 * (C) 2016 Cybozu Labs, Inc.
 */
#include <cmath>
#include <cfloat>
#include <random>
#include <vector>
#include "random.hpp"
#include "inline.hpp"

class Zipf
{
    std::vector<double> p_; // cumulative probability

    cybozu::util::Xoroshiro128Plus rand_;
public:
    /**
     * If theta is 0.0, it is uniform distribution.
     */
    Zipf(double theta, size_t nr) : p_(), rand_(cybozu::util::Random<uint64_t>()()) {
        assert(nr >= 1);
        assert(0.0 <= theta);
        assert(theta <= 1.0);
        double sum = 0.0;
        for (size_t i = 0; i < nr; i++) {
            sum += 1.0 / ::pow((double)(i + 1), theta);
        }
        double c = 1.0 / sum;
        p_.reserve(nr);
        double sumc = 0.0;
        for (size_t i = 0; i < nr; i++) {
            sumc += c / ::pow((double)(i + 1), theta);
            p_.push_back(sumc);
        }
        p_.back() = 2.0; // larger than 1.0
    }
#if 0
    void print() const {
        for (size_t i = 0; i < p_.size(); i++) {
            ::printf("%5zu %.6f\n", i, p_[i]);
        }
    }
#endif
    /**
     * Return value in [0, nr).
     */
    size_t operator()() {
        const double v = randf();
        std::vector<double>::const_iterator it = std::lower_bound(p_.begin(), p_.end(), v);
        const size_t n = it - p_.begin();
        assert(n < p_.size());
        return n;
    }
    uint64_t rand() { return rand_(); }
private:
    double randf() { return rand_() / (double)UINT64_MAX; }
};


/**
 * Fast zipf distribution by Jim Gray et al.
 */
class FastZipf
{
    //cybozu::util::Xoroshiro128Plus rand_;
    cybozu::util::Xoroshiro128Plus& rand_;
    const size_t nr_;
    const double alpha_, zetan_, eta_;
    const double threshold_;
public:
    FastZipf(cybozu::util::Xoroshiro128Plus& rand, double theta, size_t nr)
        //: rand_(cybozu::util::Random<uint64_t>()())
        : rand_(rand)
        , nr_(nr)
        , alpha_(1.0 / (1.0 - theta))
        , zetan_(zeta(nr, theta))
        , eta_((1.0 - ::pow(2.0 / (double)nr, 1.0 - theta)) / (1.0 - zeta(2, theta) / zetan_))
        , threshold_(1.0 + ::pow(0.5, theta)) {
        assert(0.0 <= theta);
        assert(theta < 1.0); // 1.0 can not be specified.
    }
    INLINE size_t operator()() {
        double u = rand_() / (double)UINT64_MAX;
#if 1
        double uz = u * zetan_;
        if (uz < 1.0) return 0;
        if (uz < threshold_) return 1;
#endif
        return (size_t)((double)nr_ * ::pow(eta_ * u - eta_ + 1.0, alpha_));
    }
    uint64_t rand() { return rand_(); }
private:
    static double zeta(size_t nr, double theta) {
        double ans = 0.0;
        for (size_t i = 0; i < nr; i++) {
            ans += ::pow(1.0 / (double)(i + 1), theta);
        }
        return ans;
    }
};

class ParetoDistribution
{
    cybozu::util::Xoroshiro128Plus rand_;
    double a_;
    double b_;
public:
    ParetoDistribution(double a, double b) : rand_(cybozu::util::Random<uint64_t>()()), a_(a), b_(b) {
        assert(a_ > 0.0);
        assert(b_ > 0.0);
    }
    /**
     * Returns:
     *   0.0 < value.
     */
    double operator()() {
        double p = rand_() / (double)UINT64_MAX;
        return b_ / ::pow(1.0 - p, 1.0 / a_);
    }
};
