#pragma once
/**
 * @file
 * @brief clock wrappers.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */

#include <chrono>
#include <deque>
#include <cinttypes>


namespace cybozu {
namespace time {

template <typename Clock = std::chrono::high_resolution_clock>
class TimeStack
{
private:
    std::deque<std::chrono::time_point<Clock> > q_;
public:
    void pushNow() {
        q_.push_front(Clock::now());
    }

    void pushTime(std::chrono::time_point<Clock> tp) {
        q_.push_front(tp);
    }

    template <typename Duration>
    unsigned long elapsed() const {
        if (q_.size() < 2) {
            return std::chrono::duration_cast<Duration>(
                std::chrono::duration<long>::zero()).count();
        } else {
            return std::chrono::duration_cast<Duration>(q_[0] - q_[1]).count();
        }
    }

    unsigned long elapsedInSec() const {
        return elapsed<std::chrono::seconds>();
    }

    unsigned long elapsedInMs() const {
        return elapsed<std::chrono::milliseconds>();
    }

    unsigned long elapsedInUs() const {
        return elapsed<std::chrono::microseconds>();
    }

    unsigned long elapsedInNs() const {
        return elapsed<std::chrono::nanoseconds>();
    }

    void clear() {
        q_.clear();
    }
};

#ifdef __x86_64__
uint64_t rdtscp()
{
    uint32_t eax, edx, ecx;
    __asm__ volatile ("rdtscp" : "=a" (eax), "=d" (edx), "=c" (ecx));
    return (uint64_t)edx << 32 | eax;
}
#elif defined(__aarch64__)
uint64_t rdtscp()
{
    // Precision is about 10nsec.
    uint64_t ts;
#if 0 // isb is costly.
    __asm__ volatile ("isb; mrs %0, cntvct_el0" : "=r" (ts));
#else
    __asm__ volatile ("mrs %0, cntvct_el0" : "=r" (ts));
#endif
    return ts;
}
uint64_t counter_frequency()
{
    uint64_t freq;
    __asm__ volatile ("mrs %0, cntfrq_el0" : "=r" (freq));
    return freq;
}
#else
uint64_t rdtscp()
{
    throw std::runtime_error("rdtscp() is not supported.");
}
#endif


}} // namespace cybozu::time
