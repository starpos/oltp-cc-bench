#include "time.hpp"
#include "sleep.hpp"
#include "thread_util.hpp"
#include "measure_util.hpp"
#include <cinttypes>
#include <vector>

int main()
{
    cybozu::thread::setThreadAffinity(::pthread_self(), 0);

#ifdef __aarch64__
    ::printf("couneter frequency: %" PRIu64 "\n", cybozu::time::counter_frequency());
#endif

    uint64_t freq = 0;
    {
        uint64_t t0, t1;
        t0 = cybozu::time::rdtscp();
        sleep_ms(1000);
        t1 = cybozu::time::rdtscp();
        freq = t1 - t0;
        ::printf("%" PRIu64 " count per 1 sec\n", freq);
    }

    std::vector<size_t> v(freq / 2);
    {
        uint64_t t0 = cybozu::time::rdtscp();
        for (auto it = v.begin(); it != v.end(); ++it) {
            uint64_t t1 = cybozu::time::rdtscp();
            *it = t1 - t0;
            t0 = t1;
        }
    }
    Histogram hist;
    for (size_t tdiff : v) hist.add(tdiff);
    std::cout << hist << std::endl;
}
