#include <cstdio>
#include "time.hpp"
#include "sleep.hpp"


int main()
{
    uint64_t t0 = cybozu::time::rdtscp();
    sleep_ms(1000);
    uint64_t t1 = cybozu::time::rdtscp();

    ::printf("%" PRIu64 " count/sec\n", t1 - t0);
}
