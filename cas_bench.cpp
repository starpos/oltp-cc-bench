#include <unordered_map>
#include <iostream>
#include <sstream>
#include <immintrin.h>
#include <unistd.h>
#include "thread_util.hpp"
#include "random.hpp"
#include "tx_util.hpp"
#include "measure_util.hpp"
#include "time.hpp"
#include "cpuid.hpp"
#include "lock.hpp"
#include "trlock.hpp"
#include "cmdline_option.hpp"


const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);

struct Line
{
    alignas(64) uint64_t obj;
};

struct Shared
{
    std::vector<Line> vec;
};


Result worker(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, Shared& shared)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    while (!start) _mm_pause();
    size_t c = 0;
    Result res;
    uint64_t v1, v2;
    uint64_t *obj = &shared.vec[idx].obj;

    v1 = __atomic_load_n(obj, __ATOMIC_RELAXED);
    while (!quit) {
        v2 = v1 + 1;
        if (!__atomic_compare_exchange(obj, &v1, &v2, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
            continue;
        }
        c++;
    }
    res.addCommit(false, c);
    return res;
}


struct CmdLineOptionPlus : CmdLineOption
{
    using base = CmdLineOption;

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
    }
};


int main(int argc, char *argv[]) try
{
    CmdLineOptionPlus opt("cas benchmark.");
    opt.parse(argc, argv);

    Shared shared;
    shared.vec.resize(opt.nrTh);
    runExec(opt, shared, worker);

} catch (std::exception& e) {
    ::fprintf(::stderr, "exeption: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
