#include <vector>
#include "sxql.hpp"
#include "thread_util.hpp"
#include "measure_util.hpp"
#include "cpuid.hpp"
#include "random.hpp"

using SXQLock = cybozu::lock::SXQLock;

const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);


struct Shared
{
    std::vector<SXQLock::Mutex> muV;
};


Result1 worker0(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, Shared& shared)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    std::vector<SXQLock::Mutex>& muV = shared.muV;

    Result1 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0), idx);

    while (!start) _mm_pause();
    while (!quit) {
        //const SXQLock::Mode mode = (rand() & 0x1) ? SXQLock::Mode::X : SXQLock::Mode::S;
        const SXQLock::Mode mode = (rand() % 128 < 16) ? SXQLock::Mode::X : SXQLock::Mode::S;
        //const SXQLock::Mode mode = SXQLock::Mode::X;
        //const SXQLock::Mode mode = SXQLock::Mode::S;
        SXQLock lk(&muV[0], mode);
        //SXQLock lk;
        //lk.tryLock(&muV[0], mode);
#if 0
        size_t nr = rand() % 1000;
        for (size_t i = 0; i < nr; i++) {
            _mm_pause();
        }
#endif
#if 0
        if (mode == SXQLock::Mode::S && (rand() & 0x1) == 1) {
            lk.tryUpgrade();
        }
#endif

        res.incCommit(false);
    }
    return res;
}


int main(int argc, char *argv[]) try
{
    CmdLineOption opt("rwlock_bench: benchmark with read-write lock.");
    opt.parse(argc, argv);

    Shared shared;
    shared.muV.resize(1);
    for (size_t i = 0; i < opt.nrLoop; i++) {
        Result1 res;
        runExec(opt, shared, worker0, res);
    }

} catch (std::exception& e) {
    ::fprintf(::stderr, "exception: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
