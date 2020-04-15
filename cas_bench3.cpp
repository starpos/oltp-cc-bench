/**
 * Performance benchmark of cache accesses with conflicts.
 */
#include <thread>
#include <vector>
#include "cache_line_size.hpp"
#include "atomic_wrapper.hpp"
#include "cpuid.hpp"
#include "random.hpp"
#include "thread_util.hpp"
#include "arch.hpp"
#include "measure_util.hpp"
#include "cybozu/option.hpp"
#include "cybozu/exception.hpp"


const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);


alignas(CACHE_LINE_SIZE)
uint64_t shared_value_ = 0;
alignas(CACHE_LINE_SIZE)
size_t dummy_ = 0;



struct alignas(CACHE_LINE_SIZE) Shared
{
    bool started;
    bool quit;
    size_t read_pct;
    std::vector<size_t> read_count;
    std::vector<size_t> write_count;

    Shared() : started(false), quit(false), read_pct(0), read_count(), write_count() {
    }
    void resize(size_t size) {
        read_count.resize(size);
        write_count.resize(size);
    }
};


void worker(size_t id, Shared& shared) try
{
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[id]);

    size_t nr_read = 0;
    size_t nr_write = 0;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + id);

    while (!loadAcquire(shared.started)) _mm_pause();

    uint64_t x = 0;
    while (!loadAcquire(shared.quit)) {
        if (rand() % 100 < shared.read_pct) {
#if 1
            x += loadAcquire(shared_value_);
#else
            // emulate invisible atomic read operation.
            uint64_t y = loadAcquire(shared_value_);
            for (;;) {
                // emulate reading the record.
                acquireFence();
                x = loadAcquire(shared_value_);
                if (x == y) break;
                y = x;
            }
#endif
            nr_read++;
        } else {
#if 1
            storeRelease(shared_value_, x);
#else
            uint64_t y = loadAcquire(shared_value_);
            for (;;) {
                if (compareExchange(shared_value_, y, x + 1)) {
                    releaseFence();
                    // emulate writing the record.
                    break;
                }
            }
#endif
            nr_write++;
        }
    }
    shared.read_count[id] += nr_read;
    shared.write_count[id] += nr_write;

} catch (std::exception& e) {
    ::fprintf(::stderr, "worker:%zu:error: %s\n", id, e.what());
} catch (...) {
    ::fprintf(::stderr, "worker:%zu:unknown error.\n", id);
}


struct Option
{
    size_t nr_threads;
    size_t read_pct;
    size_t run_period;
    size_t nr_loop;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.appendOpt(&nr_threads, 1, "th", "NUM : number of threads");
        opt.appendOpt(&read_pct, 50, "rpct", "PERCENT : read percent");
        opt.appendOpt(&run_period, 1, "p", "SEC : running period");
        opt.appendOpt(&nr_loop, 1, "loop", "NUM : number of experiments");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
        if (nr_threads == 0) {
            throw cybozu::Exception("nr_threads must not be 0");
        }
        if (read_pct > 100) {
            throw cybozu::Exception("read_pct invalid value") << read_pct;
        }
    }
};



void run_exec(const Option& opt)
{
    Shared shared;
    shared.resize(opt.nr_threads);
    shared.read_pct = opt.read_pct;

    std::vector<std::thread> th_v;
    for (size_t i = 0; i < opt.nr_threads; i++) {
        th_v.emplace_back(worker, i, std::ref(shared));
    }
    sleep_ms(100);
    storeRelease(shared.started, true);
    for (size_t i = 0; i < opt.run_period; i++) {
        sleep_ms(1000);
    }
    storeRelease(shared.quit, true);
    for (auto& th : th_v) th.join();

    size_t nr_read = 0, nr_write = 0;
    for (size_t nr : shared.read_count) nr_read += nr;
    for (size_t nr : shared.write_count) nr_write += nr;

    ::printf("nr_threads %zu read_pct %zu throughput %.3f nr_read %zu nr_write %zu\n"
             , opt.nr_threads, opt.read_pct
             , (nr_read + nr_write) / (double)opt.run_period
             , nr_read, nr_write);
    ::fflush(::stdout);
}


int main(int argc, char* argv[]) try
{
    Option opt(argc, argv);

    for (size_t i = 0; i < opt.nr_loop; i++) {
        run_exec(opt);
    }

    return 0;
} catch (std::exception& e) {
    ::fprintf(::stderr, "main:error: %s\n", e.what());
    return 1;
} catch (...) {
    ::fprintf(::stderr, "main:unknown error.\n");
    return 1;
}
