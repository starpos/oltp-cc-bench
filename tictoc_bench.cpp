#include <ctime>
#include <vector>
#include <chrono>
#include <unistd.h>
#include "tictoc.hpp"
#include "thread_util.hpp"
#include "random.hpp"
#include "measure_util.hpp"
#include "cpuid.hpp"
#include "vector_payload.hpp"
#include "cache_line_size.hpp"
#include "zipf.hpp"
#include "workload_util.hpp"


#ifdef USE_PARTITION
#include "partitioned.hpp"
#endif


using Mutex = cybozu::tictoc::Mutex;

std::vector<uint> CpuId_;

struct Shared
{
#ifdef USE_PARTITION
    PartitionedVectorWithPayload<Mutex> recV;
#else
    VectorWithPayload<Mutex> recV;
#endif
    size_t longTxSize;
    size_t nrOp;
    double wrRatio;
    size_t nrWr4Long;
    TxMode shortTxMode;
    TxMode longTxMode;
    bool usesBackOff;
    bool usesRMW;
    cybozu::tictoc::NoWaitMode nowait_mode;
    bool do_preemptive_verify;
    size_t nrTh4LongTx;
    size_t payload;
    bool usesZipf;
    double zipfTheta;
    double zipfZetan;
};


enum class Mode : bool { S = false, X = true, };


struct TicTocResult : Result1
{
    size_t nr_preemptive_aborts;

    TicTocResult() : Result1(), nr_preemptive_aborts(0) {
    }

    void operator+=(const TicTocResult& rhs) {
        Result1::operator+=(rhs);
        nr_preemptive_aborts += rhs.nr_preemptive_aborts;
    }

    std::string str() const {
        std::stringstream ss;
        ss << Result1::str();
        ss << " preemptive_aborts:" << nr_preemptive_aborts;
        return ss.str();
    }
};


TicTocResult worker2(
    size_t idx, uint8_t& ready, const bool& start, const bool& quit,
    bool& shouldQuit, Shared& shared)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    auto& recV = shared.recV;
#ifdef USE_PARTITION
    recV.allocate(idx);
    recV.checkAndWait();
#endif
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t wrRatio = size_t(shared.wrRatio * (double)SIZE_MAX);
    const TxMode shortTxMode = shared.shortTxMode;
    const TxMode longTxMode = shared.longTxMode;

    TicTocResult res;
    cybozu::util::Xoroshiro128Plus rand(::time(0), idx);
    FastZipf fastZipf(rand, shared.zipfTheta, recV.size(), shared.zipfZetan);
    cybozu::tictoc::LocalSet localSet;
    std::vector<uint8_t> value(shared.payload);

    const bool isLongTx = longTxSize != 0 && idx < shared.nrTh4LongTx; // starvation setting.
    const size_t realNrOp = isLongTx ? longTxSize : nrOp;
    const size_t realNrWr = isLongTx ? shared.nrWr4Long : size_t(shared.wrRatio * (double)nrOp);
    auto getMode = selectGetModeFunc<decltype(rand), Mode>(isLongTx, shortTxMode, longTxMode);
    auto getRecordIdx = selectGetRecordIdx<decltype(rand)>(isLongTx, shortTxMode, longTxMode, shared.usesZipf);
    localSet.init(shared.payload, realNrOp);
    localSet.setNowait(shared.nowait_mode);
    localSet.set_do_preemptive_verify(shared.do_preemptive_verify);

    store_release(ready, 1);
    while (!load_acquire(start)) _mm_pause();
    size_t count = 0; unused(count);
    while (!load_acquire(quit)) {
        size_t firstRecIdx = 0;
        uint64_t t0 = 0;
        if (shared.usesBackOff) t0 = cybozu::time::rdtscp();
        auto randState = rand.getState();
        for (size_t retry = 0;; retry++) {
            if (load_acquire(quit)) break; // to quit under starvation.
            rand.setState(randState);
            // Try to run transaction.
            for (size_t i = 0; i < realNrOp; i++) {
                size_t key = getRecordIdx(rand, fastZipf, recV.size(), realNrOp, i, firstRecIdx);
                Mode mode = getMode(rand, realNrOp, realNrWr, wrRatio, i);
                bool isWrite = (mode == Mode::X);

                auto& item = recV[key];
                Mutex& mutex = item.value;
                if (shared.usesRMW || !isWrite) {
                    localSet.read(mutex, item.payload, &value[0]);
                }
                if (isWrite) {
                    localSet.write(mutex, item.payload, &value[0]);
                }
            }
            if (unlikely(!localSet.preCommit())) {
                goto abort;
            }
            res.incCommit(isLongTx);
            res.addRetryCount(isLongTx, retry);
            break;
          abort:
            localSet.clear();
            res.incAbort(isLongTx);
            if (shared.usesBackOff) backOff(t0, retry, rand);
        }
    }
#ifdef USE_TICTOC_RTS_COUNT
    ::printf("rts_ratio_of_%zu: %zu/%zu\n"
             , idx, cybozu::tictoc::update_rts_count_
             , cybozu::tictoc::read_count_);
#endif
    res.nr_preemptive_aborts = cybozu::tictoc::get_nr_preemptive_aborts();
    return res;
}


void runTest()
{
#if 0
    runExec(4000 * 12, 12, 10, true, 0);
#endif
#if 0
    size_t nrOp = 4;
    //for (size_t nrResPerTh : {4, 4000}) {
    for (size_t nrResPerTh : {4000}) {
        for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
        //for (size_t nrTh : {32}) {
            if (nrTh > 2 && nrTh % 2 != 0) continue;
            for(size_t i = 0; i < 10; i++) {
                runExec(nrResPerTh * nrTh, nrTh, 10, false, 0, nrOp, 0);
                //sleep_ms(1000);
            }
        }
    }
#endif
#if 0
    // high-contention expr.
    for (size_t nrMutex : {40}) {
        //for (size_t nrTh : {32}) {
        //for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
        for (size_t nrTh : {8, 16, 24, 32}) {
            if (nrTh > 16 && nrTh % 2 != 0) continue;
            const size_t nrOp = 10;
            for (size_t nrWr = 0; nrWr <= nrOp; nrWr++) {
                for (size_t i = 0; i < 10; i++) {
                    bool verbose = false;
                    runExec(nrMutex, nrTh, 10, verbose, 0, nrOp, nrWr);
                }
            }
        }
    }
#endif
#if 0
    // starvation expr.
#if 0
    const size_t nrMutex = 400 * 1000 * 1000;
    const size_t nrTh = 16;
#else
    const size_t nrMutex = 40 * 1000;
    const size_t nrTh = 8;
#endif
    // for (size_t longTxPml : {20, 30, 40, 50, 60, 70, 80, 90,
    //             100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}) {
    for (size_t longTxPml : {1, 2, 3, 4, 5, 6, 7, 8, 9,
                10, 20, 30, 40, 50, 60, 70, 80, 90,
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}) {
        const size_t longTxSize = longTxPml * nrMutex / 1000;
        for (size_t i = 0; i < 10; i++) {
            bool verbose = false;
            //size_t maxSec = longTxPml >= 10 ? 20000 : 100;
            size_t maxSec = 100;
            runExec(nrMutex, nrTh, maxSec, verbose, longTxSize);
        }
    }
#endif
}

struct CmdLineOptionPlus : CmdLineOption
{
    using base = CmdLineOption;

    int usesBackOff; // 0 or 1.
    int usesRMW; // 0 or 1.
    int nowait;  // 0, 1, or 2.
    bool do_preemptive_verify;

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendOpt(&usesBackOff, 0, "backoff", "[0 or 1]: backoff (0:off, 1:on)");
        appendOpt(&usesRMW, 1, "rmw", "[0 or 1]: use read-modify-write or normal write (0:w, 1:rmw, default:1)");
        appendOpt(&nowait, 0, "nowait", "[0, 1, or 2]: use nowait optimization for write lock.");
        appendOpt(&do_preemptive_verify, 0, "preverify", "[0 or 1]: use preemptive verify.");
    }
    std::string str() const {
        return cybozu::util::formatString(
            "mode:tictoc %s backoff:%d rmw:%d nowait:%d preverify:%d"
            , base::str().c_str(), usesBackOff ? 1 : 0, usesRMW ? 1 : 0, nowait
            , int(do_preemptive_verify));
    }

    cybozu::tictoc::NoWaitMode nowait_mode() const {
        switch (nowait) {
        case 0:
            return cybozu::tictoc::NoWaitMode::Wait;
        case 1:
            return cybozu::tictoc::NoWaitMode::NoWait1;
        case 2:
            return cybozu::tictoc::NoWaitMode::Nowait2;
        default:
            throw cybozu::Exception("invalid nowait option.");
        }
    }
};


int main(int argc, char *argv[]) try
{
    CmdLineOptionPlus opt("tictoc_bench: benchmark with tictoc.");
    opt.parse(argc, argv);
    setCpuAffinityModeVec(opt.amode, CpuId_);

#ifdef NO_PAYLOAD
    if (opt.payload != 0) throw cybozu::Exception("payload not supported");
#endif

    if (opt.workload == "custom") {
        Shared shared;
        initRecordVector(shared.recV, opt);
        shared.longTxSize = opt.longTxSize;
        shared.nrOp = opt.nrOp;
        shared.wrRatio = opt.wrRatio;
        shared.nrWr4Long = opt.nrWr4Long;
        shared.shortTxMode = TxMode(opt.shortTxMode);
        shared.longTxMode = TxMode(opt.longTxMode);
        shared.usesBackOff = opt.usesBackOff ? 1 : 0;
        shared.usesRMW = opt.usesRMW ? 1 : 0;
        shared.nowait_mode = opt.nowait_mode();
        shared.do_preemptive_verify = opt.do_preemptive_verify;
        shared.nrTh4LongTx = opt.nrTh4LongTx;
        shared.payload = opt.payload;
        shared.usesZipf = opt.usesZipf;
        shared.zipfTheta = opt.zipfTheta;
        if (shared.usesZipf) {
            shared.zipfZetan = FastZipf::zeta(opt.getNrMu(), shared.zipfTheta);
        } else {
            shared.zipfZetan = 1.0;
        }
        for (size_t i = 0; i < opt.nrLoop; i++) {
            TicTocResult res;
            runExec(opt, shared, worker2, res);
        }
    } else {
        throw cybozu::Exception("bad workload.") << opt.workload;
    }
} catch (std::exception& e) {
    ::fprintf(::stderr, "exeption: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
