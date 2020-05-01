#include <cstdio>
#ifdef USE_LICC2
#include "licc2.hpp"
#else
#include "licc.hpp"
#endif
#include <algorithm>
#include "pqlock.hpp"
#include "cmdline_option.hpp"
#include "measure_util.hpp"
#include "cpuid.hpp"
#include "time.hpp"
#include "tx_util.hpp"
#include "zipf.hpp"
#include "workload_util.hpp"


#ifdef USE_PARTITION
#include "partitioned.hpp"
#endif


struct CmdLineOptionPlus : CmdLineOption
{
    using base = CmdLineOption;

    std::string modeStr;
    int pqLockType;
    int usesBackOff; // 0 or 1.
    size_t writePct; // 0 to 100.
    int usesRMW; // 0 or 1.
    bool preverify;

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendOpt(&modeStr, "licc-hybrid", "mode", "[mode]: specify mode in licc-pcc, licc-occ, licc-hybrid (default).");
        appendOpt(&pqLockType, 0, "pqlock", "[id]: pqlock type (0:none(default), 1:pqspin, 3:pqmcs1, 4:pqmcs2, 5:pq1993, 6:pq1997, 7:pqmcs3, 8:mcslike)");
        appendOpt(&usesBackOff, 0, "backoff", "[0 or 1]: backoff 0:off 1:on (default: 0)");
        appendOpt(&usesRMW, 1, "rmw", "[0 or 1]: use read-modify-write or normal write 0:w 1:rmw (default: 1)");
        appendOpt(&writePct, 50, "writepct", "[pct]: write percentage (0 to 100) for custom3 workload (default: 50)");
        appendOpt(&preverify, 0, "preverify", "[0 or 1]: preemptive verify 0:off 1:on (defaut: 0)");
    }
    std::string str() const {
        return cybozu::util::formatString(
            "mode:%s %s pqLockType:%d backoff:%d writePct:%zu rmw:%d preverify:%d"
            , modeStr.c_str(), base::str().c_str(), pqLockType
            , usesBackOff ? 1 : 0, writePct, usesRMW ? 1 : 0, preverify ? 1 : 0);
    }
};


enum class IMode : uint8_t {
    S = 0, X = 1, INVALID = 2,
};


template <typename PQLock>
struct ILockTypes
{
#ifdef USE_LICC2
    using ILock = cybozu::lock::licc2::cas::Lock;
    using IMutex = cybozu::lock::licc2::cas::Mutex;
    using ILockSet = cybozu::lock::licc2::LockSet<IMutex, ILock>;
#else
    using ILock = cybozu::lock::ILock<PQLock>;
    using IMutex = cybozu::lock::IMutex<PQLock>;
    using ILockSet = cybozu::lock::ILockSet<PQLock>;
#endif
};


#ifdef USE_LICC2
template <>
struct ILockTypes<cybozu::lock::licc2::PqMcsLike>
{
    using ILock = cybozu::lock::licc2::mcs::Lock;
    using IMutex = cybozu::lock::licc2::mcs::Mutex;
    using ILockSet = cybozu::lock::licc2::LockSet<IMutex, ILock>;
};
#endif


std::vector<uint> CpuId_;

EpochGenerator epochGen_;

enum class ReadMode : uint8_t { PCC, OCC, HYBRID };

const char* readModeToStr(ReadMode rmode)
{
    switch (rmode) {
    case ReadMode::PCC:
        return "licc-pcc";
    case ReadMode::OCC:
        return "licc-occ";
    case ReadMode::HYBRID:
        return "licc-hybrid";
    }
    return nullptr;
}

ReadMode strToReadMode(const char *s)
{
    const char *hybrid = "licc-hybrid";
    const char *occ = "licc-occ";
    const char *pcc = "licc-pcc";

    if (::strncmp(hybrid, s, ::strnlen(hybrid, 20)) == 0) {
        return ReadMode::HYBRID;
    } else if (::strncmp(occ, s, ::strnlen(occ, 20)) == 0) {
        return ReadMode::OCC;
    } else if (::strncmp(pcc, s, ::strnlen(pcc, 20)) == 0) {
        return ReadMode::PCC;
    } else {
        throw cybozu::Exception("strToReadMode: bad string") << s;
    }
}

template <typename PQLock>
struct ILockShared
{
    using IMutex = typename ILockTypes<PQLock>::IMutex;

    //std::vector<IMutex> muV;
    //std::vector<Record<IMutex> > recV;
#ifdef USE_PARTITION
    PartitionedVectorWithPayload<IMutex> recV;
#else
    VectorWithPayload<IMutex> recV;
#endif
    ReadMode rmode;
    size_t longTxSize;
    size_t nrOp;
    double wrRatio;
    size_t nrWr4Long;
    TxMode shortTxMode;
    TxMode longTxMode;
    bool usesBackOff;
    size_t writePct;
    bool usesRMW;
    size_t nrTh4LongTx;
    size_t payload;
    bool usesZipf;
    double zipfTheta;
    double zipfZetan;
    bool preverify;
};


//#define MONITOR_LATENCY
#undef MONITOR_LATENCY


struct LiccResult : Result1
{
    size_t nr_preemptive_aborts;

    LiccResult() : Result1(), nr_preemptive_aborts(0) {
    }

    void operator+=(const LiccResult& rhs) {
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


template <typename PQLock>
LiccResult worker0(size_t idx, uint8_t& ready, const bool& start, const bool& quit, bool& shouldQuit, ILockShared<PQLock>& shared)
{
    using IMutex = typename ILockTypes<PQLock>::IMutex;
    using ILockSet = typename ILockTypes<PQLock>::ILockSet;

    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    auto& recV = shared.recV;
#ifdef USE_PARTITION
    recV.allocate(idx);
    recV.checkAndWait();
#endif
    const ReadMode rmode = shared.rmode;
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t wrRatio = size_t(shared.wrRatio * (double)SIZE_MAX);
    const TxMode shortTxMode = shared.shortTxMode;
    const TxMode longTxMode = shared.longTxMode;
    const bool usesRMW = shared.usesRMW;

    LiccResult res;
    cybozu::util::Xoroshiro128Plus rand(::time(0), idx);
    FastZipf fastZipf(rand, shared.zipfTheta, recV.size(), shared.zipfZetan);
    const bool isLongTx = longTxSize != 0 && idx < shared.nrTh4LongTx;
    const size_t realNrOp = isLongTx ? longTxSize : nrOp;
    const size_t realNrWr = isLongTx ? shared.nrWr4Long : size_t((double)nrOp * shared.wrRatio);
    auto getMode = selectGetModeFunc<decltype(rand), IMode>(isLongTx, shortTxMode, longTxMode);
    auto getRecordIdx = selectGetRecordIdx<decltype(rand)>(isLongTx, shortTxMode, longTxMode, shared.usesZipf);

    EpochTxIdGenerator<9, 2> epochTxIdGen(idx + 1, epochGen_);

    ILockSet lockSet;
    lockSet.init(shared.payload, realNrOp);
    std::vector<uint8_t> value(shared.payload);

    store_release(ready, 1);
    while (!load_acquire(start)) _mm_pause();
    while (!load_acquire(quit)) {
        const uint32_t ordId = epochTxIdGen.get();
        lockSet.set_ord_id(ordId);
        size_t firstRecIdx;

        uint64_t t0;
        if (shared.usesBackOff) t0 = cybozu::time::rdtscp();
        auto randState = rand.getState();
        for (size_t retry = 0;; retry++) {
            if (load_acquire(quit)) break; // to quit under starvation.
            assert(lockSet.is_empty());
            rand.setState(randState);
#ifdef MONITOR_LATENCY
            uint64_t ts[6];
            ts[0] = cybozu::time::rdtscp();
#endif
            for (size_t i = 0; i < realNrOp; i++) {
                size_t key = getRecordIdx(rand, fastZipf, recV.size(), realNrOp, i, firstRecIdx);
                IMode mode = getMode(rand, realNrOp, realNrWr, wrRatio, i);

                auto& rec = recV[key];
                IMutex& mutex = rec.value;
                void *sharedValue = rec.payload;
                if (mode == IMode::S) {
                    const bool tryInvisibleRead =
                        (rmode == ReadMode::OCC) ||
                        (rmode == ReadMode::HYBRID && !isLongTx && retry == 0);
                    if (tryInvisibleRead) {
                        if (unlikely(!lockSet.optimistic_read(mutex, sharedValue, &value[0]))) goto abort;
                    } else {
                        if (unlikely(!lockSet.pessimistic_read(mutex, sharedValue, &value[0]))) goto abort;
                    }
                } else {
                    assert(mode == IMode::X);
                    if (usesRMW) {
                        if (unlikely(!lockSet.read_for_update(mutex, sharedValue, &value[0]))) goto abort;
                        if (unlikely(!lockSet.write(mutex, sharedValue, &value[0]))) goto abort;
                    } else {
                        if (unlikely(!lockSet.write(mutex, sharedValue, &value[0]))) goto abort;
                    }
                }
            }
#ifdef MONITOR_LATENCY
            ts[1] = cybozu::time::rdtscp();
#endif
            lockSet.reserve_all_blind_writes();
#ifdef MONITOR_LATENCY
            ts[2] = cybozu::time::rdtscp();
#endif
            if (shared.preverify && unlikely(!lockSet.preemptive_verify())) {
                res.nr_preemptive_aborts++;
                goto abort;
            }
            if (unlikely(!lockSet.protect_all())) goto abort;
#ifdef MONITOR_LATENCY
            ts[3] = cybozu::time::rdtscp();
#endif
            if (unlikely(!lockSet.verify_and_unlock())) goto abort;
#ifdef MONITOR_LATENCY
            ts[4] = cybozu::time::rdtscp();
#endif
            lockSet.update_and_unlock();
#ifdef MONITOR_LATENCY
            ts[5] = cybozu::time::rdtscp();
#endif
            res.incCommit(isLongTx);
            res.addRetryCount(isLongTx, retry);

#ifdef MONITOR_LATENCY
            if (isLongTx) {
                ::printf("read %" PRIu64 "\t"
                         "bw %" PRIu64 "\t"
                         "protect %" PRIu64 "\t"
                         "verify %" PRIu64 "\t"
                         "update %" PRIu64 "\n"
                         , ts[1] - ts[0]
                         , ts[2] - ts[1]
                         , ts[3] - ts[2]
                         , ts[4] - ts[3]
                         , ts[5] - ts[4]);
            }
#endif

            break;
          abort:
            res.incAbort(isLongTx);
            lockSet.clear();
            if (shared.usesBackOff) backOff(t0, retry, rand);
        }
    }

    return res;
}


/**
 * This worker is for short-long-long workload.
 */
template <typename PQLock>
Result2 worker1(size_t idx, uint8_t& ready, const bool& start, const bool& quit, bool& shouldQuit, ILockShared<PQLock>& shared)
{
    using IMutex = typename ILockTypes<PQLock>::IMutex;
    using ILockSet = typename ILockTypes<PQLock>::ILockSet;

    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    //std::vector<IMutex>& muV = shared.muV;
    //std::vector<Record<IMutex> >& recV = shared.recV;
    auto& recV = shared.recV;
#ifdef USE_PARTITION
    recV.allocate(idx);
    recV.checkAndWait();
#endif
    const ReadMode rmode = shared.rmode;

    const size_t txSize = [&]() -> size_t {
        if (idx == 0) {
            return std::max<size_t>(recV.size() / 2, 10);
        } else if (idx <= 5) {
            return std::max<size_t>(recV.size() / 10, 10);
        } else {
            return 10;
        }
    }();

    const bool isLongTx = txSize > 10;

    Result2 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0), idx);
    BoolRandom<decltype(rand)> boolRand(rand);
    const size_t realNrOp = txSize;

    EpochTxIdGenerator<9, 2> epochTxIdGen(idx + 1, epochGen_);

    std::vector<uint8_t> value(shared.payload);
    ILockSet lockSet;
    lockSet.init(shared.payload, realNrOp);

    store_release(ready, 1);
    while (!load_acquire(start)) _mm_pause();
    while (!load_acquire(quit)) {
        const uint32_t ordId = epochTxIdGen.get();

        lockSet.set_ord_id(ordId);
        uint64_t t0;
        if (shared.usesBackOff) t0 = cybozu::time::rdtscp();
        auto randState = rand.getState();
        for (size_t retry = 0;; retry++) {
            if (load_acquire(quit)) break; // to quit under starvation.
            assert(lockSet.is_empty());
            rand.setState(randState);
            boolRand.reset();
            for (size_t i = 0; i < realNrOp; i++) {
                IMode mode = rand() % 100 < shared.writePct ? IMode::X : IMode::S;
                size_t key = rand() % recV.size();
                auto& rec = recV[key];
                IMutex& mutex = rec.value;
                void *sharedValue = rec.payload;
                if (mode == IMode::S) {
                    const bool tryInvisibleRead =
                        (rmode == ReadMode::OCC) ||
                        (rmode == ReadMode::HYBRID && !isLongTx && retry == 0);
                    if (tryInvisibleRead) {
                        if (unlikely(!lockSet.optimistic_read(mutex, sharedValue, &value[0]))) goto abort;
                    } else {
                        if (unlikely(!lockSet.pessimistic_read(mutex, sharedValue, &value[0]))) goto abort;
                    }
                } else {
                    assert(mode == IMode::X);
                    if (shared.usesRMW) {
                        if (unlikely(!lockSet.read_for_update(mutex, sharedValue, &value[0]))) goto abort;
                        if (unlikely(!lockSet.write(mutex, sharedValue, &value[0]))) goto abort;
                    } else {
                        if (unlikely(!lockSet.write(mutex, sharedValue, &value[0]))) goto abort;
                    }
                }
            }
            lockSet.reserve_all_blind_writes();
            if (unlikely(!lockSet.protect_all())) goto abort;
            if (unlikely(!lockSet.verify_and_unlock())) goto abort;
            lockSet.update_and_unlock();
            res.incCommit(txSize);
            res.addRetryCount(txSize, retry);
            break;
          abort:
            res.incAbort(txSize);
            lockSet.clear();
            if (shared.usesBackOff) backOff(t0, retry, rand);
        }
    }
    return res;
}


template <typename PQLock>
void setShared(const CmdLineOptionPlus& opt, ILockShared<PQLock>& shared)
{
    initRecordVector(shared.recV, opt);
    shared.rmode = strToReadMode(opt.modeStr.c_str());
    shared.longTxSize = opt.longTxSize;
    shared.nrOp = opt.nrOp;
    shared.wrRatio = opt.wrRatio;
    shared.nrWr4Long = opt.nrWr4Long;
    shared.shortTxMode = TxMode(opt.shortTxMode);
    shared.longTxMode = TxMode(opt.longTxMode);
    shared.usesBackOff = opt.usesBackOff != 0;
    shared.writePct = opt.writePct;
    shared.usesRMW = opt.usesRMW != 0;
    shared.nrTh4LongTx = opt.nrTh4LongTx;
    shared.payload = opt.payload;
    shared.usesZipf = opt.usesZipf;
    shared.zipfTheta = opt.zipfTheta;
    if (opt.usesZipf) {
        shared.zipfZetan = FastZipf::zeta(opt.getNrMu(), shared.zipfTheta);
    } else {
        shared.zipfZetan = 1.0;
    }
    shared.preverify = opt.preverify;
}


template <typename PQLock>
void dispatch1(CmdLineOptionPlus& opt)
{
    ILockShared<PQLock> shared;
    setShared<PQLock>(opt, shared);

    for (size_t i = 0; i < opt.nrLoop; i++) {
        if (opt.workload == "custom") {
            LiccResult res;
            runExec(opt, shared, worker0<PQLock>, res);
        } else if (opt.workload == "custom3") {
            Result2 res;
            runExec(opt, shared, worker1<PQLock>, res);
        } else {
            throw cybozu::Exception("dispatch1 unknown workload") << opt.workload;
        }
        epochGen_.reset();
    }
}


enum PQLockType
{
    // (0:none, 1:pqspin, 2:pqposix, 3:pqmcs1, 4:pqmcs2, 5:pq1993, 6:pq1997, 7:pqmcs3, 8:mcslike)");
    USE_PQNoneLock = 0,
    USE_PQSpinLock = 1,
    USE_PQPosixLock = 2,
    USE_PQMcsLock = 3,
    USE_PQMcsLock2 = 4,
    USE_PQ1993Lock = 5,
    USE_PQ1997Lock = 6, // buggy.
    USE_PQMcsLock3 = 7,
    USE_PQMcsLike = 8, // This is for LICC2.
};


void dispatch0(CmdLineOptionPlus& opt)
{
    switch (opt.pqLockType) {
    case USE_PQNoneLock:
        dispatch1<cybozu::lock::PQNoneLock>(opt);
        break;
#ifndef USE_LICC2
    case USE_PQSpinLock:
        dispatch1<cybozu::lock::PQSpinLock>(opt);
        break;
#if 0 // PQPosixLock does not support move constructor/assign.
    case USE_PQPosixLock:
        dispatch1<cybozu::lock::PQPosixLock>(opt);
        break;
#endif
#if 0 // disabled for saving compilation time.
    case USE_PQMcsLock:
        dispatch1<cybozu::lock::PQMcsLock>(opt);
        break;
    case USE_PQMcsLock2:
        dispatch1<cybozu::lock::PQMcsLock2>(opt);
        break;
    case USE_PQ1993Lock:
        dispatch1<cybozu::lock::PQ1993Lock>(opt);
        break;
    case USE_PQ1997Lock:
        dispatch1<cybozu::lock::PQ1997Lock>(opt);
        break;
#endif
    case USE_PQMcsLock3:
        dispatch1<cybozu::lock::PQMcsLock3>(opt);
        break;
#else // USE_LICC2
    case USE_PQMcsLike:
        dispatch1<cybozu::lock::licc2::PqMcsLike>(opt);
        break;
#endif // USE_LICC2
    default:
        throw cybozu::Exception("bad pqLockType") << opt.pqLockType;
    }
}

int main(int argc, char *argv[]) try
{
    CmdLineOptionPlus opt("licc_bench: benchmark with licc lock.");
    opt.parse(argc, argv);
    setCpuAffinityModeVec(opt.amode, CpuId_);

#ifdef NO_PAYLOAD
    if (opt.payload != 0) throw cybozu::Exception("payload not supported");
#endif

    dispatch0(opt);

} catch (std::exception& e) {
    ::fprintf(::stderr, "exception: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
