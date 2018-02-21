#include <cstdio>
#include "licc.hpp"
#include "pqlock.hpp"
#include "cmdline_option.hpp"
#include "measure_util.hpp"
#include "cpuid.hpp"
#include "time.hpp"
#include "tx_util.hpp"
#include <algorithm>


struct CmdLineOptionPlus : CmdLineOption
{
    using base = CmdLineOption;

    std::string modeStr;
    int pqLockType;
    int usesBackOff; // 0 or 1.
    size_t writePct; // 0 to 100.
    int usesRMW; // 0 or 1.

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendOpt(&modeStr, "licc-hybrid", "mode", "[mode]: specify mode in licc-pcc, licc-occ, licc-hybrid.");
        appendOpt(&pqLockType, 0, "pqlock", "[id]: pqlock type (0:none, 1:pqspin, 3:pqmcs1, 4:pqmcs2, 5:pq1993, 6:pq1997, 7:pqmcs3)");
        appendOpt(&usesBackOff, 0, "backoff", "[0 or 1]: backoff 0:off 1:on");
        appendOpt(&usesRMW, 1, "rmw", "[0 or 1]: use read-modify-write or normal write 0:w 1:rmw (default: 1)");
        appendOpt(&writePct, 50, "writepct", "[pct]: write percentage (0 to 100) for custom3 workload");
    }
    std::string str() const {
        return cybozu::util::formatString(
            "mode:%s %s pqLockType:%d backoff:%d writePct:%zu rmw:%d"
            , modeStr.c_str(), base::str().c_str(), pqLockType
            , usesBackOff ? 1 : 0, writePct, usesRMW ? 1 : 0);
    }
};


using ILockData = cybozu::lock::ILockData;

enum class IMode : uint8_t {
    S = 0, X = 1, INVALID = 2,
};

template <typename PQLock>
struct ILockTypes
{
    using ILock = cybozu::lock::ILock<PQLock>;
    using IMutex = cybozu::lock::IMutex<PQLock>;
    using ILockSet = cybozu::lock::ILockSet<PQLock>;
};


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
    VectorWithPayload<IMutex> recV;
    ReadMode rmode;
    size_t longTxSize;
    size_t nrOp;
    size_t nrWr;
    int shortTxMode;
    int longTxMode;
    bool usesBackOff;
    size_t writePct;
    bool usesRMW;
    size_t nrTh4LongTx;
    size_t payload;
};


//#define MONITOR_LATENCY
#undef MONITOR_LATENCY


template <typename PQLock>
Result1 worker0(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, ILockShared<PQLock>& shared)
{
    using IMutex = typename ILockTypes<PQLock>::IMutex;
    using ILockSet = typename ILockTypes<PQLock>::ILockSet;

    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    //std::vector<IMutex>& muV = shared.muV;
    VectorWithPayload<IMutex>& recV = shared.recV;
    const ReadMode rmode = shared.rmode;
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t nrWr = shared.nrWr;
    const int shortTxMode = shared.shortTxMode;
    const int longTxMode = shared.longTxMode;
    const bool usesRMW = shared.usesRMW;

    Result1 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    BoolRandom<decltype(rand)> boolRand(rand);
    std::vector<bool> isWriteV;
    std::vector<size_t> tmpV; // for fillModeVec
    const bool isLongTx = longTxSize != 0 && idx < shared.nrTh4LongTx;
    const size_t realNrOp = isLongTx ? longTxSize : nrOp;
    if (!isLongTx && shortTxMode == USE_MIX_TX) {
        isWriteV.resize(nrOp);
    }

#if 0
    cybozu::lock::OrdIdGen ordIdGen;
    assert(idx < cybozu::lock::MAX_WORKER_ID);
    ordIdGen.workerId = idx;
    cybozu::lock::SimpleEpochGenerator epochGen;
#else
    EpochTxIdGenerator<8, 2> epochTxIdGen(idx + 1, epochGen_);
#endif

#if 0
    GetModeFunc<decltype(rand), IMode>
        getMode(boolRand, isWriteV, isLongTx,
                shortTxMode, longTxMode, realNrOp, nrWr);
#endif
#if 0
    GetRecordIdxFunc<decltype(rand)>
        getRecordIdx(rand, isLongTx, shortTxMode, longTxMode, muV.size(), realNrOp);
#endif

    ILockSet lockSet;
    std::vector<uint8_t> value(shared.payload);

    //std::unordered_map<size_t, size_t> retryMap;
#if 0
    uint64_t tdiffTotal = 0;
    uint64_t count = 0;

    uint64_t nrSuccess = 1000; // initial abort rate is 0.1%.
    uint64_t nrAbort = 1;
    size_t factor = 1;
#endif

    while (!start) _mm_pause();
    while (!quit) {
        if (!isLongTx && shortTxMode == USE_MIX_TX) {
            fillModeVec(isWriteV, rand, nrWr, tmpV);
        }

#if 0
        ordIdGen.epochId = epochGen.get();
        const uint32_t ordId = ordIdGen.ordId;
#else
        const uint32_t ordId = epochTxIdGen.get();
#endif

        lockSet.init(ordId, shared.payload, realNrOp);
        //::printf("Tx begin\n"); // debug code
        size_t firstRecIdx;

        uint64_t t0;
        if (shared.usesBackOff) {
            t0 = cybozu::time::rdtscp();
        }
        auto randState = rand.getState();
        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            assert(lockSet.isEmpty());
            rand.setState(randState);
            //::printf("begin\n"); // QQQQQ
#ifdef MONITOR_LATENCY
            uint64_t ts[6];
            ts[0] = cybozu::time::rdtscp();
#endif
            for (size_t i = 0; i < realNrOp; i++) {
                //::printf("op %zu\n", i); // debug code
#if 0
                IMode mode = getMode(i);
#else
                IMode mode = getMode<decltype(rand), IMode>(
                    boolRand, isWriteV, isLongTx, shortTxMode, longTxMode,
                    realNrOp, nrWr, i);
#endif
#if 0
                size_t key = getRecordIdx(i);
#elif 1
                size_t key = getRecordIdx(rand, isLongTx, shortTxMode, longTxMode, recV.size(), realNrOp, i, firstRecIdx);
#else
                size_t key = rand() % recV.size();
#endif
                //::printf("mode %c key %zu\n", mode == IMode::S ? 'S' : 'X', key); // QQQQQ
                auto& rec = recV[key];
                IMutex& mutex = rec.value;
                void *sharedValue = rec.payload;
                if (mode == IMode::S) {
                    const bool tryInvisibleRead =
                        (rmode == ReadMode::OCC) ||
                        (rmode == ReadMode::HYBRID && !isLongTx && retry == 0);
                    if (tryInvisibleRead) {
                        lockSet.invisibleRead(mutex, sharedValue, &value[0]);
                    } else {
                        if (!lockSet.reservedRead(mutex, sharedValue, &value[0])) goto abort;
                    }
                } else {
                    assert(mode == IMode::X);
                    if (usesRMW) {
                        if (!lockSet.readForUpdate(mutex, sharedValue, &value[0])) goto abort;
                        if (!lockSet.write(mutex, sharedValue, &value[0])) goto abort;
                    } else {
                        if (!lockSet.write(mutex, sharedValue, &value[0])) goto abort;
                    }
                }
            }
#ifdef MONITOR_LATENCY
            ts[1] = cybozu::time::rdtscp();
#endif
            //::printf("try protect %zu\n", retry); // debug code
            lockSet.blindWriteReserveAll();
#ifdef MONITOR_LATENCY
            ts[2] = cybozu::time::rdtscp();
#endif
            if (!lockSet.protectAll()) goto abort;
#ifdef MONITOR_LATENCY
            ts[3] = cybozu::time::rdtscp();
#endif
            if (!lockSet.verifyAndUnlock()) goto abort;
#ifdef MONITOR_LATENCY
            ts[4] = cybozu::time::rdtscp();
#endif
            lockSet.updateAndUnlock();
#ifdef MONITOR_LATENCY
            ts[5] = cybozu::time::rdtscp();
#endif
            res.incCommit(isLongTx);
            res.addRetryCount(isLongTx, retry);
            //retryMap[retry]++;

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

#if 0
            nrSuccess++;
#endif
            break;
          abort:
            res.incAbort(isLongTx);
            lockSet.clear();
#if 0 // Backoff
            const size_t n = rand() % (1 << (retry + 10));
            for (size_t i = 0; i < n; i++) _mm_pause();
#elif 1
#if 0
            nrAbort++;
#endif
            if (shared.usesBackOff) {
                backOff(t0, retry, rand);
            }
#elif 0
            nrAbort++;
            if ((rand() & 0x1) == 0) continue; // do not use backoff.
            if (nrAbort < 10000) {
                // do noghint
            } else if (nrAbort * 10 / (nrSuccess + nrAbort) > 0) {
                // Abort rate > 10%
                factor = std::min<size_t>(factor + 1, 20);
            } else if (nrAbort * 10 / (nrSuccess + nrAbort) == 0) {
                // Abort rate < 10%
                factor = std::max<size_t>(factor - 1, 1);
            }
            if (nrSuccess + nrAbort > UINT64_MAX / 2) {
                nrSuccess /= 2;
                nrAbort = std::max<uint64_t>(nrAbort / 2, 1);
            }
            const uint64_t t1 = cybozu::time::rdtscp();
            const uint64_t tdiff = std::max<uint64_t>(t1 - t0, 2);
            uint64_t waittic = rand() % (tdiff << factor);
            uint64_t t2 = t1;
            while (t2 - t1 < waittic) {
                _mm_pause();
                t2 = cybozu::time::rdtscp();
            }
            t0 = t2;
#endif
        }
    }
    //for (const auto& pair: retryMap) {
     //   ::printf("retry:%zu\tcount:%zu\n", pair.first, pair.second);
    //}
    //::printf("%zu\n", tdiffTotal / count);
    //::printf("idx %zu factor %zu nrSuccess %zu nrAbort %zu\n", idx, factor, nrSuccess, nrAbort);
    return res;
}


/**
 * This worker is for short-long-long workload.
 */
template <typename PQLock>
Result2 worker1(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, ILockShared<PQLock>& shared)
{
    using IMutex = typename ILockTypes<PQLock>::IMutex;
    using ILockSet = typename ILockTypes<PQLock>::ILockSet;

    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    //std::vector<IMutex>& muV = shared.muV;
    //std::vector<Record<IMutex> >& recV = shared.recV;
    auto& recV = shared.recV;
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
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    BoolRandom<decltype(rand)> boolRand(rand);
    const size_t realNrOp = txSize;

#if 0
    cybozu::lock::OrdIdGen ordIdGen;
    assert(idx + 1 < cybozu::lock::MAX_WORKER_ID);
    ordIdGen.workerId = idx + 1;
    cybozu::lock::SimpleEpochGenerator epochGen;
#else
    EpochTxIdGenerator<8, 2> epochTxIdGen(idx + 1, epochGen_);
#if 0
    if (idx == 0) {
        epochTxIdGen.setOrderId(0);
    } else if (idx <= 5) {
        epochTxIdGen.setOrderId(1);
    } else {
        epochTxIdGen.setOrderId(3);
    }
#endif
#if 0
    if (idx == 0) {
        epochTxIdGen.boost(1000);
    } else if (idx <= 5) {
        epochTxIdGen.boost(100);
    }
#endif
#endif

    std::vector<uint8_t> value(shared.payload);
    ILockSet lockSet;

    while (!start) _mm_pause();
    while (!quit) {
#if 0
        ordIdGen.epochId = epochGen.get();
        const uint32_t ordId = ordIdGen.ordId;
#else
        const uint32_t ordId = epochTxIdGen.get();
#endif

        lockSet.init(ordId, shared.payload, realNrOp);
        uint64_t t0;
        if (shared.usesBackOff) t0 = cybozu::time::rdtscp();
        auto randState = rand.getState();
        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            assert(lockSet.isEmpty());
            rand.setState(randState);
            boolRand.reset();
            for (size_t i = 0; i < realNrOp; i++) {
#if 0
                IMode mode = boolRand() ? IMode::X : IMode::S;
#else
                IMode mode = rand() % 100 < shared.writePct ? IMode::X : IMode::S;
#endif
                size_t key = rand() % recV.size();
                auto& rec = recV[key];
                IMutex& mutex = rec.value;
                void *sharedValue = rec.payload;
                if (mode == IMode::S) {
                    const bool tryInvisibleRead =
                        (rmode == ReadMode::OCC) ||
                        (rmode == ReadMode::HYBRID && !isLongTx && retry == 0);
                    if (tryInvisibleRead) {
                        lockSet.invisibleRead(mutex, sharedValue, &value[0]);
                    } else {
                        if (!lockSet.reservedRead(mutex, sharedValue, &value[0])) goto abort;
                    }
                } else {
                    assert(mode == IMode::X);
                    if (shared.usesRMW) {
                        if (!lockSet.readForUpdate(mutex, sharedValue, &value[0])) goto abort;
                        if (!lockSet.write(mutex, sharedValue, &value[0])) goto abort;
                    } else {
                        if (!lockSet.write(mutex, sharedValue, &value[0])) goto abort;
                    }
                }
            }
            lockSet.blindWriteReserveAll();
            if (!lockSet.protectAll()) goto abort;
            if (!lockSet.verifyAndUnlock()) goto abort;
            lockSet.updateAndUnlock();
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

#ifdef MUTEX_ON_CACHELINE
    shared.recV.setPayloadSize(opt.payload, CACHE_LINE_SIZE);
#else
    shared.recV.setPayloadSize(opt.payload);
#endif
    shared.recV.resize(opt.getNrMu());
    shared.rmode = strToReadMode(opt.modeStr.c_str());
    shared.longTxSize = opt.longTxSize;
    shared.nrOp = opt.nrOp;
    shared.nrWr = opt.nrWr;
    shared.shortTxMode = opt.shortTxMode;
    shared.longTxMode = opt.longTxMode;
    shared.usesBackOff = opt.usesBackOff != 0;
    shared.writePct = opt.writePct;
    shared.usesRMW = opt.usesRMW != 0;
    shared.nrTh4LongTx = opt.nrTh4LongTx;
    shared.payload = opt.payload;
}


template <typename PQLock>
void dispatch1(CmdLineOptionPlus& opt)
{
    ILockShared<PQLock> shared;
    setShared<PQLock>(opt, shared);

    for (size_t i = 0; i < opt.nrLoop; i++) {
        if (opt.workload == "custom") {
            Result1 res;
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
    // (0:none, 1:pqspin, 2:pqposix, 3:pqmcs1, 4:pqmcs2, 5:pq1993, 6:pq1997, 7:pqmcs3)");
    USE_PQNoneLock = 0,
    USE_PQSpinLock = 1,
    USE_PQPosixLock = 2,
    USE_PQMcsLock = 3,
    USE_PQMcsLock2 = 4,
    USE_PQ1993Lock = 5,
    USE_PQ1997Lock = 6, // buggy.
    USE_PQMcsLock3 = 7,
};


void dispatch0(CmdLineOptionPlus& opt)
{
    switch (opt.pqLockType) {
    case USE_PQNoneLock:
        dispatch1<cybozu::lock::PQNoneLock>(opt);
        break;
    case USE_PQSpinLock:
        dispatch1<cybozu::lock::PQSpinLock>(opt);
        break;
#if 0 // PQPosixLock does not support move constructor/assign.
    case USE_PQPosixLock:
        dispatch1<cybozu::lock::PQPosixLock>(opt);
        break;
#endif
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
    case USE_PQMcsLock3:
        dispatch1<cybozu::lock::PQMcsLock3>(opt);
        break;
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
