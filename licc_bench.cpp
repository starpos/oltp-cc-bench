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

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendOpt(&modeStr, "licc-hybrid", "mode", "[mode]: specify mode in licc-pcc, licc-occ, licc-hybrid.");
        appendOpt(&pqLockType, 0, "pqlock", "[id]: pqlock type (0:none, 1:pqspin, 3:pqmcs1, 4:pqmcs2, 5:pq1993, 6:pq1997, 7:pqmcs3)");
        appendOpt(&usesBackOff, 0, "backoff", "[0 or 1]: backoff 0:off 1:on");
        appendOpt(&writePct, 50, "writepct", "[pct]: write percentage (0 to 100) for custom3 workload");
    }
    std::string str() const {
        return cybozu::util::formatString(
            "mode:%s %s pqLockType:%d backoff:%d writePct:%zu"
            , modeStr.c_str(), base::str().c_str(), pqLockType, usesBackOff ? 1 : 0, writePct);
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


const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);

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

    std::vector<IMutex> muV;
    ReadMode rmode;
    size_t longTxSize;
    size_t nrOp;
    size_t nrWr;
    int shortTxMode;
    int longTxMode;
    bool usesBackOff;
    size_t writePct;
};


template <typename PQLock>
Result1 worker0(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, ILockShared<PQLock>& shared)
{
    using IMutex = typename ILockTypes<PQLock>::IMutex;
    using ILockSet = typename ILockTypes<PQLock>::ILockSet;

    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    std::vector<IMutex>& muV = shared.muV;
    const ReadMode rmode = shared.rmode;
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t nrWr = shared.nrWr;
    const int shortTxMode = shared.shortTxMode;
    const int longTxMode = shared.longTxMode;

    Result1 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    BoolRandom<decltype(rand)> boolRand(rand);
    std::vector<bool> isWriteV;
    std::vector<size_t> tmpV; // for fillModeVec
    const bool isLongTx = longTxSize != 0 && idx == 0;
    const size_t realNrOp = isLongTx ? longTxSize : nrOp;
    if (!isLongTx && shortTxMode == USE_MIX_TX) {
        isWriteV.resize(nrOp);
    }

    cybozu::lock::OrdIdGen ordIdGen;
    assert(idx < cybozu::lock::MAX_WORKER_ID);
    ordIdGen.workerId = idx;
    cybozu::lock::SimpleEpochGenerator epochGen;

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

        ordIdGen.epochId = epochGen.get();
        const uint32_t ordId = ordIdGen.ordId;

        lockSet.init(ordId);
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
                size_t key = getRecordIdx(rand, isLongTx, shortTxMode, longTxMode, muV.size(), realNrOp, i, firstRecIdx);
#else
                size_t key = rand() % muV.size();
#endif
                //::printf("mode %c key %zu\n", mode == IMode::S ? 'S' : 'X', key); // QQQQQ
                IMutex& mutex = muV[key];

                if (mode == IMode::S) {
                    const bool tryInvisibleRead =
                        (rmode == ReadMode::OCC) ||
                        (rmode == ReadMode::HYBRID && !isLongTx && retry == 0);
                    if (tryInvisibleRead) {
                        lockSet.invisibleRead(mutex);
                    } else {
                        if (!lockSet.reservedRead(mutex)) goto abort;
                    }
                } else {
                    assert(mode == IMode::X);
                    if (!lockSet.write(mutex)) goto abort;
                }
            }
            //::printf("try protect %zu\n", retry); // debug code
            if (!lockSet.protectAll()) goto abort;
            if (!lockSet.verifyAndUnlock()) goto abort;
            lockSet.updateAndUnlock();
            res.incCommit(isLongTx);
            res.addRetryCount(isLongTx, retry);
            //retryMap[retry]++;
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


template <typename PQLock>
Result2 worker1(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, ILockShared<PQLock>& shared)
{
    using IMutex = typename ILockTypes<PQLock>::IMutex;
    using ILockSet = typename ILockTypes<PQLock>::ILockSet;

    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    std::vector<IMutex>& muV = shared.muV;
    const ReadMode rmode = shared.rmode;

    const size_t txSize = [&]() -> size_t {
        if (idx == 0) {
            return std::max<size_t>(muV.size() / 2, 10);
        } else if (idx <= 5) {
            return std::max<size_t>(muV.size() / 10, 10);
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

    ILockSet lockSet;

    while (!start) _mm_pause();
    while (!quit) {
#if 0
        ordIdGen.epochId = epochGen.get();
        const uint32_t ordId = ordIdGen.ordId;
#else
        const uint32_t ordId = epochTxIdGen.get();
#endif

        lockSet.init(ordId);
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
                size_t key = rand() % muV.size();
                IMutex& mutex = muV[key];
                if (mode == IMode::S) {
                    const bool tryInvisibleRead =
                        (rmode == ReadMode::OCC) ||
                        (rmode == ReadMode::HYBRID && !isLongTx && retry == 0);
                    if (tryInvisibleRead) {
                        lockSet.invisibleRead(mutex);
                    } else {
                        if (!lockSet.reservedRead(mutex)) goto abort;
                    }
                } else {
                    assert(mode == IMode::X);
                    if (!lockSet.write(mutex)) goto abort;
                }
            }
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
    shared.muV.resize(opt.getNrMu());
    shared.rmode = strToReadMode(opt.modeStr.c_str());
    shared.longTxSize = opt.longTxSize;
    shared.nrOp = opt.nrOp;
    shared.nrWr = opt.nrWr;
    shared.shortTxMode = opt.shortTxMode;
    shared.longTxMode = opt.longTxMode;
    shared.usesBackOff = opt.usesBackOff != 0;
    shared.writePct = opt.writePct;
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
        }
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
    dispatch0(opt);

} catch (std::exception& e) {
    ::fprintf(::stderr, "exception: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
