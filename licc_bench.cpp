#include <cstdio>
#include "licc.hpp"
#include "cmdline_option.hpp"
#include "measure_util.hpp"
#include "cpuid.hpp"
#include "time.hpp"
#include <algorithm>


struct CmdLineOptionPlus : CmdLineOption
{
    using base = CmdLineOption;

    std::string modeStr;

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendOpt(&modeStr, "licc-hybrid", "mode", "[mode]: specify mode in licc-pcc, licc-occ, licc-hybrid.");
    }
    std::string str() const {
        return cybozu::util::formatString("mode:%s %s", modeStr.c_str(), base::str().c_str());
    }
};


using ILockData = cybozu::lock::ILockData;
using ILock = cybozu::lock::ILock;
using IMutex = cybozu::lock::IMutex;

enum class IMode : uint8_t {
    S = 0, X = 1, INVALID = 2,
};

const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);

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


struct ILockShared
{
    std::vector<IMutex> muV;
    ReadMode rmode;
    size_t longTxSize;
    size_t nrOp;
    size_t nrWr;
    int shortTxMode;
    int longTxMode;
};


Result worker0(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, ILockShared& shared)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    std::vector<IMutex>& muV = shared.muV;
    const ReadMode rmode = shared.rmode;
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t nrWr = shared.nrWr;
    const int shortTxMode = shared.shortTxMode;
    const int longTxMode = shared.longTxMode;

    Result res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    std::vector<size_t> muIdV(nrOp);
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
    cybozu::lock::EpochGenerator epochGen;

#if 0
    GetModeFunc<decltype(rand), IMode>
        getMode(boolRand, isWriteV, isLongTx,
                shortTxMode, longTxMode, realNrOp, nrWr);
#endif
#if 0
    GetRecordIdxFunc<decltype(rand)>
        getRecordIdx(rand, isLongTx, shortTxMode, longTxMode, muV.size(), realNrOp);
#endif

    cybozu::lock::ILockSet lockSet;

    //std::unordered_map<size_t, size_t> retryMap;
    uint64_t tdiffTotal = 0;
    uint64_t count = 0;

    uint64_t nrSuccess = 1000; // initial abort rate is 0.1%.
    uint64_t nrAbort = 1;
    size_t factor = 1;

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

        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            assert(lockSet.isEmpty());
            //::printf("begin\n"); // QQQQQ
	    const uint64_t t0 = cybozu::time::rdtscp();
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
            nrSuccess++;
            break;
          abort:
            res.incAbort(isLongTx);
            lockSet.clear();
#if 0 // Backoff
            const size_t n = rand() % (1 << (retry + 10));
            for (size_t i = 0; i < n; i++) _mm_pause();
#elif 1
            nrAbort++;
            // Adaptive backoff.
            const uint64_t t1 = cybozu::time::rdtscp();
            const uint64_t tdiff = std::max<uint64_t>(t1 - t0, 2);
            tdiffTotal += tdiff; count++;
            uint64_t waittic = rand() % (tdiff << std::min<size_t>(retry + 1, 10));
            //uint64_t waittic = rand() % (tdiff << 18);
            uint64_t t2 = t1;
            while (t2 - t1 < waittic) {
                _mm_pause();
                t2 = cybozu::time::rdtscp();
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


void setShared(const CmdLineOptionPlus& opt, ILockShared& shared)
{
    shared.muV.resize(opt.getNrMu());
    shared.rmode = strToReadMode(opt.modeStr.c_str());
    shared.longTxSize = opt.longTxSize;
    shared.nrOp = opt.nrOp;
    shared.nrWr = opt.nrWr;
    shared.shortTxMode = opt.shortTxMode;
    shared.longTxMode = opt.longTxMode;
}


int main(int argc, char *argv[]) try
{
    CmdLineOptionPlus opt("licc_bench: benchmark with licc lock.");
    opt.parse(argc, argv);

    ILockShared shared;
    setShared(opt, shared);
    for (size_t i = 0; i < opt.nrLoop; i++) {
        runExec(opt, shared, worker0);
    }

} catch (std::exception& e) {
    ::fprintf(::stderr, "exception: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
