#include <cstdio>
#include "licc.hpp"
#include "cmdline_option.hpp"
#include "measure_util.hpp"
#include "cpuid.hpp"


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

    GetModeFunc<decltype(rand), IMode>
        getMode(boolRand, isWriteV, isLongTx,
                shortTxMode, longTxMode, realNrOp, nrWr);

    cybozu::lock::ILockSet lockSet;

    while (!start) _mm_pause();
    while (!quit) {
        if (!isLongTx && shortTxMode == USE_MIX_TX) {
            fillModeVec(isWriteV, rand, nrWr, tmpV);
        }

        ordIdGen.epochId = epochGen.get();
        const uint32_t ordId = ordIdGen.ordId;

        lockSet.init(ordId);
        //::printf("Tx begin\n"); // debug code

        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            assert(lockSet.isEmpty());

            for (size_t i = 0; i < realNrOp; i++) {
                //::printf("op %zu\n", i); // debug code
                IMode mode = getMode(i);
                IMutex& mutex = muV[rand() % muV.size()];

                if (mode == IMode::S) {
                    const bool tryInvisibleRead =
                        (rmode == ReadMode::OCC) ||
                        (rmode == ReadMode::HYBRID && !isLongTx && retry == 0);
                    if (tryInvisibleRead) {
                        lockSet.invisibleRead(mutex);
                    } else if (mode == IMode::S) {
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
            break;
          abort:
            res.incAbort(isLongTx);
            lockSet.clear();
        }
    }
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
