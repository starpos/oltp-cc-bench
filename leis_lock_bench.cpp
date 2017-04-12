#include <unistd.h>
#include <immintrin.h>
#include "thread_util.hpp"
#include "random.hpp"
#include "cpuid.hpp"
#include "measure_util.hpp"
#include "leis_lock.hpp"


using LeisLockSet  = cybozu::lock::LeisLockSet;
using Mutex = LeisLockSet::Mutex;
using Mode = LeisLockSet::Mode;

const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);


struct Shared
{
    std::string workload;
    size_t nrMuPerTh;
    std::vector<Mutex> muV;
    int shortMode;
    size_t longTxSize;
    size_t nrOp;
    size_t nrWr;

    std::string str() const {
        return cybozu::util::formatString(
            "mode:%s workload:%s longTxSize:%zu nrMutex:%zu nrMutexPerTh:%zu "
            "shortMode:%d"
            , "leis", workload.c_str()
            , longTxSize, muV.size(), nrMuPerTh, shortMode);
    }
};


template <int shortMode>
Result worker(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, Shared& shared)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    std::vector<Mutex>& muV = shared.muV;
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t nrWr = shared.nrWr;

    Result res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    std::vector<size_t> muIdV(nrOp);
    LeisLockSet llSet;
    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    // USE_MIX_TX
    std::vector<bool> isWriteV(nrOp);
    std::vector<size_t> tmpV2; // for fillModeVec.

    // USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);

    while (!start) _mm_pause();
    size_t count = 0; unused(count);
    while (!quit) {
        const bool isLongTx = longTxSize != 0 && idx == 0; // starvation setting.
        if (isLongTx) {
            muIdV.resize(longTxSize);
            if (longTxSize > muV.size() * 5 / 1000) { // over 0.5%
                fillMuIdVecArray(muIdV, rand, muV.size(), tmpV);
            } else {
                fillMuIdVecLoop(muIdV, rand, muV.size());
            }
        } else {
            muIdV.resize(nrOp);
            fillMuIdVecLoop(muIdV, rand, muV.size());
            if (shortMode == USE_MIX_TX ) {
                isWriteV.resize(nrOp);
                fillModeVec(isWriteV, rand, nrWr, tmpV2);
            }
        }

#if 0
        // leis-lock will get best performance with total-ordered accesses.
        std::sort(muIdV.begin(), muIdV.end());
#endif

        assert(llSet.empty());
        for (size_t retry = 0;; retry++) {
            bool abort = false;
            const size_t sz = muIdV.size();
            unused(sz);
            for (size_t i = 0; i < muIdV.size(); i++) {
                //Mode mode = (rand() % 2 == 0 ? Mode::X : Mode::S);
                Mode mode;
                if (shortMode == USE_LONG_TX_2) {
                    mode = boolRand() ? Mode::X : Mode::S;
                } else if (shortMode == USE_READONLY_TX) {
                    mode = Mode::S;
                } else if (shortMode == USE_WRITEONLY_TX) {
                    mode = Mode::X;
                } else if (shortMode == USE_MIX_TX) {
                    mode = isWriteV[i] ? Mode::X : Mode::S;
                } else {
                    assert(shortMode == USE_R2W2);
                    // Last two will be write op.
                    if (i == sz - 1 || i == sz - 2) {
                        mode = Mode::X;
                    } else {
                        mode = Mode::S;
                    }
                }

                if (!llSet.lock(&muV[muIdV[i]], mode)) {
                    res.incAbort(isLongTx);
                    abort = true;
                    break;
                }
            }
            if (abort) {
                llSet.recover();
                continue;
            }

            res.incCommit(isLongTx);
            llSet.unlock();
            res.addRetryCount(isLongTx, retry);
            break; // retry is not required.
        }

#if 0
        // This is startvation expr only.
        count++;
        //if (isLongTx && (longTxSize >= 5 * muV.size() / 100) && count >= 10) {
        if (isLongTx && count >= 5) {
            shouldQuit = true;
            break;
        }
#endif
    }
    return res;
}

void runTest()
{
#if 0
    //for (size_t nrResPerTh : {4}) {
    for (size_t nrResPerTh : {4000}) {
    //for (size_t nrResPerTh : {4, 4000}) {
        //for (size_t nrTh : {32}) {
        for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
            if (nrTh > 2 && nrTh % 2 != 0) continue;
            for (size_t i = 0; i < 10; i++) {
                bool verbose = false;
                runExec(nrResPerTh, nrTh, 10, verbose, 0, 4, 2);
                //sleepMs(1000);
            }
        }
    }
#endif
#if 0
    runExec(5000, 8, 10, true);
#endif
#if 0
    // high-contention expr.
    for (size_t nrMutex : {40}) {
        //for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
        //for (size_t nrTh : {32}) {
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
    //for (size_t longTxPct : {1}) {
    for (size_t longTxPml : {1, 2, 3, 4, 5, 6, 7, 8, 9,
                10, 20, 30, 40, 50, 60, 70, 80, 90,
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}) {
        const size_t longTxSize = longTxPml * nrMutex / 1000;
        for (size_t i = 0; i < 10; i++) {
            bool verbose = false;
            //size_t maxSec = longTxPct >= 5 ? 20000 : 100;
            //size_t maxSec = 30000;
            size_t maxSec = 100;
            runExec(nrMutex / nrTh, nrTh, maxSec, verbose, longTxSize);
            sleepMs(1000);
        }
    }
#endif
}

struct CmdLineOptionPlus : CmdLineOption
{
    using base = CmdLineOption;

    size_t nrMuPerTh;
    std::string workload;
    int shortMode;
    size_t longTxSize;

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendMust(&nrMuPerTh, "mu", "[num]: number of mutexes per thread.");
        appendMust(&workload, "w", "[workload]: workload type in 'shortlong', 'high-contention', 'high-conflicts'.");
        appendOpt(&shortMode, 0, "sm", "[id]: short workload mode (0:r2w2, 1:long2, 2:ro, 3:wo, 4:mix)");
        appendOpt(&longTxSize, 0, "long-tx-size", "[size]: long tx size. 0 means no long tx.");
    }
    void parse(int argc, char *argv[]) {
        base::parse(argc, argv);
        if (nrMuPerTh == 0) {
            throw cybozu::Exception("nrMuPerTh must not be 0.");
        }
        if (longTxSize > nrMuPerTh * nrTh) {
            throw cybozu::Exception("longTxSize is too large: up to nrMuPerTh * nrTh.");
        }
    }
};


void dispatch1(CmdLineOptionPlus& opt, Shared& shared)
{
    switch (opt.shortMode) {
    case USE_R2W2:
        runExec(opt, shared, worker<USE_R2W2>);
        break;
    case USE_LONG_TX_2:
        runExec(opt, shared, worker<USE_LONG_TX_2>);
        break;
    case USE_READONLY_TX:
        runExec(opt, shared, worker<USE_READONLY_TX>);
        break;
    case USE_WRITEONLY_TX:
        runExec(opt, shared, worker<USE_WRITEONLY_TX>);
        break;
    case USE_MIX_TX:
        runExec(opt, shared, worker<USE_MIX_TX>);
        break;
    default:
        throw cybozu::Exception("bad shortMode") << opt.shortMode;
    }
}

int main(int argc, char *argv[]) try
{
    CmdLineOptionPlus opt("leis_lock_bench: benchmark with leis lock.");
    opt.parse(argc, argv);

    if (opt.workload == "shortlong") {
        Shared shared;
        shared.workload = opt.workload;
        shared.nrMuPerTh = opt.nrMuPerTh;
        shared.muV.resize(opt.nrMuPerTh * opt.nrTh);
        shared.shortMode = opt.shortMode;
        shared.longTxSize = opt.longTxSize;
        shared.nrOp = 4;
        shared.nrWr = 2;
        for (size_t i = 0; i < opt.nrLoop; i++) {
            dispatch1(opt, shared);
        }
    } else {
        throw cybozu::Exception("bad workload.") << opt.workload;
    }
} catch (std::exception& e) {
    ::fprintf(::stderr, "exeption: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
