#include <ctime>
#include <vector>
#include <chrono>
#include <unistd.h>
#include "tictoc.hpp"
#include "thread_util.hpp"
#include "random.hpp"
#include "measure_util.hpp"
#include "cpuid.hpp"

using Mutex = cybozu::tictoc::Mutex;

const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);


struct Shared
{
    std::vector<Mutex> muV;
    size_t longTxSize;
    size_t nrOp;
    size_t nrWr;
    int shortTxMode;
    int longTxMode;
};


enum class Mode : bool { S = false, X = true, };


Result worker(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, Shared& shared)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    std::vector<Mutex>& muV = shared.muV;
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t nrWr = shared.nrWr;
    const int shortTxMode = shared.shortTxMode;
    const int longTxMode = shared.longTxMode;

    Result res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    std::vector<size_t> muIdV(nrOp);
    cybozu::tictoc::WriteSet ws;
    cybozu::tictoc::ReadSet rs;
    cybozu::tictoc::LockSet ls;
    cybozu::tictoc::Flags flags;
    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    // USE_MIX_TX
    std::vector<bool> isWriteV(nrOp);
    std::vector<size_t> tmpV2; // for fillModeVec.

    // USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);

    const bool isLongTx = longTxSize != 0 && idx == 0; // starvation setting.
    if (isLongTx) {
        muIdV.resize(longTxSize);
    } else {
        muIdV.resize(nrOp);
        if (shortTxMode == USE_MIX_TX) {
            isWriteV.resize(nrOp);
        }
    }
    GetModeFunc<decltype(rand), Mode>
        getMode(boolRand, isWriteV, isLongTx,
                shortTxMode, longTxMode, muIdV.size(), nrWr);


    while (!start) _mm_pause();
    size_t count = 0; unused(count);
    while (!quit) {
        if (isLongTx) {
            if (longTxSize > muV.size() * 5 / 1000) {
                fillMuIdVecArray(muIdV, rand, muV.size(), tmpV);
            } else {
                fillMuIdVecLoop(muIdV, rand, muV.size());
            }
        } else {
            fillMuIdVecLoop(muIdV, rand, muV.size());
            if (shortTxMode == USE_MIX_TX) {
                fillModeVec(isWriteV, rand, nrWr, tmpV2);
            }
        }

        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            // Try to run transaction.
            for (size_t i = 0; i < muIdV.size(); i++) {
                const bool isWrite = bool(getMode(i));

                // read
                cybozu::tictoc::Reader r;
                r.prepare(&muV[muIdV[i]]);
                for (;;) {
                    // should read data here.
                    r.readFence();
                    if (r.isReadSucceeded()) break;
                    r.prepareRetry();
                }
                rs.push_back(std::move(r));
                if (isWrite) {
                    // write
                    ws.emplace_back(&muV[muIdV[i]]);
                }
            }
            if (!cybozu::tictoc::preCommit(rs, ws, ls, flags)) {
                res.incAbort(isLongTx);
                continue;
            }
            res.incCommit(isLongTx);
            res.addRetryCount(isLongTx, retry);
            break;
        }

#if 0
        // This is starvation expr only
        count++;
        if (isLongTx && (longTxSize >= 1 * muV.size() / 100) && count >= 5) {
            shouldQuit = true;
            break;
        }
#endif
    }
    return res;
}


Result worker2(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, Shared& shared)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    std::vector<Mutex>& muV = shared.muV;
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t nrWr = shared.nrWr;
    const int shortTxMode = shared.shortTxMode;
    const int longTxMode = shared.longTxMode;

    Result res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    std::vector<size_t> muIdV(nrOp);
    cybozu::tictoc::LocalSet localSet;
    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    // USE_MIX_TX
    std::vector<bool> isWriteV(nrOp);
    std::vector<size_t> tmpV2; // for fillModeVec.

    // USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);

    const bool isLongTx = longTxSize != 0 && idx == 0; // starvation setting.
    if (isLongTx) {
        muIdV.resize(longTxSize);
    } else {
        muIdV.resize(nrOp);
        if (shortTxMode == USE_MIX_TX) {
            isWriteV.resize(nrOp);
        }
    }
    GetModeFunc<decltype(rand), Mode>
        getMode(boolRand, isWriteV, isLongTx,
                shortTxMode, longTxMode, muIdV.size(), nrWr);


    while (!start) _mm_pause();
    size_t count = 0; unused(count);
    while (!quit) {
        if (isLongTx) {
            if (longTxSize > muV.size() * 5 / 1000) {
                fillMuIdVecArray(muIdV, rand, muV.size(), tmpV);
            } else {
                fillMuIdVecLoop(muIdV, rand, muV.size());
            }
        } else {
            fillMuIdVecLoop(muIdV, rand, muV.size());
            if (shortTxMode == USE_MIX_TX) {
                fillModeVec(isWriteV, rand, nrWr, tmpV2);
            }
        }

        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            // Try to run transaction.
            for (size_t i = 0; i < muIdV.size(); i++) {
                const bool isWrite = bool(getMode(i));
                Mutex& mutex = muV[muIdV[i]];
                if (!isWrite) {
                    localSet.read(mutex);
                } else {
                    localSet.write(mutex);
                }
            }
            if (!localSet.preCommit()) {
                localSet.clear();
                res.incAbort(isLongTx);
                continue;
            }
            res.incCommit(isLongTx);
            res.addRetryCount(isLongTx, retry);
            break;
        }
    }
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
                //sleepMs(1000);
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

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
    }
    std::string str() const {
        return cybozu::util::formatString("mode:tictoc ") + base::str();
    }
};


int main(int argc, char *argv[]) try
{
    CmdLineOptionPlus opt("tictoc_bench: benchmark with tictoc.");
    opt.parse(argc, argv);

    if (opt.workload == "custom") {
        Shared shared;
        shared.muV.resize(opt.getNrMu());
        shared.longTxSize = opt.longTxSize;
        shared.nrOp = opt.nrOp;
        shared.nrWr = opt.nrWr;
        shared.shortTxMode = opt.shortTxMode;
        shared.longTxMode = opt.longTxMode;
        for (size_t i = 0; i < opt.nrLoop; i++) {
            runExec(opt, shared, worker2);
        }
    } else {
        throw cybozu::Exception("bad workload.") << opt.workload;
    }
} catch (std::exception& e) {
    ::fprintf(::stderr, "exeption: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
