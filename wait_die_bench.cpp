#include "thread_util.hpp"
#include "random.hpp"
#include <immintrin.h>
#include <unistd.h>
#include "cpuid.hpp"
#include "measure_util.hpp"

#include "wait_die.hpp"
#include "tx_util.hpp"

using Lock = cybozu::wait_die::WaitDieLock;
using Mutex = Lock::Mutex;
using Mode = Lock::Mode;

const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);


struct Shared
{
    std::vector<Mutex> muV;
    size_t longTxSize;
    size_t nrOp;
    size_t nrWr;
    int shortTxMode;
    int longTxMode;

    GlobalTxIdGenerator globalTxIdGen;
    SimpleTxIdGenerator simpleTxIdGen;

    Shared() : globalTxIdGen(5, 10) {}
};


template <int txIdGenType>
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
    std::vector<Lock> lockV;
    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    PriorityIdGenerator<12> priIdGen;
    priIdGen.init(idx + 1);
    TxIdGenerator localTxIdGen(&shared.globalTxIdGen);

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
            if (longTxSize > muV.size() * 5 / 1000) { // over 0.5%
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

        // Do not sort with noWait mode.
        //std::sort(muIdV.begin(), muIdV.end());

        uint64_t txId;
        if (txIdGenType == SCALABLE_TXID_GEN) {
            txId = priIdGen.get(isLongTx ? 0 : 1);
        } else if (txIdGenType == BULK_TXID_GEN) {
            txId = localTxIdGen.get();
        } else if (txIdGenType == SIMPLE_TXID_GEN) {
            txId = shared.simpleTxIdGen.get();
        } else {
            throw cybozu::Exception("bad txIdGenType") << txIdGenType;
        }

        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            assert(lockV.empty());
            bool abort = false;
            const size_t sz = muIdV.size();
            unused(sz);
            for (size_t i = 0; i < muIdV.size(); i++) {
                Mode mode = getMode(i);
                lockV.emplace_back();
                if (!lockV.back().lock(&muV[muIdV[i]], mode, txId)) {
                    res.incAbort(isLongTx);
                    abort = true;
                    break;
                }
            }
            if (abort) {
                lockV.clear();
                continue;
            }

            res.incCommit(isLongTx);
            lockV.clear();
            res.addRetryCount(isLongTx, retry);
            break; // retry is not required.
        }

#if 0
        // This is starvation expr only.
        count++;
        if (isLongTx && (longTxSize >= 5 * muV.size() / 100) && count >= 5) {
            shouldQuit = true;
            break;
        }
#endif
    }
    return res;
}


template <int txIdGenType>
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
    cybozu::wait_die::LockSet lockSet;
    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    PriorityIdGenerator<12> priIdGen;
    priIdGen.init(idx + 1);
    TxIdGenerator localTxIdGen(&shared.globalTxIdGen);

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
            if (longTxSize > muV.size() * 5 / 1000) { // over 0.5%
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

        // Do not sort with noWait mode.
        //std::sort(muIdV.begin(), muIdV.end());

        uint64_t txId;
        if (txIdGenType == SCALABLE_TXID_GEN) {
            txId = priIdGen.get(isLongTx ? 0 : 1);
        } else if (txIdGenType == BULK_TXID_GEN) {
            txId = localTxIdGen.get();
        } else if (txIdGenType == SIMPLE_TXID_GEN) {
            txId = shared.simpleTxIdGen.get();
        } else {
            throw cybozu::Exception("bad txIdGenType") << txIdGenType;
        }
        lockSet.setTxId(txId);

        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            assert(lockSet.empty());
            bool abort = false;
            for (size_t i = 0; i < muIdV.size(); i++) {
                Mode mode = getMode(i);
                Mutex& mutex = muV[muIdV[i]];
                if (!lockSet.lock(mutex, mode)) {
                    abort = true;
                    break;
                }
            }
            if (abort) {
                lockSet.clear();
                res.incAbort(isLongTx);
                continue;
            }

            res.incCommit(isLongTx);
            lockSet.clear();
            res.addRetryCount(isLongTx, retry);
            break; // retry is not required.
        }
    }
    return res;
}


void runTest()
{
#if 0
    //for (size_t nrResPerTh : {40}) {
    //for (size_t nrResPerTh : {4, 4000}) {
    for (size_t nrResPerTh : {4000}) {
        for (size_t nrTh : {32}) {
            //for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
            if (nrTh > 2 && nrTh % 2 != 0) continue;
            for (size_t i = 0; i < 10; i++) {
                bool verbose = false;
                runExec(nrResPerTh * nrTh, nrTh, 10, verbose, 0, 4, 2);
                //sleepMs(1000);
            }
        }
    }
#endif
#if 0
    // high-contention expr.
    for (size_t nrMutex : {40}) {
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
    runExec(5000, 8, 10, true);
#endif
#if 0
    // starvation expr.
    //const size_t nrMutex = 400 * 1000 * 1000;
    //const size_t nrTh = 16;
    const size_t nrMutex = 40 * 1000;
    const size_t nrTh = 8;
    //for (size_t longTxPct : {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60}) {
    for (size_t longTxPml : {1, 2, 3, 4, 5, 6, 7, 8, 9,
                10, 20, 30, 40, 50, 60, 70, 80, 90,
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}) {
        const size_t longTxSize = longTxPml * nrMutex / 1000;
        for (size_t i = 0; i < 10; i++) {
            bool verbose = false;
            //size_t maxSec = longTxPml >= 50 ? 50000 : 100;
            size_t maxSec = 100;
            runExec(nrMutex, nrTh, maxSec, verbose, longTxSize);
            sleepMs(1000);
        }
    }
#endif
}

struct CmdLineOptionPlus : CmdLineOption
{
    using base = CmdLineOption;

    int txIdGenType;

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendOpt(&txIdGenType, 0, "txid-gen", "[id]: txid gen method (0:sclable, 1:bulk, 2:simple)");
    }
    std::string str() const {
        return cybozu::util::formatString("mode:wait-die ") +
            base::str() +
            cybozu::util::formatString(" txidGenType:%d", txIdGenType);
    }
};

void dispatch1(CmdLineOptionPlus& opt, Shared& shared)
{
    switch (opt.txIdGenType) {
    case SCALABLE_TXID_GEN:
        runExec(opt, shared, worker2<SCALABLE_TXID_GEN>);
        break;
    case BULK_TXID_GEN:
        runExec(opt, shared, worker2<BULK_TXID_GEN>);
        break;
    case SIMPLE_TXID_GEN:
        runExec(opt, shared, worker2<SIMPLE_TXID_GEN>);
        break;
    default:
        throw cybozu::Exception("bad txIdGenType") << opt.txIdGenType;
    }
}

int main(int argc, char *argv[]) try
{
    CmdLineOptionPlus opt("wait_die_bench: benchmark with wait-die lock.");
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
