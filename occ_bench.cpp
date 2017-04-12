#include <ctime>
#include <vector>
#include <chrono>
#include <unistd.h>
#include "occ.hpp"
#include "thread_util.hpp"
#include "random.hpp"
#include "measure_util.hpp"
#include "cpuid.hpp"


using Mutex = cybozu::occ::OccLock::Mutex;

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
            "workload:%s longTxSize:%zu nrMutex:%zu nrMutexPerTh:%zu "
            "shortMode:%d"
            , workload.c_str()
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

    std::vector<Mutex*> writeSet;
    std::vector<cybozu::occ::OccReader> readSet;
    std::vector<cybozu::occ::OccLock> lockV;

    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    // USE_MIX_TX
    std::vector<bool> isWriteV(nrOp);
    std::vector<size_t> tmpV2; // for fillModeVec.

    // USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);

    while (!start) _mm_pause();
    while (!quit) {
        //const bool isLongTx = rand() % 1000 == 0; // 0.1% long transaction.
        const bool isLongTx = longTxSize != 0 && idx == 0; // starvation setting.
        if (isLongTx) {
            muIdV.resize(longTxSize);
            if (longTxSize > muV.size() * 5 / 1000) {
                fillMuIdVecArray(muIdV, rand, muV.size(), tmpV);
            } else {
                fillMuIdVecLoop(muIdV, rand, muV.size());
            }
        } else {
            muIdV.resize(nrOp);
            fillMuIdVecLoop(muIdV, rand, muV.size());
            if (shortMode == USE_MIX_TX) {
                isWriteV.resize(nrOp);
                fillModeVec(isWriteV, rand, nrWr, tmpV2);
            }
        }

        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            // Try to run transaction.
            bool abort = false;
            assert(writeSet.empty());
            assert(readSet.empty());
            assert(lockV.empty());

            const size_t sz = muIdV.size();
            unused(sz);
            for (size_t i = 0; i < muIdV.size(); i++) {
                bool isWrite;
                if (shortMode == USE_LONG_TX_2) {
                    isWrite = boolRand();
                } else if (shortMode == USE_READONLY_TX) {
                    isWrite = false;
                } else if (shortMode == USE_WRITEONLY_TX) {
                    isWrite = true;
                } else if (shortMode == USE_MIX_TX) {
                    isWrite = isWriteV[i];
                } else {
                    isWrite = (i == sz - 1 || i == sz - 2);
                }

                // read
                for (;;) {
                    cybozu::occ::OccReader r;
                    r.prepare(&muV[muIdV[i]]);
                    // should read resource here.
                    r.readFence();
                    if (r.verifyAll()) {
                        readSet.push_back(std::move(r));
                        break;
                    }
                }
                if (isWrite) {
                    // write
                    writeSet.push_back(&muV[muIdV[i]]);
                }
            }
#if 0
            // delay
            if (isLongTx) ::usleep(1);
#endif

            // commit phase.
            std::sort(writeSet.begin(), writeSet.end());
            for (Mutex* mutex : writeSet) {
                lockV.emplace_back(mutex);
            }
            __atomic_thread_fence(__ATOMIC_ACQ_REL);
            for (cybozu::occ::OccReader& r : readSet) {
                const Mutex *p = reinterpret_cast<Mutex*>(r.getId());
                const bool ret =
                    std::binary_search(writeSet.begin(), writeSet.end(), p)
                    ? r.verifyVersion() : r.verifyAll();
                if (!ret) {
                    abort = true;
                    break;
                }
            }
            if (abort) {
                lockV.clear();
                writeSet.clear();
                readSet.clear();
                res.incAbort(isLongTx);
                continue;
            }
            // We can commit.
            for (cybozu::occ::OccLock& lk : lockV) {
                // should write resource here.
                lk.update();
                lk.writeFence();
                lk.unlock();
            }
            lockV.clear();
            writeSet.clear();
            readSet.clear();
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
    for (size_t nrResPerTh : {4000}) {
    //for (size_t nrResPerTh : {4, 4000}) {
        //for (size_t nrTh : {32}) {
        for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
            if (nrTh > 2 && nrTh % 2 != 0) continue;
            for(size_t i = 0; i < 10; i++) {
                bool verbose = false;
                runExec(nrResPerTh * nrTh, nrTh, 10, verbose, 0, 4, 0);
                //sleepMs(1000);
            }
        }
    }
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
    for (size_t longTxPml : {1, 2, 3, 4, 5, 6, 7, 8, 9,
                10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                20, 30, 40, 50, 60, 70, 80, 90,
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}) {
        const size_t longTxSize = longTxPml * nrMutex / 1000;
        for (size_t i = 0; i < 10; i++) {
            bool verbose = false;
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
    CmdLineOptionPlus opt("occ_bench: benchmark with silo-occ.");
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
