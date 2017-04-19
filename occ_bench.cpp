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

    std::vector<Mutex*> writeSet;
    std::vector<cybozu::occ::OccReader> readSet;
    std::vector<cybozu::occ::OccLock> lockV;

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
            if (shortTxMode == USE_MIX_TX) {
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

            for (size_t i = 0; i < muIdV.size(); i++) {
                const bool isWrite = bool(getMode(i));

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
                const Mutex *p = reinterpret_cast<Mutex*>(r.getMutexId());
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

    cybozu::occ::LockSet lockSet;

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
    while (!quit) {
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
            if (shortTxMode == USE_MIX_TX) {
                isWriteV.resize(nrOp);
                fillModeVec(isWriteV, rand, nrWr, tmpV2);
            }
        }

        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            // Try to run transaction.
            assert(lockSet.empty());

            for (size_t i = 0; i < muIdV.size(); i++) {
                const bool isWrite = bool(getMode(i));

                Mutex& mutex = muV[muIdV[i]];
                bool ret = lockSet.read(mutex);
                unused(ret); assert(ret);
                if (isWrite) {
                    ret = lockSet.write(mutex);
                    assert(ret);
                }
            }

            // commit phase.
            lockSet.lock();
            if (!lockSet.verify()) {
                lockSet.clear();
                res.incAbort(isLongTx);
                continue;
            }
            lockSet.updateAndUnlock();
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

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
    }
    std::string str() const {
        return cybozu::util::formatString("mode:silo-occ ") + base::str();
    }
};


int main(int argc, char *argv[]) try
{
    CmdLineOptionPlus opt("occ_bench: benchmark with silo-occ.");
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
