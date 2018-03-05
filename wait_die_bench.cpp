#include "thread_util.hpp"
#include "random.hpp"
#include <unistd.h>
#include "cpuid.hpp"
#include "measure_util.hpp"
#include "arch.hpp"
#include "vector_payload.hpp"

#include "wait_die.hpp"
#include "tx_util.hpp"

using Lock = cybozu::wait_die::WaitDieLock;
using Mutex = Lock::Mutex;
using Mode = Lock::Mode;

std::vector<uint> CpuId_;

EpochGenerator epochGen_;


struct Shared
{
    VectorWithPayload<Mutex> recV;
    size_t longTxSize;
    size_t nrOp;
    size_t nrWr;
    size_t nrWr4Long;
    int shortTxMode;
    int longTxMode;
    bool usesBackOff;
    size_t writePct;
    bool usesRMW;
    size_t nrTh4LongTx;
    size_t payload;

    GlobalTxIdGenerator globalTxIdGen;
    SimpleTxIdGenerator simpleTxIdGen;

    Shared() : globalTxIdGen(5, 10) {}
    //Shared() : globalTxIdGen(7, 5) {}
};


#if 0
std::atomic<size_t> g_cas_success(0);
std::atomic<size_t> g_cas_total(0);
std::atomic<size_t> g_retry_total(0);
std::atomic<size_t> g_tx_success(0);
std::atomic<size_t> g_ts_total(0);
#endif


template <int txIdGenType>
Result1 worker2(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, Shared& shared)
{
#if 0
    cybozu::wait_die::cas_success = 0;
    cybozu::wait_die::cas_total = 0;
#endif

    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    VectorWithPayload<Mutex>& recV = shared.recV;
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t nrWr = shared.nrWr;
    const int shortTxMode = shared.shortTxMode;
    const int longTxMode = shared.longTxMode;

    Result1 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    cybozu::wait_die::LockSet lockSet;

    std::vector<uint8_t> value(shared.payload);
    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    PriorityIdGenerator<12> priIdGen;
    priIdGen.init(idx + 1);
    TxIdGenerator localTxIdGen(&shared.globalTxIdGen);
    EpochTxIdGenerator<8, 2> epochTxIdGen(idx + 1, epochGen_);

    // USE_MIX_TX
    std::vector<bool> isWriteV(nrOp);
    std::vector<size_t> tmpV2; // for fillModeVec.

    // USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);


    const bool isLongTx = longTxSize != 0 && idx < shared.nrTh4LongTx; // starvation setting.
    const size_t realNrOp = isLongTx ? longTxSize : nrOp;
    const size_t realNrWr = isLongTx ? shared.nrWr4Long : nrWr;
    if (!isLongTx && shortTxMode == USE_MIX_TX) {
        isWriteV.resize(nrOp);
    }
    lockSet.init(shared.payload, realNrOp);

#if 0
    GetModeFunc<decltype(rand), Mode>
        getMode(boolRand, isWriteV, isLongTx,
                shortTxMode, longTxMode, realNrOp, nrWr);
#endif

#if 0
    uint64_t ts_total = 0;
    uint64_t nr_success = 0;
    uint64_t retry_total = 0;
#endif

    while (!start) _mm_pause();
    size_t count = 0; unused(count);
    while (!quit) {
        if (!isLongTx && shortTxMode == USE_MIX_TX) {
            fillModeVec(isWriteV, rand, nrWr, tmpV2);
        }

        uint64_t txId;
        if (txIdGenType == SCALABLE_TXID_GEN) {
            txId = priIdGen.get(isLongTx ? 0 : 1);
        } else if (txIdGenType == BULK_TXID_GEN) {
            txId = localTxIdGen.get();
        } else if (txIdGenType == SIMPLE_TXID_GEN) {
            txId = shared.simpleTxIdGen.get();
        } else if (txIdGenType == EPOCH_TXID_GEN) {
            txId = epochTxIdGen.get();
        } else {
            throw cybozu::Exception("bad txIdGenType") << txIdGenType;
        }
        lockSet.setTxId(txId);
        size_t firstRecIdx;
        uint64_t t0;
        if (shared.usesBackOff) t0 = cybozu::time::rdtscp();
#if 0
        uint64_t ts[3];
        ts[0] = cybozu::time::rdtscp();
#endif
        auto randState = rand.getState();
        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            assert(lockSet.empty());
            rand.setState(randState);
#if 0
            ts[1] = cybozu::time::rdtscp();
#endif
            for (size_t i = 0; i < realNrOp; i++) {
#if 0
                Mode mode = getMode(i);
#else
                Mode mode = getMode<decltype(rand), Mode>(
                    rand, boolRand, isWriteV, isLongTx, shortTxMode, longTxMode,
                    realNrOp, realNrWr, i);
#endif
                size_t key = getRecordIdx(
                    rand, isLongTx, shortTxMode, longTxMode,
                    recV.size(), realNrOp, i, firstRecIdx);
                auto& item = recV[key];
                Mutex& mutex = item.value;
                if (mode == Mode::S) {
                    if (!lockSet.read(mutex, item.payload, &value[0])) goto abort;
                } else {
                    assert(mode == Mode::X);
                    if (shared.usesRMW) {
                        if (!lockSet.readForUpdate(mutex, item.payload, &value[0])) goto abort;
                        if (!lockSet.write(mutex, item.payload, &value[0])) goto abort;
                    } else {
                        if (!lockSet.write(mutex, item.payload, &value[0])) goto abort;
                    }
                }
            }
            if (!lockSet.blindWriteLockAll()) goto abort;
            lockSet.updateAndUnlock();
            res.incCommit(isLongTx);
            res.addRetryCount(isLongTx, retry);
#if 0
            ts[2] = cybozu::time::rdtscp();
            ts_total += ts[2] - ts[0];
            nr_success++;
            retry_total += retry;
#endif
            //::printf("last trial latency %" PRIu64 " total latency %" PRIu64 "\n", ts[2] - ts[1], ts[2] - ts[0]);
            break; // retry is not required.

          abort:
            lockSet.unlock();
            res.incAbort(isLongTx);
            if (shared.usesBackOff) backOff(t0, retry, rand);
            // continue
        }
    }

#if 0
    g_cas_success += cybozu::wait_die::cas_success;
    g_cas_total += cybozu::wait_die::cas_total;
    g_retry_total += retry_total;
    g_tx_success += nr_success;
    g_ts_total += ts_total;
#endif
    //::printf("average latency %f\n", (double)ts_total / (double)nr_success);
    //::printf("average retry %f\n", (double)retry_total / (double)nr_success);

    return res;
}


/**
 * Long transactions with several transaction sizes.
 */
Result2 worker3(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, Shared& shared)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    VectorWithPayload<Mutex>& recV = shared.recV;

    const size_t txSize = [&]() -> size_t {
        if (idx == 0) {
            return std::max<size_t>(recV.size() / 2, 10);
        } else if (idx <= 5) {
            return std::max<size_t>(recV.size() / 10, 10);
        } else {
            return 10;
        }
    }();

    Result2 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    cybozu::wait_die::LockSet lockSet;
    lockSet.init(shared.payload, txSize);
    std::vector<uint8_t> value(shared.payload);

#if 0
    TxIdGenerator localTxIdGen(&shared.globalTxIdGen);
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

    BoolRandom<decltype(rand)> boolRand(rand);

    const size_t realNrOp = txSize;

    while (!start) _mm_pause();
    size_t count = 0; unused(count);
    while (!quit) {
#if 0
        const uint64_t txId = localTxIdGen.get();
#else
        const uint64_t txId = epochTxIdGen.get();
#endif
        lockSet.setTxId(txId);
        uint64_t t0;
        if (shared.usesBackOff) t0 = cybozu::time::rdtscp();
        auto randState = rand.getState();
        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            assert(lockSet.empty());
            rand.setState(randState);
            boolRand.reset();
            for (size_t i = 0; i < realNrOp; i++) {
#if 0
                const Mode mode = boolRand() ? Mode::X : Mode::S;
#else
                const Mode mode = rand() % 100 < shared.writePct ? Mode::X : Mode::S;
#endif
                const size_t key = rand() % recV.size();
                auto& item = recV[key];
                Mutex& mutex = item.value;
                if (mode == Mode::S) {
                    if (!lockSet.read(mutex, item.payload, &value[0])) goto abort;
                } else {
                    assert(mode == Mode::X);
                    if (shared.usesRMW) {
                        if (!lockSet.readForUpdate(mutex, item.payload, &value[0])) goto abort;
                        if (!lockSet.write(mutex, item.payload, &value[0])) goto abort;
                    } else {
                        if (!lockSet.write(mutex, item.payload, &value[0])) goto abort;
                    }
                }
            }
            if (!lockSet.blindWriteLockAll()) goto abort;
            lockSet.updateAndUnlock();
            res.incCommit(txSize);
            res.addRetryCount(txSize, retry);
            break; // retry is not required.

          abort:
            lockSet.unlock();
            res.incAbort(txSize);
            if (shared.usesBackOff) backOff(t0, retry, rand);
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
    int usesBackOff; // 0 or 1.
    size_t writePct;
    int usesRMW; // 0 or 1.

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendOpt(&txIdGenType, 3, "txid-gen", "[id]: txid gen method (0:sclable, 1:bulk, 2:simple, 3:epoch(default))");
        appendOpt(&usesBackOff, 0, "backoff", "[0 or 1]: backoff 0:off 1:on");
        appendOpt(&usesRMW, 1, "rmw", "[0 or 1]: use read-modify-write or normal write 0:w 1:rmw (default: 1)");
        appendOpt(&writePct, 50, "writepct", "[pct]: write percentage (0 to 100) for custom3 workload.");
    }
    std::string str() const {
        return cybozu::util::formatString(
            "mode:wait-die %s txidGenType:%d backoff:%d writePct:%zu rmw:%d"
            , base::str().c_str(), txIdGenType, usesBackOff ? 1 : 0
            , writePct, usesRMW ? 1 : 0);
    }
};


void dispatch1(CmdLineOptionPlus& opt, Shared& shared)
{
    Result1 res;
    switch (opt.txIdGenType) {
    case SCALABLE_TXID_GEN:
        runExec(opt, shared, worker2<SCALABLE_TXID_GEN>, res);
        break;
    case BULK_TXID_GEN:
        runExec(opt, shared, worker2<BULK_TXID_GEN>, res);
        break;
    case SIMPLE_TXID_GEN:
        runExec(opt, shared, worker2<SIMPLE_TXID_GEN>, res);
        break;
    case EPOCH_TXID_GEN:
	runExec(opt, shared, worker2<EPOCH_TXID_GEN>, res);
	break;
    default:
        throw cybozu::Exception("bad txIdGenType") << opt.txIdGenType;
    }
}


int main(int argc, char *argv[]) try
{
    CmdLineOptionPlus opt("wait_die_bench: benchmark with wait-die lock.");
    opt.parse(argc, argv);
    setCpuAffinityModeVec(opt.amode, CpuId_);

#ifdef NO_PAYLOAD
    if (opt.payload != 0) throw cybozu::Exception("payload not supported");
#endif

    if (opt.workload == "custom") {
        Shared shared;
#ifdef MUTEX_ON_CACHELINE
        shared.recV.setPayloadSize(opt.payload, CACHE_LINE_SIZE);
#else
        shared.recV.setPayloadSize(opt.payload);
#endif
        shared.recV.resize(opt.getNrMu());
        shared.longTxSize = opt.longTxSize;
        shared.nrOp = opt.nrOp;
        shared.nrWr = opt.nrWr;
        shared.nrWr4Long = opt.nrWr4Long;
        shared.shortTxMode = opt.shortTxMode;
        shared.longTxMode = opt.longTxMode;
        shared.usesBackOff = opt.usesBackOff ? 1 : 0;
        shared.nrTh4LongTx = opt.nrTh4LongTx;
        shared.usesRMW = opt.usesRMW != 0;
        shared.payload = opt.payload;
        for (size_t i = 0; i < opt.nrLoop; i++) {
            dispatch1(opt, shared);
            epochGen_.reset();
        }
    } else if (opt.workload == "custom3") {
        Shared shared;
#ifdef MUTEX_ON_CACHELINE
        shared.recV.setPayloadSize(opt.payload, CACHE_LINE_SIZE);
#else
        shared.recV.setPayloadSize(opt.payload);
#endif
        shared.recV.resize(opt.getNrMu());
        shared.usesBackOff = opt.usesBackOff ? 1 : 0;
        shared.writePct = opt.writePct;
        shared.usesRMW = opt.usesRMW != 0;
        shared.payload = opt.payload;
        for (size_t i = 0; i < opt.nrLoop; i++) {
            Result2 res;
            runExec(opt, shared, worker3, res);
            epochGen_.reset();
        }
    } else {
        throw cybozu::Exception("bad workload.") << opt.workload;
    }

#if 0
    ::printf("CAS success %zu total %zu  rate %f\n"
             , g_cas_success.load(), g_cas_total.load()
             , (double)(g_cas_success.load()) / (double)g_cas_total.load());
    ::printf("average retry %f\n", (double)g_retry_total.load() / (double)g_tx_success.load());
    ::printf("average latency %f\n", (double)g_ts_total.load() / (double)g_tx_success.load());
#endif

} catch (std::exception& e) {
    ::fprintf(::stderr, "exeption: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
