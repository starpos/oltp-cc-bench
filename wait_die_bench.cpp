#include "thread_util.hpp"
#include "random.hpp"
#include <unistd.h>
#include "cpuid.hpp"
#include "measure_util.hpp"
#include "arch.hpp"
#include "vector_payload.hpp"
#include "zipf.hpp"
#include "workload_util.hpp"

#include "wait_die.hpp"
#include "tx_util.hpp"

#ifdef USE_PARTITION
#include "partitioned.hpp"
#endif


using Lock = cybozu::wait_die::LockSet::Lock;
using Mutex = Lock::Mutex;
using Mode = Lock::Mode;

std::vector<uint> CpuId_;

EpochGenerator epochGen_;


struct Shared
{
#ifdef USE_PARTITION
    PartitionedVectorWithPayload<Mutex> recV;
#else
    VectorWithPayload<Mutex> recV;
#endif
    size_t longTxSize;
    size_t nrOp;
    double wrRatio;
    size_t nrWr4Long;
    TxMode shortTxMode;
    TxMode longTxMode;
    bool usesBackOff;
    size_t writePct;
    bool usesRMW;
    size_t nrTh4LongTx;
    size_t payload;
    bool usesZipf;
    double zipfTheta;
    double zipfZetan;

    GlobalTxIdGenerator globalTxIdGen;
    SimpleTxIdGenerator simpleTxIdGen;

    Shared() : globalTxIdGen(5, 10) {}
    //Shared() : globalTxIdGen(7, 5) {}
};


template <int txIdGenType>
Result1 worker2(size_t idx, uint8_t& ready, const bool& start, const bool& quit, bool& shouldQuit, Shared& shared)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    auto& recV = shared.recV;
#ifdef USE_PARTITION
    recV.allocate(idx);
    recV.checkAndWait();
#endif
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t wrRatio = size_t(shared.wrRatio * (double)SIZE_MAX);
    const TxMode shortTxMode = shared.shortTxMode;
    const TxMode longTxMode = shared.longTxMode;

    Result1 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0), idx);
    FastZipf fastZipf(rand, shared.zipfTheta, recV.size(), shared.zipfZetan);

    cybozu::wait_die::LockSet lockSet;

    std::vector<uint8_t> value(shared.payload);

    PriorityIdGenerator<12> priIdGen;
    priIdGen.init(idx + 1);
    TxIdGenerator localTxIdGen(&shared.globalTxIdGen);
    EpochTxIdGenerator<9, 2> epochTxIdGen(idx + 1, epochGen_);
    const bool isLongTx = longTxSize != 0 && idx < shared.nrTh4LongTx; // starvation setting.
    const size_t realNrOp = isLongTx ? longTxSize : nrOp;
    const size_t realNrWr = isLongTx ? shared.nrWr4Long : size_t(shared.wrRatio * (double)nrOp);
    auto getMode = selectGetModeFunc<decltype(rand), Mode>(isLongTx, shortTxMode, longTxMode);
    auto getRecordIdx = selectGetRecordIdx<decltype(rand)>(isLongTx, shortTxMode, longTxMode, shared.usesZipf);

    lockSet.init(shared.payload, realNrOp);

    store_release(ready, 1);
    while (!load_acquire(start)) _mm_pause();
    size_t count = 0; unused(count);
    while (likely(!load_acquire(quit))) {
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
        uint64_t t0 = -1, t1 = -1, t2 = -1; // -1 for debug.
        log_timestamp_if_necessary_on_tx_start(t0, shared.usesBackOff);
        auto randState = rand.getState();
        for (size_t retry = 0;; retry++) {
            if (unlikely(load_acquire(quit))) break; // to quit under starvation.
            assert(lockSet.empty());
            rand.setState(randState);
            log_timestamp_if_necessary_on_trial_start(t0, t1, t2, retry, shared.usesBackOff);
            for (size_t i = 0; i < realNrOp; i++) {
                size_t key = getRecordIdx(rand, fastZipf, recV.size(), realNrOp, i, firstRecIdx);
                Mode mode = getMode(rand, realNrOp, realNrWr, wrRatio, i);

                auto& item = recV[key];
                Mutex& mutex = item.value;
                if (mode == Mode::S) {
                    if (unlikely(!lockSet.read(mutex, item.payload, &value[0]))) goto abort;
                } else {
                    assert(mode == Mode::X);
                    if (shared.usesRMW) {
                        if (unlikely(!lockSet.readForUpdate(mutex, item.payload, &value[0]))) goto abort;
                        if (unlikely(!lockSet.write(mutex, item.payload, &value[0]))) goto abort;
                    } else {
                        if (unlikely(!lockSet.write(mutex, item.payload, &value[0]))) goto abort;
                    }
                }
            }
            if (unlikely(!lockSet.blindWriteLockAll())) goto abort;
            lockSet.updateAndUnlock();
            log_timestamp_if_necessary_on_commit(res, t0, t1, t2);
            res.incCommit(isLongTx);
            res.addRetryCount(isLongTx, retry);
            break; // retry is not required.

          abort:
            lockSet.unlock();
            log_timestamp_if_necessary_on_abort(res, t1, t2);
            res.incAbort(isLongTx);
            if (shared.usesBackOff) backOff(t1, retry, rand);
            // continue
        }
    }
    return res;
}


/**
 * Long transactions with several transaction sizes.
 */
Result2 worker3(size_t idx, uint8_t& ready, const bool& start, const bool& quit, bool& shouldQuit, Shared& shared)
{
    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    auto& recV = shared.recV;
#ifdef USE_PARTITION
    recV.allocate(idx);
    recV.checkAndWait();
#endif

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
    cybozu::util::Xoroshiro128Plus rand(::time(0), idx);
    cybozu::wait_die::LockSet lockSet;
    lockSet.init(shared.payload, txSize);
    std::vector<uint8_t> value(shared.payload);

#if 0
    TxIdGenerator localTxIdGen(&shared.globalTxIdGen);
#else
    EpochTxIdGenerator<9, 2> epochTxIdGen(idx + 1, epochGen_);
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

    store_release(ready, 1);
    while (!load_acquire(start)) _mm_pause();
    size_t count = 0; unused(count);
    while (likely(!load_acquire(quit))) {
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
            if (unlikely(load_acquire(quit))) break; // to quit under starvation.
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
                    if (unlikely(!lockSet.read(mutex, item.payload, &value[0]))) goto abort;
                } else {
                    assert(mode == Mode::X);
                    if (shared.usesRMW) {
                        if (unlikely(!lockSet.readForUpdate(mutex, item.payload, &value[0]))) goto abort;
                        if (unlikely(!lockSet.write(mutex, item.payload, &value[0]))) goto abort;
                    } else {
                        if (unlikely(!lockSet.write(mutex, item.payload, &value[0]))) goto abort;
                    }
                }
            }
            if (unlikely(!lockSet.blindWriteLockAll())) goto abort;
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
        initRecordVector(shared.recV, opt);
        shared.longTxSize = opt.longTxSize;
        shared.nrOp = opt.nrOp;
        shared.wrRatio = opt.wrRatio;
        shared.nrWr4Long = opt.nrWr4Long;
        shared.shortTxMode = TxMode(opt.shortTxMode);
        shared.longTxMode = TxMode(opt.longTxMode);
        shared.usesBackOff = opt.usesBackOff ? 1 : 0;
        shared.nrTh4LongTx = opt.nrTh4LongTx;
        shared.usesRMW = opt.usesRMW != 0;
        shared.payload = opt.payload;
        shared.usesZipf = opt.usesZipf;
        shared.zipfTheta = opt.zipfTheta;
        if (shared.usesZipf) {
            shared.zipfZetan = FastZipf::zeta(opt.getNrMu(), shared.zipfTheta);
        } else {
            shared.zipfZetan = 1.0;
        }
        for (size_t i = 0; i < opt.nrLoop; i++) {
            dispatch1(opt, shared);
            epochGen_.reset();
        }
    } else if (opt.workload == "custom3") {
        Shared shared;
        initRecordVector(shared.recV, opt);
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
} catch (std::exception& e) {
    ::fprintf(::stderr, "exeption: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
