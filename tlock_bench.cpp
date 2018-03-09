#include <unordered_map>
#include <iostream>
#include <sstream>
#include <unistd.h>
#include "thread_util.hpp"
#include "random.hpp"
#include "tx_util.hpp"
#include "measure_util.hpp"
#include "time.hpp"
#include "cpuid.hpp"
#include "lock.hpp"
#include "trlock.hpp"
#include "cmdline_option.hpp"
#include "arch.hpp"


using Spinlock = cybozu::lock::TtasSpinlockT<0>;

struct LockInfo
{
    size_t muId;
    bool mode; // 0:read, 1:write.
    uint state; // 0: wait, 1: tlocked, 2: nlocked
};

struct TxInfo
{
    Spinlock::Mutex mutex;
    size_t thId; // thread id.
    uint32_t txId; // transaction id.
    std::vector<LockInfo> lockInfo;

    std::string str() const {
        std::string s = cybozu::util::formatString(
            "%03zu txId %u\n", thId, txId);
        for (size_t i = 0; i < lockInfo.size(); i++) {
            s += cybozu::util::formatString(
                "  muId %zu  mode %c  state %u\n"
                , lockInfo[i].muId
                , lockInfo[i].mode ? 'S' : 'W'
                , lockInfo[i].state);
        }
        return s;
    }
};


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

//using PQLock = cybozu::lock::PQNoneLock;
//using PQLock = cybozu::lock::PQSpinLock;
//using PQLock = cybozu::lock::PQMcsLock;
//using PQLock = cybozu::lock::PQMcsLock2;
//using PQLock = cybozu::lock::PQPosixLock;
//using PQLock = cybozu::lock::PQ1993Lock;
//using PQLock = cybozu::lock::PQ1997Lock;
//using PQLock = cybozu::lock::PQMcsLock3;


template <typename PQLock>
struct TLockTypes
{
    //using LockData = cybozu::lock::LockDataMG;
    using LockData = cybozu::lock::LockDataXS;
    using TLock = cybozu::lock::TransferableLockT<PQLock, LockData>;
    using Mutex = typename TLock::Mutex;
    using Mode = typename TLock::Mode;
    using TLockReader = cybozu::lock::TransferableLockReaderT<PQLock, LockData>;
};

template <typename PQLock>
struct ILockTypes
{
    using ILockData = cybozu::lock::LockData64;
    using ILock = cybozu::lock::InterceptibleLock64T<PQLock>;
    using IMutex = typename ILock::Mutex;
    using IMode = typename ILock::Mode;
    using ILockReader = cybozu::lock::InterceptibleLock64ReaderT<PQLock>;
};


// #define MONITOR
#undef MONITOR


enum class ReadMode : uint8_t { LOCK, OCC, HYBRID };

const char* readModeToStr(ReadMode rmode)
{
    switch (rmode) {
    case ReadMode::LOCK:
        return "trlock";
    case ReadMode::OCC:
        return "trlock-occ";
    case ReadMode::HYBRID:
        return "trlock-hybrid";
    }
    return nullptr;
}

ReadMode strToReadMode(const char *s)
{
    const char *hybrid = "trlock-hybrid";
    const char *occ = "trlock-occ";
    const char *lock = "trlock";

    if (::strncmp(hybrid, s, ::strnlen(hybrid, 20)) == 0) {
        return ReadMode::HYBRID;
    } else if (::strncmp(occ, s, ::strnlen(occ, 20)) == 0) {
        return ReadMode::OCC;
    } else if (::strncmp(lock, s, ::strnlen(lock, 20)) == 0) {
        return ReadMode::LOCK;
    } else {
        throw cybozu::Exception("strToReadMode: bad string") << s;
    }
}


void testLockStateXS()
{
    using LockState = cybozu::lock::LockStateXS;
    using Mode = LockState::Mode;

    LockState s;

    assert(s.isUnlocked());
    assert(s.canSet(Mode::X));
    assert(s.canSet(Mode::S));
    assert(!s.canClear(Mode::X));
    assert(!s.canClear(Mode::S));

    s.set(Mode::X);
    assert(!s.canSet(Mode::X));
    assert(s.canClear(Mode::X));
    assert(!s.canSet(Mode::S));
    assert(!s.canClear(Mode::S));
    s.clear(Mode::X);
    assert(s.isUnlocked());

    s.set(Mode::S);
    assert(!s.canSet(Mode::X));
    assert(!s.canClear(Mode::X));
    assert(s.canSet(Mode::S));
    assert(s.canClear(Mode::S));
    s.clear(Mode::S);
    assert(s.isUnlocked());

    for (size_t i = 0; i < 0x7F; i++) {
        assert(s.canSet(Mode::S));
        s.set(Mode::S);
        assert(!s.canSet(Mode::X));
        assert(!s.canClear(Mode::X));
    }
    assert(!s.canSet(Mode::S));
    for (size_t i = 0; i < 0x7F; i++) {
        assert(!s.canSet(Mode::X));
        assert(s.canClear(Mode::S));
        s.clear(Mode::S);
    }
    assert(s.isUnlocked());
}

void testLockStateMG()
{
    using LockState = cybozu::lock::LockStateMG;
    using Mode = LockState::Mode;

    LockState s;

    s.set(Mode::X);
    assert(!s.canSet(Mode::X));
    assert(!s.canSet(Mode::S));
    assert(!s.canSet(Mode::IX));
    assert(!s.canSet(Mode::IS));
    assert(!s.canSet(Mode::SIX));
    s.clear(Mode::X);
    assert(s.isUnlocked());

    s.set(Mode::S);
    assert(!s.canSet(Mode::X));
    assert(s.canSet(Mode::S));
    assert(!s.canSet(Mode::IX));
    assert(s.canSet(Mode::IS));
    assert(!s.canSet(Mode::SIX));
    s.clear(Mode::S);
    assert(s.isUnlocked());

    s.set(Mode::IX);
    assert(!s.canSet(Mode::X));
    assert(!s.canSet(Mode::S));
    assert(s.canSet(Mode::IX));
    assert(s.canSet(Mode::IS));
    assert(!s.canSet(Mode::SIX));
    s.clear(Mode::IX);
    assert(s.isUnlocked());

    s.set(Mode::IS);
    assert(!s.canSet(Mode::X));
    assert(s.canSet(Mode::S));
    assert(s.canSet(Mode::IX));
    assert(s.canSet(Mode::IS));
    assert(s.canSet(Mode::SIX));
    s.clear(Mode::IS);
    assert(s.isUnlocked());

    s.set(Mode::SIX);
    assert(!s.canSet(Mode::X));
    assert(!s.canSet(Mode::S));
    assert(!s.canSet(Mode::IX));
    assert(s.canSet(Mode::IS));
    assert(!s.canSet(Mode::SIX));
    s.clear(Mode::SIX);
    assert(s.isUnlocked());

    for (size_t i = 0; i < 10; i++) {
        assert(s.canSet(Mode::S));
        s.set(Mode::S);
    }
    assert(!s.canSet(Mode::X));
    for (size_t i = 0; i < 10; i++) {
        s.clear(Mode::S);
    }
    assert(s.isUnlocked());
}



void testRandom()
{
    std::vector<size_t> muIdV;
    cybozu::util::XorShift128 rand(::time(0));

    size_t nr = 1000;
    muIdV.resize(4000);

    cybozu::time::TimeStack<> ts;
    ts.pushNow();
    for (size_t i = 0; i < nr; i++) {
        fillMuIdVecHash(muIdV, rand, 40000);
    }
    ts.pushNow();
    ::printf("%.03f op/sec\n", nr / (ts.elapsedInUs() / (double)1000000));
}


const std::vector<uint> CpuId_ = getCpuIdList(CpuAffinityMode::CORE);


template <typename Lock, typename LockReader>
void clearLocks(
    std::vector<Lock>& writeLocks,
    std::vector<Lock>& readLocks,
    std::vector<uintptr_t>& writeSet,
    std::vector<LockReader>& readSet)
{
    writeLocks.clear();
    readLocks.clear();
    writeSet.clear();
    readSet.clear();
}


template <typename PQLock>
struct TLockShared
{
    using Mutex = typename TLockTypes<PQLock>::Mutex;

    std::vector<Mutex> muV;
    ReadMode rmode;
    TxInfo txInfo;
    size_t longTxSize;
    size_t nrOp;
    size_t nrWr;
    size_t nrWr4Long;
    int shortTxMode;
    int longTxMode;

    GlobalTxIdGenerator globalTxIdGen;
    SimpleTxIdGenerator simpleTxIdGen;

    TLockShared() : globalTxIdGen(5, 10) {}
};


template <int txIdGenType, typename PQLock>
Result1 tWorker(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, TLockShared<PQLock>& shared)
{
    using Mutex = typename TLockTypes<PQLock>::Mutex;
    using TLock = typename TLockTypes<PQLock>::TLock;
    using Mode = typename TLockTypes<PQLock>::Mode;
    using TLockReader = typename TLockTypes<PQLock>::TLockReader;

    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    std::vector<Mutex>& muV = shared.muV;
    const ReadMode rmode = shared.rmode;
    TxInfo& txInfo = shared.txInfo;
    unused(txInfo);
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t nrWr = shared.nrWr;
    const int shortTxMode = shared.shortTxMode;
    const int longTxMode = shared.longTxMode;

    PriorityIdGenerator<12> priIdGen;
    priIdGen.init(idx + 1);
    TxIdGenerator localTxIdGen(&shared.globalTxIdGen);

    Result1 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0), idx);
    std::vector<size_t> muIdV(nrOp);
    std::vector<TLock> writeLocks;
    std::vector<TLock> readLocks;
    std::vector<uintptr_t> writeSet;
    std::vector<TLockReader> readSet;

    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    // USE_MIX_TX
    std::vector<bool> isWriteV;
    std::vector<size_t> tmpV2; // for fillModeVec;

    // USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);

#ifdef MONITOR
    {
        Spinlock lk(&txInfo.mutex);
        txInfo.thId = i;
    }
#endif

    const bool isLongTx = longTxSize != 0 && idx == 0; // starvation setting.
    if (isLongTx) {
        muIdV.resize(longTxSize);
    } else {
        muIdV.resize(nrOp);
        if (shortTxMode == USE_MIX_TX) {
            isWriteV.resize(nrOp);
        }
    }

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
#if 0 /* For test. */
        std::sort(muIdV.begin(), muIdV.end());
#endif

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
            assert(writeLocks.empty());
            assert(readLocks.empty());
            assert(writeSet.empty());
            assert(readSet.empty());
#ifdef MONITOR
            {
                Spinlock lk(&txInfo.mutex);
                txInfo.txId = txId;
            }
#endif
            for (size_t i = 0; i < muIdV.size(); i++) {
                Mode mode = getMode<decltype(rand), Mode>(
                    rand, boolRand, isWriteV, isLongTx, shortTxMode, longTxMode,
                    nrOp, nrWr, i);
#ifdef MONITOR
                {
                    Spinlock lk(&txInfo.mutex);
                    txInfo.lockInfo.emplace_back();
                    LockInfo &lkInfo = txInfo.lockInfo.back();
                    lkInfo.mode = mode == Mode::S ? 0 : 1;
                    lkInfo.muId = muIdV[i];
                    lkInfo.state = 0;
                }
#endif
                Mutex *mutex = &muV[muIdV[i]];
                if (mode == Mode::S) {
                    const bool tryOccRead = rmode == ReadMode::OCC
                        || (rmode == ReadMode::HYBRID && !isLongTx && retry == 0);
                    if (tryOccRead) {
                        readSet.emplace_back();
                        TLockReader& r = readSet.back();
                        for (;;) {
                            r.prepare(mutex);
                            // read
                            r.readFence();
                            if (r.verifyAll()) {
#if 0
                                ::printf("READ %zu %s\n", r.getMutexId(), r.getLockData().str().c_str()); // QQQ
#endif
                                break;
                            }
                        }
                    } else {
                        readLocks.emplace_back(mutex, mode, txId);
                        TLock& lk = readLocks.back();
                        for (;;) {
                            // read
                            if (!lk.intercepted()) break;
                            // intercept failed.
                            lk.relock();
                        }
                    }
                } else {
                    assert(mode == Mode::X);
                    writeLocks.emplace_back(mutex, mode, txId);
                    TLock& lk = writeLocks.back();
                    // modify the resource on write-set.
                    writeSet.emplace_back(lk.getMutexId());
                }
#ifdef MONITOR
                {
                    Spinlock lk(&txInfo.mutex);
                    LockInfo &lkInfo = txInfo.lockInfo.back();
                    lkInfo.state = 1;
                }
#endif
            }

            // Pre-commit.
#if 0 // preemptive aborts
            for (TLockReader& r : readSet) {
                if (!r.verifyAll()) {
                    res.incAbort(isLongTx);
                    goto abort;
                }
            }
            for (size_t i = 0; i < readLocks.size(); i++) {
                if (readLocks[i].intercepted()){
                    res.incIntercepted(isLongTx);
                    goto abort;
                }
            }
            for (size_t i = 0; i < writeLocks.size(); i++) {
                if (writeLocks[i].intercepted()) {
                    res.incIntercepted(isLongTx);
                    goto abort;
                }
            }
#endif // preemptive aborts
            for (size_t i = 0; i < writeLocks.size(); i++) {
                if (!writeLocks[i].protect()) {
                    res.incIntercepted(isLongTx);
                    goto abort;
                }
#ifdef MONITOR
                {
                    Spinlock lk(&txInfo.mutex);
                    LockInfo &lkInfo = txInfo.lockInfo[i];
                    lkInfo.state = 2;
                }
#endif
            }
#if 0 // protect read locks.
            for (size_t i = 0; i < readLocks.size(); i++) {
                if (!readLocks[i].protect()) {
                    res.incIntercepted(isLongTx);
                    goto abort;
                }
            }
            // serialization points.
            __atomic_thread_fence(__ATOMIC_ACQUIRE);
#else // verify read locks.
            // serialization points.
            __atomic_thread_fence(__ATOMIC_ACQ_REL);
            for (size_t i = 0; i < readLocks.size(); i++) {
                if (!readLocks[i].unchanged()) {
                    res.incIntercepted(isLongTx);
                    goto abort;
                }
            }
#endif
            if (!readSet.empty() && !writeSet.empty()) {
                // for binary search in verify phase.
                std::sort(writeSet.begin(), writeSet.end());
            }

            for (TLockReader& r : readSet) {
                const bool ret =
                    (!writeSet.empty() && std::binary_search(writeSet.begin(), writeSet.end(), r.getMutexId()))
                    ? r.verifyVersion() : r.verifyAll();
                if (!ret) {
#if 0
                    ::printf("NG %zu %zu \nbefore %s\nafter  %s\n"
                             , idx, r.getMutexId()
                             , r.getLockData().str().c_str()
                             , lockD.str().c_str()); // debug
#endif
                    res.incAbort(isLongTx);
                    goto abort;
                } else {
#if 0
                    ::printf("OK %zu %zu \nbefore %s\nafter  %s\n"
                             , idx, r.getMutexId()
                             , r.getLockData().str().c_str()
                             , lockD.str().c_str()); // debug
#endif
                }
            }

            // We can commit.
            for (TLock &lk : readLocks) {
                assert(lk.mode() != Mode::X);
                lk.unlock();
            }
            for (TLock &lk : writeLocks) {
                assert(lk.mode() == Mode::X);
                lk.update();
                lk.writeFence();
                lk.unlock();
            }
            res.incCommit(isLongTx);
            clearLocks(writeLocks, readLocks, writeSet, readSet);
#ifdef MONITOR
            {
                Spinlock lk(&txInfo.mutex);
                txInfo.lockInfo.clear();
            }
#endif
            // Tx succeeded.
            res.addRetryCount(isLongTx, retry);
            break;

          abort:
            clearLocks(writeLocks, readLocks, writeSet, readSet);
        }

#if 0
        // This is starvation expr only.
        count++;
        if (isLongTx && (longTxSize >= 5 * muV.size() / 100) && count >= 10) {
            shouldQuit = true;
            break;
        }
#endif
    }

#if 0 // for pq1997 only.
    cybozu::lock::PQ1997Lock::Mutex::gc();
#endif
    return res;
}


template <typename TxIdGen, typename TLockTypes>
Result1 readWorker(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, std::vector<typename TLockTypes::Mutex>& muV, TxIdGen& txIdGen, ReadMode rmode, __attribute__((unused)) TxInfo& txInfo)
{
    using TLock = typename TLockTypes::TLock;
    using TLockReader = typename TLockTypes::TLockReader;
    using Mutex = typename TLockTypes::Mutex;
    using Mode = typename TLockTypes::Mode;

    unused(shouldQuit);

    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);
    Result1 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0), idx);
    std::vector<size_t> muIdV(4);
    std::vector<TLock> writeLocks;
    std::vector<TLock> readLocks;
    std::vector<uintptr_t> writeSet;
    std::vector<TLockReader> readSet;
    const bool isLongTx = false;

    while (!start) _mm_pause();
    //size_t count = 0;
    while (!quit) {
        muIdV.resize(4);
        fillMuIdVecLoop(muIdV, rand, muV.size());

        const uint32_t txId = txIdGen.get();

        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            //bool abort = false;
            assert(writeLocks.empty());
            assert(readLocks.empty());
            assert(writeSet.empty());
            assert(readSet.empty());
            for (size_t i = 0; i < muIdV.size(); i++) {
                Mode mode = Mode::S;
                Mutex *mutex = &muV[muIdV[i]];
                const bool tryOccRead = rmode == ReadMode::OCC
                    || (rmode == ReadMode::HYBRID && retry == 0);
                if (tryOccRead) {
                    readSet.emplace_back();
                    TLockReader& r = readSet.back();
                    for (;;) {
                        r.prepare(mutex);
                        // read
                        r.readFence();
                        if (r.verifyAll()) break;
                    }
                } else {
                    readLocks.emplace_back(mutex, mode, txId);
                    TLock& lk = readLocks.back();
                    for (;;) {
                        // read
                        if (!lk.intercepted()) break;
                        // intercept failed.
                        lk.relock();
                    }
                }
            }

            // Pre-commit.
            // verify read locks.
            // serialization points.
            //__atomic_thread_fence(__ATOMIC_ACQUIRE);
            for (TLockReader& r : readSet) {
                const bool ret = r.verifyAll();
                if (!ret) {
                    res.incAbort(isLongTx);
                    goto abort;
                }
            }
            // We can commit.
            res.incCommit(isLongTx);
            clearLocks(writeLocks, readLocks, writeSet, readSet);

            // Tx succeeded.
            res.addRetryCount(isLongTx, retry);
            break;

          abort:
            clearLocks(writeLocks, readLocks, writeSet, readSet);
        }

    }
    return res;
}

template <typename TxIdGen, typename TLockTypes>
Result1 contentionWorker(size_t idx, const bool& start, const bool& quit, std::vector<typename TLockTypes::Mutex>& muV, TxIdGen& txIdGen, ReadMode rmode, size_t nrOp, size_t nrWr)
{
    using Mutex = typename TLockTypes::Mutex;
    using TLock = typename TLockTypes::TLock;
    using TLockReader = typename TLockTypes::TLockReader;
    using Mode = typename TLockTypes::Mode;

    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);
    const bool isLongTx = false;
    Result1 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0), idx);
    assert(nrWr <= nrOp);

    std::vector<size_t> muIdV(nrOp);

    std::vector<TLock> writeLocks;
    std::vector<TLock> readLocks;
    std::vector<uintptr_t> writeSet;
    std::vector<TLockReader> readSet;

    std::vector<size_t> tmpV; // for fillMuIdVecArray.
    std::vector<bool> isWriteV(nrOp);

    while (!start) _mm_pause();
    size_t count = 0; unused(count);
    while (!quit) {
        muIdV.resize(nrOp);
        fillMuIdVecLoop(muIdV, rand, muV.size());
        isWriteV.resize(nrOp);
        fillModeVec(isWriteV, rand, nrWr, tmpV);

#if 0 /* For test. */
        std::sort(muIdV.begin(), muIdV.end());
#endif

        const size_t sz = muIdV.size();
        unused(sz);
        const uint32_t txId = txIdGen.get();

        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            assert(writeLocks.empty());
            assert(readLocks.empty());
            assert(writeSet.empty());
            assert(readSet.empty());
            for (size_t i = 0; i < muIdV.size(); i++) {
                Mode mode = isWriteV[i] ? Mode::X : Mode::S;

                Mutex *mutex = &muV[muIdV[i]];
                if (mode == Mode::S) {
                    const bool tryOccRead = rmode == ReadMode::OCC
                        || (rmode == ReadMode::HYBRID && retry == 0);
                    if (tryOccRead) {
                        readSet.emplace_back();
                        TLockReader& r = readSet.back();
                        for (;;) {
                            r.prepare(mutex);
                            // read
                            r.readFence();
                            if (r.verifyAll()) break;
                        }
                    } else {
                        readLocks.emplace_back(mutex, mode, txId);
                        TLock& lk = readLocks.back();
                        for (;;) {
                            // read
                            if (!lk.intercepted()) break;
                            // intercept failed.
                            lk.relock();
                        }
                    }
                } else {
                    assert(mode == Mode::X);
                    writeLocks.emplace_back(mutex, mode, txId);
                    TLock& lk = writeLocks.back();
                    // modify the resource on write-set.
                    writeSet.emplace_back(lk.getMutexId());
                }
            }

            // Pre-commit.
            for (size_t i = 0; i < writeLocks.size(); i++) {
                if (!writeLocks[i].protect()) {
                    res.incIntercepted(isLongTx);
                    goto abort;
                }
            }
#if 0 // protect read locks.
            for (size_t i = 0; i < readLocks.size(); i++) {
                if (!readLocks[i].protect()) {
                    res.incIntercepted(isLongTx);
                    goto abort;
                }
            }
            // serialization points.
            __atomic_thread_fence(__ATOMIC_ACQUIRE);
#else // verify read locks.
            // serialization points.
            __atomic_thread_fence(__ATOMIC_ACQUIRE);
            for (size_t i = 0; i < readLocks.size(); i++) {
                if (!readLocks[i].unchanged()) {
                    res.incIntercepted(isLongTx);
                    goto abort;
                }
            }
#endif
            if (!readSet.empty() && !writeSet.empty()) {
                // for binary search in verify phase.
                std::sort(writeSet.begin(), writeSet.end());
            }

            for (TLockReader& r : readSet) {
                const bool ret =
                    (!writeSet.empty() && std::binary_search(writeSet.begin(), writeSet.end(), r.getMutexId()))
                    ? r.verifyVersion() : r.verifyAll();
                if (!ret) {
                    res.incAbort(isLongTx);
                    goto abort;
                }
            }

            // We can commit.
            for (TLock &lk : readLocks) {
                assert(lk.mode() != Mode::X);
                lk.unlock();
            }
            for (TLock &lk : writeLocks) {
                assert(lk.mode() == Mode::X);
                lk.update();
                lk.writeFence();
                lk.unlock();
            }
            res.incCommit(isLongTx);
            clearLocks(writeLocks, readLocks, writeSet, readSet);

            // Tx succeeded.
            res.addRetryCount(isLongTx, retry);
            break;

          abort:
            clearLocks(writeLocks, readLocks, writeSet, readSet);
        }
    }
    return res;
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
    size_t nrWr4Long;
    int shortTxMode;
    int longTxMode;

    GlobalTxIdGenerator globalTxIdGen;
    SimpleTxIdGenerator simpleTxIdGen;

    ILockShared() : globalTxIdGen(5, 10) {}
};


/**
 * Using ILock.
 */
template <int txIdGenType, typename PQLock>
Result1 iWorker(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, ILockShared<PQLock>& shared)
{
    using IMutex = typename ILockTypes<PQLock>::IMutex;
    using ILock = typename ILockTypes<PQLock>::ILock;
    using IMode = typename ILockTypes<PQLock>::IMode;
    using ILockReader = typename ILockTypes<PQLock>::ILockReader;

    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    PriorityIdGenerator<12> priIdGen;
    priIdGen.init(idx + 1);
    TxIdGenerator localTxIdGen(&shared.globalTxIdGen);

    std::vector<IMutex>& muV = shared.muV;
    const ReadMode rmode = shared.rmode;
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t nrWr = shared.nrWr;
    const int shortTxMode = shared.shortTxMode;
    const int longTxMode = shared.longTxMode;

    Result1 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0), idx);
    std::vector<size_t> muIdV(nrOp);
    std::vector<ILock> writeLocks;
    std::vector<ILock> readLocks;
    std::vector<uintptr_t> writeSet;
    std::vector<ILockReader> readSet;

    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    // for USE_MIX_TX
    std::vector<bool> isWriteV;
    std::vector<size_t> tmpV2; // for fillModeVec;

    // for USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);


    const bool isLongTx = longTxSize != 0 && idx == 0; // starvation setting.
    if (isLongTx) {
        muIdV.resize(longTxSize);
    } else {
        muIdV.resize(nrOp);
    }

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
                isWriteV.resize(nrOp);
                fillModeVec(isWriteV, rand, nrWr, tmpV2);
            }
        }
#if 0 /* For test. */
        std::sort(muIdV.begin(), muIdV.end());
#endif

        uint64_t priId;
        if (txIdGenType == SCALABLE_TXID_GEN) {
            priId = priIdGen.get(isLongTx ? 0 : 1);
        } else if (txIdGenType == BULK_TXID_GEN) {
            priId = localTxIdGen.get();
        } else if (txIdGenType == SIMPLE_TXID_GEN) {
            priId = shared.simpleTxIdGen.get();
        } else {
            throw cybozu::Exception("bad txIdGenType") << txIdGenType;
        }
        //::printf("worker %zu priId: %" PRIx64 "\n", idx, priId);

        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.
            assert(writeLocks.empty());
            assert(readLocks.empty());
            assert(writeSet.empty());
            assert(readSet.empty());

            for (size_t i = 0; i < muIdV.size(); i++) {
                IMode mode = getMode<decltype(rand), IMode>(
                    rand, boolRand, isWriteV, isLongTx, shortTxMode, longTxMode,
                    nrOp, nrWr, i);

                IMutex &mutex = muV[muIdV[i]];
                if (mode == IMode::S) {
                    const bool tryOccRead = rmode == ReadMode::OCC
                        || (rmode == ReadMode::HYBRID && !isLongTx && retry == 0);
                    if (tryOccRead) {
                        readSet.emplace_back();
                        ILockReader& r = readSet.back();
                        for (;;) {
                            r.prepare(mutex);
                            // read
                            r.readFence();
                            if (r.verifyAll()) {
#if 0
                                ::printf("READ %zu %s\n", r.getMutexId(), r.getLockData().str().c_str()); // QQQ
#endif
                                break;
                            }
                        }
                    } else {
                        readLocks.emplace_back(mutex, mode, priId);
                        ILock& lk = readLocks.back();
                        for (;;) {
                            // read
                            if (!lk.intercepted()) break;
                            // intercept failed.
                            lk.relock();
                        }
                    }
                } else {
                    assert(mode == IMode::X);
                    writeLocks.emplace_back(mutex, mode, priId);
                    ILock& lk = writeLocks.back();
                    // modify the resource on write-set.
                    writeSet.emplace_back(lk.getMutexId());
                }
            }

            // Pre-commit.
            for (size_t i = 0; i < writeLocks.size(); i++) {
                if (!writeLocks[i].protect()) {
                    res.incIntercepted(isLongTx);
                    goto abort;
                }
            }

            // serialization points.
            // write locks use cmpxchg, read verify use mov.
            // In x86, they are not reordered.
            __atomic_thread_fence(__ATOMIC_ACQ_REL);

            // verify read locks.
            for (size_t i = 0; i < readLocks.size(); i++) {
                if (!readLocks[i].unchanged()) {
                    res.incIntercepted(isLongTx);
                    goto abort;
                }
            }
            if (!readSet.empty() && !writeSet.empty()) {
                // for binary search in verify phase.
                std::sort(writeSet.begin(), writeSet.end());
            }

            for (ILockReader& r : readSet) {
                const bool ret =
                    (!writeSet.empty() && std::binary_search(writeSet.begin(), writeSet.end(), r.getMutexId()))
                    ? r.verifyVersion() : r.verifyAll();
                if (!ret) {
#if 0
                    ::printf("NG %zu %zu \nbefore %s\nafter  %s\n"
                             , idx, r.getMutexId()
                             , r.getLockData().str().c_str()
                             , lockD.str().c_str()); // debug
#endif
                    res.incAbort(isLongTx);
                    goto abort;
                } else {
#if 0
                    ::printf("OK %zu %zu \nbefore %s\nafter  %s\n"
                             , idx, r.getMutexId()
                             , r.getLockData().str().c_str()
                             , lockD.str().c_str()); // debug
#endif
                }
            }

            // We can commit.
            for (ILock &lk : readLocks) {
                assert(lk.mode() != IMode::X);
                lk.unlock();
            }
            for (ILock &lk : writeLocks) {
                assert(lk.mode() == IMode::X);
                lk.update();
                lk.writeFence();
                lk.unlock();
            }
            res.incCommit(isLongTx);
            clearLocks(writeLocks, readLocks, writeSet, readSet);

            // Tx succeeded.
            res.addRetryCount(isLongTx, retry);
            break;

          abort:
            clearLocks(writeLocks, readLocks, writeSet, readSet);
        }

#if 0
        // This is starvation expr only.
        count++;
        if (isLongTx && (longTxSize >= 5 * muV.size() / 100) && count >= 10) {
            shouldQuit = true;
            break;
        }
#endif
    }

    return res;
}


/**
 * Using ILock.
 */
template <int txIdGenType, typename PQLock>
Result1 iWorker2(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, ILockShared<PQLock>& shared)
{
    using IMutex = typename ILockTypes<PQLock>::IMutex;
    using IMode = typename ILockTypes<PQLock>::IMode;

    unused(shouldQuit);
    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);

    PriorityIdGenerator<12> priIdGen;
    priIdGen.init(idx + 1);
    TxIdGenerator localTxIdGen(&shared.globalTxIdGen);

    std::vector<IMutex>& muV = shared.muV;
    const ReadMode rmode = shared.rmode;
    const size_t longTxSize = shared.longTxSize;
    const size_t nrOp = shared.nrOp;
    const size_t nrWr = shared.nrWr;
    const int shortTxMode = shared.shortTxMode;
    const int longTxMode = shared.longTxMode;

    Result1 res;
    cybozu::util::Xoroshiro128Plus rand(::time(0), idx);

    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    // for USE_MIX_TX
    std::vector<bool> isWriteV;
    std::vector<size_t> tmpV2; // for fillModeVec;

    // for USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);


    const bool isLongTx = longTxSize != 0 && idx == 0; // starvation setting.
    const size_t realNrOp = isLongTx ? longTxSize : nrOp;
    const size_t realNrWr = isLongTx ? shared.nrWr4Long : nrWr;
    if (!isLongTx && shortTxMode == USE_MIX_TX) {
        isWriteV.resize(nrOp);
    }
#if 0
    GetModeFunc<decltype(rand), IMode>
        getMode(boolRand, isWriteV, isLongTx,
                shortTxMode, longTxMode, realNrOp, nrWr);
#endif

    cybozu::lock::ILockSet<PQLock> lockSet;

    while (!start) _mm_pause();
    size_t count = 0; unused(count);
    while (!quit) {
        if (!isLongTx && shortTxMode == USE_MIX_TX) {
            fillModeVec(isWriteV, rand, nrWr, tmpV2);
        }

        uint64_t priId;
        if (txIdGenType == SCALABLE_TXID_GEN) {
            priId = priIdGen.get(isLongTx ? 0 : 1);
        } else if (txIdGenType == BULK_TXID_GEN) {
            priId = localTxIdGen.get();
        } else if (txIdGenType == SIMPLE_TXID_GEN) {
            priId = shared.simpleTxIdGen.get();
        } else {
            throw cybozu::Exception("bad txIdGenType") << txIdGenType;
        }
        //::printf("worker %zu priId: %" PRIx64 "\n", idx, priId);

        assert(lockSet.isEmpty());
        lockSet.setPriorityId(priId);

        size_t firstRecIdx;
        for (size_t retry = 0;; retry++) {
            if (quit) break; // to quit under starvation.

            for (size_t i = 0; i < realNrOp; i++) {
#if 0
                IMode mode = getMode(i);
#else
                IMode mode = getMode<decltype(rand), IMode>(
                    rand, boolRand, isWriteV, isLongTx, shortTxMode, longTxMode,
                    realNrOp, realNrWr, i);

#endif
                size_t key = getRecordIdx(rand, isLongTx, shortTxMode, longTxMode,
                                          muV.size(), realNrOp, i, firstRecIdx);
                IMutex &mutex = muV[key];

#if 0
                ::printf("%p mutex:%p mode:%hhu  %s\n", &lockSet, &mutex, mode, mutex.atomicLoad().str().c_str()); // QQQQQ
                ::fflush(::stdout);
#endif

#if 1
                const bool tryOccRead = rmode == ReadMode::OCC
                    || (rmode == ReadMode::HYBRID && !isLongTx && retry == 0);
                if (tryOccRead) {
                    bool ret = lockSet.optimisticRead(mutex);
                    unused(ret);
                    assert(ret);
                } else if (mode == IMode::S) {
                    if (!lockSet.pessimisticRead(mutex)) {
                        res.incAbort(isLongTx);
                        goto abort;
                    }
                }
#else
                if (mode == IMode::S) {
                    const bool tryOccRead = rmode == ReadMode::OCC
                        || (rmode == ReadMode::HYBRID && !isLongTx && retry == 0);
                    if (tryOccRead) {
                        bool ret = lockSet.optimisticRead(mutex);
                        unused(ret);
                        assert(ret);
                    } else {
                        if (!lockSet.pessimisticRead(mutex)) {
                            res.incAbort(isLongTx);
                            goto abort;
                        }
                    }
                }
#endif
                if (mode == IMode::X) {
                    assert(mode == IMode::X);
                    if (!lockSet.write(mutex)) {
                        res.incIntercepted(isLongTx);
                        goto abort;
                    }
                }
                //lockSet.print(); // QQQQQ
            }

            // Pre-commit.
            if (!lockSet.protect()) {
                res.incIntercepted(isLongTx);
                goto abort;
            }
            if (!lockSet.verify()) {
                res.incAbort(isLongTx);
                goto abort;
            }

            // We can commit.
            lockSet.updateAndUnlock();
            res.incCommit(isLongTx);

            // Tx succeeded.
            res.addRetryCount(isLongTx, retry);
            break;

          abort:
            lockSet.clear();
        }
    }

    return res;
}




void runTest()
{
#if 0
    ::fprintf(::stderr, "sizeof(Mutex) = %zu\n", sizeof(Mutex));
#endif
#if 0
    TLock::LockData lockD;
    TLock::LockState lockS;
    __uint128_t xxx;
    xxx = 0x000000000000000c;
    xxx <<= 64;
    xxx |= 0x0000008000062905;
    ::memcpy(&lockD, &xxx, sizeof(xxx));
    ::printf("%s\n", lockD.str().c_str());

    xxx = 0x0000000000000011;
    xxx <<= 64;
    xxx |= 0x00000080000628fa;
    ::memcpy(&lockD, &xxx, sizeof(xxx));
    ::printf("%s\n", lockD.str().c_str());

    ::exit(0);
#endif
#if 0
    testLockStateXS();
    testLockStateMG();
#endif
#if 0
    TLock::LockState lockS;
    lockS.set(Mode::S);
    ::printf("canSet %u\n", lockS.canSet(Mode::S));
    lockS.set(Mode::S);
    ::printf("%s\n", lockS.str().c_str());
#endif

#if 0
    //for (ReadMode rmode : {ReadMode::LOCK}) {
    for (ReadMode rmode : {ReadMode::LOCK, ReadMode::OCC, ReadMode::HYBRID}) {
    //for (ReadMode rmode : {ReadMode::HYBRID}) {
        for (size_t nrResPerTh : {4000}) {
        //for (size_t nrResPerTh : {4}) {
        //for (size_t nrResPerTh : {4, 4000}) {
            //for (size_t nrTh : {32}) {
            for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
                if (nrTh > 2 && nrTh % 2 != 0) continue;
                for(size_t i = 0; i < 10; i++) {
                    bool verbose = false;
                    runExec(nrResPerTh * nrTh, nrTh, 10, rmode, verbose, 0, 4, 2);
                    //sleepMs(1000);
                }
            }
        }
    }
#endif
#if 0
    // high-contention expr.
    //for (ReadMode rmode : {ReadMode::LOCK}) {
    for (ReadMode rmode : {ReadMode::LOCK, ReadMode::OCC, ReadMode::HYBRID}) {
        for (size_t nrMutex : {40}) {
            //for (size_t nrTh : {32}) {
            //for (size_t nrTh = 1; nrTh <= 32; nrTh++) {
            for (size_t nrTh : {8, 16, 24, 32}) {
                if (nrTh > 16 && nrTh % 2 != 0) continue;
                for (size_t nrWr = 0; nrWr <= 10; nrWr++) {
                    for (size_t i = 0; i < 10; i++) {
                        bool verbose = false;
                        runExec(nrMutex, nrTh, 10, rmode, verbose, 0, 10, nrWr);
                    }
                }
            }
        }
    }
#endif
#if 0
    // starvation expr.
    //for (ReadMode rmode : {ReadMode::LOCK}) {
    //for (ReadMode rmode : {ReadMode::OCC}) {
    for (ReadMode rmode : {ReadMode::LOCK, ReadMode::OCC, ReadMode::HYBRID}) {
#if 0
        const size_t nrMutex = 400 * 1000 * 1000;
        const size_t nrTh = 16;
#else
        const size_t nrMutex = 40 * 1000;
        const size_t nrTh = 8;
#endif
        // for (size_t longTxPml : {10, 20, 30, 40, 50, 60, 70, 80, 90,
        //             100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}) {
        //for (size_t longTxPml : {11, 12, 13, 14, 15, 16, 17, 18, 19}) {
        // for (size_t longTxPml : {1, 2, 3, 4, 5, 6, 7, 8, 9,
        //             10, 20, 30, 40, 50, 60, 70, 80, 90,
        //             100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}) {
        for (size_t longTxPml : {
                1, 2, 3, 4, 5, 6, 7, 8, 9,
                10, 20, 30, 40, 50, 60, 70, 80, 90,
                100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}) {
            const size_t longTxSize = longTxPml * nrMutex / 1000;
            for (size_t i = 0; i < 10; i++) {
                bool verbose = false;
                //size_t maxSec = longTxPml >= 50 ? 10000 : 100;
                size_t maxSec = 100;
                runExec(nrMutex, nrTh, maxSec, rmode, verbose, longTxSize);
                //sleepMs(1000);
            }
        }
    }
#endif
#if 0
    runExec(5000, 8, 10, ReadMode::LOCK, true);
#endif

#if 0
    testRandom();
#endif
}


struct CmdLineOptionPlus : CmdLineOption
{
    using base = CmdLineOption;

    std::string modeStr;
    int txIdGenType;
    int pqLockType;

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendOpt(&modeStr, "trlock", "mode", "[mode]: specify mode in trlock, trlock-occ, trlock-hybrid.");
        appendOpt(&txIdGenType, 0, "txid-gen", "[id]: txid gen method (0:sclable, 1:bulk, 2:simple)");
        appendOpt(&pqLockType, 0, "pqlock", "[id]: pqlock type (0:none, 1:pqspin, 2:pqposix, 3:pqmcs1, 4:pqmcs2, 5:pq1993, 6:pq1997, 7:pqmcs3)");
    }
    std::string str() const {
        return cybozu::util::formatString("mode:%s ", modeStr.c_str()) +
            base::str() +
            cybozu::util::formatString(
                " txIdGenType:%d pqLockType:%d"
                , txIdGenType, pqLockType);
    }
};


template <typename PQLock, int workerType, int txIdGenType, typename Shared>
struct Dispatch3
{
};

template <typename PQLock, int txIdGenType, typename Shared>
struct Dispatch3<PQLock, 0, txIdGenType, Shared>
{
    static void run(CmdLineOptionPlus& opt, Shared& shared) {
        Result1 res;
#if 0
        runExec(opt, shared, iWorker<txIdGenType, PQLock>, res);
#else
        runExec(opt, shared, iWorker2<txIdGenType, PQLock>, res);
#endif
    }
};

template <typename PQLock, int txIdGenType, typename Shared>
struct Dispatch3<PQLock, 1, txIdGenType, Shared>
{
    static void run(CmdLineOptionPlus& opt, Shared& shared) {
        Result1 res;
        runExec(opt, shared, tWorker<txIdGenType, PQLock>, res);
    }
};

template <typename PQLock, int workerType, typename Shared>
void dispatch2(CmdLineOptionPlus& opt, Shared& shared)
{
    switch (opt.txIdGenType) {
    case SCALABLE_TXID_GEN:
        Dispatch3<PQLock, workerType, SCALABLE_TXID_GEN, Shared>::run(opt, shared);
        break;
    case BULK_TXID_GEN:
        Dispatch3<PQLock, workerType, BULK_TXID_GEN, Shared>::run(opt, shared);
        break;
    case SIMPLE_TXID_GEN:
        Dispatch3<PQLock, workerType, SIMPLE_TXID_GEN, Shared>::run(opt, shared);
        break;
    default:
        throw cybozu::Exception("bad txIdGenType") << opt.txIdGenType;
    }
};


template <typename PQLock>
void dispatch1(CmdLineOptionPlus& opt)
{
    if (opt.workload == "custom") {
        ILockShared<PQLock> shared;
        shared.muV.resize(opt.getNrMu());
        shared.rmode = strToReadMode(opt.modeStr.c_str());
        shared.longTxSize = opt.longTxSize;
        shared.nrOp = opt.nrOp;
        shared.nrWr = opt.nrWr;
        shared.nrWr4Long = opt.nrWr4Long;
        shared.shortTxMode = opt.shortTxMode;
        shared.longTxMode = opt.longTxMode;
        for (size_t i = 0; i < opt.nrLoop; i++) {
            dispatch2<PQLock, 0>(opt, shared);
        }
    } else if (opt.workload == "custom-t") {
        TLockShared<PQLock> shared;
        shared.muV.resize(opt.getNrMu());
        shared.rmode = strToReadMode(opt.modeStr.c_str());
        shared.longTxSize = opt.longTxSize;
        shared.nrOp = opt.nrOp;
        shared.nrWr = opt.nrWr;
        shared.nrWr4Long = opt.nrWr4Long;
        shared.shortTxMode = opt.shortTxMode;
        shared.longTxMode = opt.longTxMode;
        for (size_t i = 0; i < opt.nrLoop; i++) {
            dispatch2<PQLock, 1>(opt, shared);
        }
    } else {
        throw cybozu::Exception("bad workload.") << opt.workload;
    }
}

void dispatch0(CmdLineOptionPlus& opt)
{
    switch (opt.pqLockType) {
    case USE_PQNoneLock:
        dispatch1<cybozu::lock::PQNoneLock>(opt);
        break;
    case USE_PQSpinLock:
        dispatch1<cybozu::lock::PQSpinLock>(opt);
        break;
    case USE_PQPosixLock:
        dispatch1<cybozu::lock::PQPosixLock>(opt);
        break;
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
    CmdLineOptionPlus opt("tlock_bench: benchmark with transferable/interceptible lock.");
    opt.parse(argc, argv);
    dispatch0(opt);

} catch (std::exception& e) {
    ::fprintf(::stderr, "exeption: %s\n", e.what());
} catch (...) {
    ::fprintf(::stderr, "unknown error\n");
}
