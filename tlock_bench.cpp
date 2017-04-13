#include <unordered_map>
#include <iostream>
#include <sstream>
#include <immintrin.h>
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
    // (0:none, 1:pqspin, 2:pqposix, 3:pqmcs1, 4:pqmcs2, 5:pq1993, 6:pq1997)");
    USE_PQNoneLock = 0,
    USE_PQSpinLock = 1,
    USE_PQPosixLock = 2,
    USE_PQMcsLock = 3,
    USE_PQMcsLock2 = 4,
    USE_PQ1993Lock = 5,
    USE_PQ1997Lock = 6,
};

//using PQLock = cybozu::lock::PQNoneLock;
//using PQLock = cybozu::lock::PQSpinLock;
//using PQLock = cybozu::lock::PQMcsLock;
//using PQLock = cybozu::lock::PQMcsLock2;
//using PQLock = cybozu::lock::PQPosixLock;
//using PQLock = cybozu::lock::PQ1993Lock;
//using PQLock = cybozu::lock::PQ1997Lock;


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

    std::string workload;
    std::vector<Mutex> muV;
    size_t nrMuPerTh;
    ReadMode rmode;
    TxInfo txInfo;
    int shortMode;
    int txIdGenType;
    int pqLockType;
    size_t longTxSize;

    GlobalTxIdGenerator globalTxIdGen;
    SimpleTxIdGenerator simpleTxIdGen;

    TLockShared() : globalTxIdGen(5, 10) {}

    std::string str() const {
        return cybozu::util::formatString(
            "mode:%s workload:%s longTxSize:%zu nrMutex:%zu nrMutexPerTh:%zu "
            "shortMode:%d txIdGenType:%d pqLockType:%d"
            , readModeToStr(rmode), workload.c_str()
            , longTxSize, muV.size(), nrMuPerTh
            , shortMode, txIdGenType, pqLockType);
    }
};


template <int shortMode, int txIdGenType, typename PQLock>
Result tWorker(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, TLockShared<PQLock>& shared)
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

    PriorityIdGenerator<12> priIdGen;
    priIdGen.init(idx + 1);
    TxIdGenerator localTxIdGen(&shared.globalTxIdGen);

    Result res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    std::vector<size_t> muIdV(4);
    std::vector<TLock> writeLocks;
    std::vector<TLock> readLocks;
    std::vector<uintptr_t> writeSet;
    std::vector<TLockReader> readSet;

    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    // USE_MIX_TX
    std::vector<bool> isWriteV(4);
    std::vector<size_t> tmpV2; // for fillModeVec;

    // USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);

#ifdef MONITOR
    {
        Spinlock lk(&txInfo.mutex);
        txInfo.thId = i;
    }
#endif

    while (!start) _mm_pause();
    size_t count = 0; unused(count);
    while (!quit) {
        //const bool isLongTx =  rand() % 1000 == 0; // 0.1% long transaction.
        //const bool isLongTx = idx == 0 || idx == 1; // starvation setting.
        const bool isLongTx = longTxSize != 0 && idx == 0; // starvation setting.
        if (isLongTx) {
            muIdV.resize(longTxSize);
            if (longTxSize > muV.size() * 5 / 1000) {
                fillMuIdVecArray(muIdV, rand, muV.size(), tmpV);
            } else {
                fillMuIdVecLoop(muIdV, rand, muV.size());
            }
        } else {
            muIdV.resize(4);
            fillMuIdVecLoop(muIdV, rand, muV.size());
            if (shortMode == USE_MIX_TX) {
                isWriteV.resize(4);
                fillModeVec(isWriteV, rand, 2, tmpV2);
            }
        }
#if 0 /* For test. */
        std::sort(muIdV.begin(), muIdV.end());
#endif

        const size_t sz = muIdV.size();

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
                    if (i == sz - 1 || i == sz - 2) {
                        mode = Mode::X;
                    } else {
                        mode = Mode::S;
                    }
                }

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
                        || (rmode == ReadMode::HYBRID && retry == 0);
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
Result readWorker(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, std::vector<typename TLockTypes::Mutex>& muV, TxIdGen& txIdGen, ReadMode rmode, __attribute__((unused)) TxInfo& txInfo)
{
    using TLock = typename TLockTypes::TLock;
    using TLockReader = typename TLockTypes::TLockReader;
    using Mutex = typename TLockTypes::Mutex;
    using Mode = typename TLockTypes::Mode;

    unused(shouldQuit);

    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);
    Result res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
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
Result contentionWorker(size_t idx, const bool& start, const bool& quit, std::vector<typename TLockTypes::Mutex>& muV, TxIdGen& txIdGen, ReadMode rmode, size_t nrOp, size_t nrWr)
{
    using Mutex = typename TLockTypes::Mutex;
    using TLock = typename TLockTypes::TLock;
    using TLockReader = typename TLockTypes::TLockReader;
    using Mode = typename TLockTypes::Mode;

    cybozu::thread::setThreadAffinity(::pthread_self(), CpuId_[idx]);
    const bool isLongTx = false;
    Result res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
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

    std::string workload;
    size_t nrMuPerTh;
    std::vector<IMutex> muV;
    ReadMode rmode;
    int shortMode;
    int txIdGenType;
    int pqLockType;
    size_t longTxSize;

    GlobalTxIdGenerator globalTxIdGen;
    SimpleTxIdGenerator simpleTxIdGen;

    ILockShared() : globalTxIdGen(5, 10) {}

    std::string str() const {
        return cybozu::util::formatString(
            "workload:%s mode:%s longTxSize:%zu nrMutex:%zu nrMutexPerTh:%zu "
            "shortMode:%d txIdGenType:%d pqLockType:%d"
            , workload.c_str(), readModeToStr(rmode)
            , longTxSize, muV.size(), nrMuPerTh
            , shortMode, txIdGenType, pqLockType);
    }
};


/**
 * Using ILock.
 */
template <int shortMode, int txIdGenType, typename PQLock>
Result iWorker(size_t idx, const bool& start, const bool& quit, bool& shouldQuit, ILockShared<PQLock>& shared)
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

    Result res;
    cybozu::util::Xoroshiro128Plus rand(::time(0) + idx);
    std::vector<size_t> muIdV(4);
    std::vector<ILock> writeLocks;
    std::vector<ILock> readLocks;
    std::vector<uintptr_t> writeSet;
    std::vector<ILockReader> readSet;

    std::vector<size_t> tmpV; // for fillMuIdVecArray.

    // for USE_MIX_TX
    std::vector<bool> isWriteV(4);
    std::vector<size_t> tmpV2; // for fillModeVec;

    // for USE_LONG_TX_2
    BoolRandom<decltype(rand)> boolRand(rand);

    while (!start) _mm_pause();
    size_t count = 0; unused(count);
    while (!quit) {
        //const bool isLongTx =  rand() % 1000 == 0; // 0.1% long transaction.
        //const bool isLongTx = idx == 0 || idx == 1; // starvation setting.
        const bool isLongTx = longTxSize != 0 && idx == 0; // starvation setting.
        if (isLongTx) {
            muIdV.resize(longTxSize);
            if (longTxSize > muV.size() * 5 / 1000) {
                fillMuIdVecArray(muIdV, rand, muV.size(), tmpV);
            } else {
                fillMuIdVecLoop(muIdV, rand, muV.size());
            }
        } else {
            muIdV.resize(4);
            fillMuIdVecLoop(muIdV, rand, muV.size());
            if (shortMode == USE_MIX_TX) {
                isWriteV.resize(4);
                fillModeVec(isWriteV, rand, 2, tmpV2);
            }
        }
#if 0 /* For test. */
        std::sort(muIdV.begin(), muIdV.end());
#endif

        const size_t sz = muIdV.size();

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
            assert(writeLocks.empty());
            assert(readLocks.empty());
            assert(writeSet.empty());
            assert(readSet.empty());

            for (size_t i = 0; i < muIdV.size(); i++) {
                //Mode mode = (rand() % 2 == 0 ? Mode::X : Mode::S);
                IMode mode;
                if (shortMode == USE_LONG_TX_2) {
                    mode = boolRand() ? IMode::X : IMode::S;
                } else if (shortMode == USE_READONLY_TX) {
                    mode = IMode::S;
                } else if (shortMode == USE_WRITEONLY_TX) {
                    mode = IMode::X;
                } else if (shortMode == USE_MIX_TX) {
                    mode = isWriteV[i] ? IMode::X : IMode::S;
                } else {
                    assert(shortMode == USE_R2W2);
                    if (i == sz - 1 || i == sz - 2) {
                        mode = IMode::X;
                    } else {
                        mode = IMode::S;
                    }
                }

                IMutex &mutex = muV[muIdV[i]];
                if (mode == IMode::S) {
                    const bool tryOccRead = rmode == ReadMode::OCC
                        || (rmode == ReadMode::HYBRID && retry == 0);
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
    size_t nrMuPerTh;
    std::string workload;
    int shortMode;
    int txIdGenType;
    int pqLockType;
    size_t longTxSize;

    CmdLineOptionPlus(const std::string& description) : CmdLineOption(description) {
        appendOpt(&modeStr, "lock", "mode", "[mode]: specify mode in trlock, trlock-occ, trlock-hybrid.");
        appendMust(&nrMuPerTh, "mu", "[num]: number of mutexes per thread.");
        appendMust(&workload, "w", "[workload]: workload type in 'shortlong', 'shortlong-t', 'high-contention', 'high-conflicts'.");
        appendOpt(&shortMode, 0, "sm", "[id]: short workload mode (0:r2w2, 1:long2, 2:ro, 3:wo, 4:mix)");
        appendOpt(&txIdGenType, 0, "txid-gen", "[id]: txid gen method (0:sclable, 1:bulk, 2:simple)");
        appendOpt(&longTxSize, 0, "long-tx-size", "[size]: long tx size. 0 means no long tx.");
        appendOpt(&pqLockType, 0, "pqlock", "[id]: pqlock type (0:none, 1:pqspin, 2:pqposix, 3:pqmcs1, 4:pqmcs2, 5:pq1993, 6:pq1997)");
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


template <typename PQLock, int workerType, int shortMode, int txIdGenType, typename Shared>
struct Dispatch4
{
};

template <typename PQLock, int shortMode, int txIdGenType, typename Shared>
struct Dispatch4<PQLock, 0, shortMode, txIdGenType, Shared>
{
    static void run(CmdLineOptionPlus& opt, Shared& shared) {
        runExec(opt, shared, iWorker<shortMode, txIdGenType, PQLock>);
    }
};

template <typename PQLock, int shortMode, int txIdGenType, typename Shared>
struct Dispatch4<PQLock, 1, shortMode, txIdGenType, Shared>
{
    static void run(CmdLineOptionPlus& opt, Shared& shared) {
        runExec(opt, shared, tWorker<shortMode, txIdGenType, PQLock>);
    }
};

template <typename PQLock, int workerType, int shortMode, typename Shared>
void dispatch3(CmdLineOptionPlus& opt, Shared& shared)
{
    switch (opt.txIdGenType) {
    case SCALABLE_TXID_GEN:
        Dispatch4<PQLock, workerType, shortMode, SCALABLE_TXID_GEN, Shared>::run(opt, shared);
        break;
    case BULK_TXID_GEN:
        Dispatch4<PQLock, workerType, shortMode, BULK_TXID_GEN, Shared>::run(opt, shared);
        break;
    case SIMPLE_TXID_GEN:
        Dispatch4<PQLock, workerType, shortMode, SIMPLE_TXID_GEN, Shared>::run(opt, shared);
        break;
    default:
        throw cybozu::Exception("bad txIdGenType") << opt.txIdGenType;
    }
};

template <typename PQLock, int workerType, typename Shared>
void dispatch2(CmdLineOptionPlus& opt, Shared& shared)
{
    switch (opt.shortMode) {
    case USE_R2W2:
        dispatch3<PQLock, workerType, USE_R2W2>(opt, shared);
        break;
    case USE_LONG_TX_2:
        dispatch3<PQLock, workerType, USE_LONG_TX_2>(opt, shared);
        break;
    case USE_READONLY_TX:
        dispatch3<PQLock, workerType, USE_READONLY_TX>(opt, shared);
        break;
    case USE_WRITEONLY_TX:
        dispatch3<PQLock, workerType, USE_WRITEONLY_TX>(opt, shared);
        break;
    case USE_MIX_TX:
        dispatch3<PQLock, workerType, USE_MIX_TX>(opt, shared);
        break;
    default:
        throw cybozu::Exception("bad shortMode") << opt.shortMode;
    }
}


template <typename PQLock>
void dispatch1(CmdLineOptionPlus& opt)
{
    if (opt.workload == "shortlong") {
        ILockShared<PQLock> shared;
        shared.workload = opt.workload;
        shared.nrMuPerTh = opt.nrMuPerTh;
        shared.muV.resize(opt.nrMuPerTh * opt.nrTh);
        shared.rmode = strToReadMode(opt.modeStr.c_str());
        shared.shortMode = opt.shortMode;
        shared.txIdGenType = opt.txIdGenType;
        shared.pqLockType = opt.pqLockType;
        shared.longTxSize = opt.longTxSize;
        for (size_t i = 0; i < opt.nrLoop; i++) {
            dispatch2<PQLock, 0>(opt, shared);
        }
    } else if (opt.workload == "shortlong-t") {
        TLockShared<PQLock> shared;
        shared.workload = opt.workload;
        shared.nrMuPerTh = opt.nrMuPerTh;
        shared.muV.resize(shared.nrMuPerTh * opt.nrTh);
        shared.rmode = strToReadMode(opt.modeStr.c_str());
        shared.shortMode = opt.shortMode;
        shared.txIdGenType = opt.txIdGenType;
        shared.pqLockType = opt.pqLockType;
        shared.longTxSize = opt.longTxSize;
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
