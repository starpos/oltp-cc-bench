#pragma once
/**
 * Transferable Lock.
 */
#include "pqlock.hpp"
#include "lock.hpp"
#include "lock_data.hpp"
#include "constexpr_util.hpp"
#include "allocator.hpp"

#define USE_TRLOCK_PQMCS
//#undef USE_TRLOCK_PQMCS


namespace cybozu {
namespace lock {

struct LockDataXS
{
    using LockState = LockStateXS;
    using AtomicDataType = uint128_t;
    /**
     * 128bit data.
     *
     * 0-31(32bit):   TxID
     * 32-39(8bit):   t-lock (transferable)
     * 40-47(8bit):   reserved.
     * 48-63(8bit):   n-lock (non-transferable)
     * 56-63(8bit):   reserved.
     * 64-95(32bit):  interception count
     * 96-127(32bit): update version
     */
#if 0
    uint32_t txId;
    LockState tState;
    uint8_t reserved1;
    LockState nState;
    uint8_t reserved2;
    uint32_t iVersion;
    uint32_t uVersion;
#else
    // top half
    uint32_t txId;
    uint32_t iVersion;
    // bottom half
    LockState tState;
    LockState nState;
    uint16_t reserved1;
    uint32_t uVersion;
#endif
#if 0
    LockDataXS() : txId(UINT32_MAX), tState(), nState(), iVersion(0), uVersion(0) {}
#else
    LockDataXS() : txId(UINT32_MAX), iVersion(0), tState(), nState(), uVersion(0) {}
#endif
    LockDataXS(uint128_t x) {
        ::memcpy(this, &x, sizeof(*this));
    }

    // Set bottom half only.
    void setBottomHalf(uint64_t x) {
        ::memset(this, 0, sizeof(uint64_t));
        ::memcpy((uint8_t *)this + sizeof(uint64_t), &x, sizeof(uint64_t));
    }

    std::string str() const {
        return cybozu::util::formatString(
            "txId %10u  "
            "t-state %s  "
            "n-state %s  "
            "iVersion %u  "
            "uVersion %u"
            , txId
            , tState.str().c_str()
            , nState.str().c_str()
            , iVersion
            , uVersion);
    }
    bool operator==(const LockDataXS& rhs) const {
        return ::memcmp(this, &rhs, sizeof(*this)) == 0;
    }
    bool operator!=(const LockDataXS& rhs) const {
        return !operator==(rhs);
    }
    operator uint128_t() const {
        uint128_t x;
        ::memcpy(&x, this, sizeof(x));
        return x;
    }
};


struct LockDataMG
{
    using LockState = LockStateMG;
    using AtomicDataType = uint128_t;
    /**
     * 128bit data.
     *
     * 0-31(32bit):   TxID
     * 32-47(16bit):  t-lock (transferable)
     * 48-63(16bit):  n-lock (non-transferable)
     * 64-95(32bit):  interception count
     * 96-127(32bit): update version
     */
    uint32_t txId;
    LockState tState;
    LockState nState;
    uint32_t iVersion;
    uint32_t uVersion;

    LockDataMG() : txId(UINT32_MAX), tState(), nState(), iVersion(0), uVersion(0) {}
    LockDataMG(uint128_t x) {
        ::memcpy(this, &x, sizeof(*this));
    }

    std::string str() const {
        return cybozu::util::formatString(
            "txId %10u\n"
            "t-state %s\n"
            "n-state %s\n"
            "iVersion %u\n"
            "uVersion %u\n"
            , txId
            , tState.str().c_str()
            , nState.str().c_str()
            , iVersion
            , uVersion);
    }
    bool operator==(const LockDataMG& rhs) const {
        return ::memcmp(this, &rhs, sizeof(*this)) == 0;
    }
    bool operator!=(const LockDataMG& rhs) const {
        return !operator==(rhs);
    }
    operator uint128_t() const {
        uint128_t x;
        ::memcpy(&x, this, sizeof(x));
        return x;
    }
};


/**
 * Transferable Lock.
 */
template <typename PQLock, typename LockData>
class TransferableLockT
{
public:
    using LockState = typename LockData::LockState;
    using Mode = typename LockState::Mode;
    using AtomicDataType = typename LockData::AtomicDataType;

    struct Mutex {
#ifdef MUTEX_ON_CACHELINE
        alignas(CACHE_LINE_SIZE)
#endif
        AtomicDataType obj;
        static_assert(sizeof(LockData) <= sizeof(AtomicDataType), "LockData must not proceed AtomicDataType.");
#ifdef USE_TRLOCK_PQMCS
        std::unique_ptr<typename PQLock::Mutex> pqMutexP;
#endif

        Mutex() : obj()
#ifdef USE_TRLOCK_PQMCS
                , pqMutexP(new typename PQLock::Mutex())
#endif
        {
            LockData lockD;
            __atomic_store_n(&obj, lockD, __ATOMIC_RELAXED);
        }
        LockData load() const {
            return __atomic_load_n(&obj, __ATOMIC_RELAXED);
        }
        /**
         * Bottom half 64bit read is enough for optimistic read.
         * 128bit atomic read is very slow than 64bit atomic read in some kind of CPUs.
         */
        LockData loadBottomHalf() const {
            const uint64_t *ptr = (const uint64_t *)(((const uint8_t *)&obj) + sizeof(uint64_t));
            uint64_t val = __atomic_load_n(ptr, __ATOMIC_RELAXED);
            LockData ret;
            ret.setBottomHalf(val);
            return ret;
        }
        bool compareAndSwap(LockData& before, const LockData& after) {
            return __atomic_compare_exchange(&obj, (AtomicDataType *)&before, (AtomicDataType *)&after, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
        }
        void set(const LockData& after) {
            obj = after;
        }
        void atomicSet(const LockData& after) {
            __atomic_store_n(&obj, after, __ATOMIC_RELAXED);
        }
    };
private:
    Mutex *mutex_;
    uint32_t txId_;
    uint32_t iVersion_;
    uint32_t uVersion_;
    Mode mode_;
    bool protected_;
    mutable bool intercepted_;
    bool updated_;
public:
    TransferableLockT()
        : mutex_(nullptr), txId_(), iVersion_(), uVersion_(), mode_(Mode::INVALID)
        , protected_(), intercepted_(), updated_() {}
    /**
     * When the constructor returned, t-lock is held.
     * However, the lock may be intercepted by prior Tx before protect() succeeded.
     */
    TransferableLockT(Mutex *mutex, Mode mode, uint32_t txId) : TransferableLockT() {
        lock(mutex, mode, txId);
    }
    ~TransferableLockT() noexcept {
        unlock();
    }
    TransferableLockT(const TransferableLockT&) = delete;
    TransferableLockT(TransferableLockT&& rhs) : TransferableLockT() { swap(rhs); }
    TransferableLockT& operator=(const TransferableLockT&) = delete;
    TransferableLockT& operator=(TransferableLockT&& rhs) { swap(rhs); return *this; }

    void lock(Mutex *mutex, Mode mode, uint32_t txId) {
        assert(!mutex_);
        assert(mutex);
        assert(mode != Mode::INVALID);
        assert(txId != UINT32_MAX);
        mutex_ = mutex;
        mode_ = mode;
        txId_ = txId;
        protected_ = false;
        intercepted_ = false;
        updated_ = false;

        LockData lockD0 = mutex_->load();
        for (;;) {
            LockData lockD1;
            if (isUnlockedOrShared(lockD0)) {
                prepareTLock(lockD0, lockD1);
            } else if (canIntercept(lockD0)) {
                prepareIntercept(lockD0, lockD1);
            } else {
                waitFor(lockD0);
                continue;
            }
            if (!mutex_->compareAndSwap(lockD0, lockD1)) {
                continue;
            }
            // Locked (transferable).
            iVersion_ = lockD1.iVersion;
            uVersion_ = lockD1.uVersion;
            break;
        }
    }
    void unlock() {
        if (!mutex_) return;
        for (;;) {
            if (intercepted_) {
                assert(!protected_);
                mutex_ = nullptr;
                return;
            }
            LockData lockD0 = mutex_->load();
            if (interceptedDetail(lockD0)) {
                mutex_ = nullptr;
                return;
            }
            LockData lockD1 = lockD0;
            if (protected_) {
                lockD1.nState.clear(mode_);
                if (updated_) {
                    lockD1.uVersion++;
                }
            } else {
                lockD1.tState.clear(mode_);
            }
            if (mode_ != Mode::X) {
                /*
                 * If it keeps txIds who have shared lock,
                 * next txId version of the lock object will be determined correctly.
                 * Currently we choose not to maintain such txIds,
                 * so next txId must be UINT32_MAX in order to avoid dead-lock.
                 * This means everyone can intercept the lock.
                 */
                if (lockD1.txId == txId_ &&
                    (!lockD1.tState.isUnlocked() || !lockD1.nState.isUnlocked())) {
                    lockD1.txId = UINT32_MAX;
                }
            }
#if 1
            if (protected_ && mode_ == Mode::X) {
                /*
                 * Any other threads can not change the mutex here.
                 * You must write fence before really unlock.
                 * In x86_64 write-write order does not change, so we need compiler fence only.
                 */
                __atomic_thread_fence(__ATOMIC_RELEASE);
                mutex_->set(lockD1);
                break;
            } else {
                if (mutex_->compareAndSwap(lockD0, lockD1)) {
                    break;
                }
            }
#else
            if (mutex_->compareAndSwap(lockD0, lockD1)) {
#if 0
                ::printf("UNLOCK %zu\nbefore %s\nafter  %s\n"
                         , getMutexId(), lockD0.str().c_str(), lockD1.str().c_str()); // QQQ
#endif
                break;
            }
#endif
        }
        mutex_ = nullptr;
    }
    void relock() {
        Mutex *mutex = mutex_;
        Mode mode = mode_;
        uint32_t txId = txId_;
        unlock();
        lock(mutex, mode, txId);
    }
    /**
     * Return true if the lock has been intercepted by another Tx.
     */
    bool intercepted() {
        if (intercepted_) return true;
        LockData lockD = mutex_->load();
        return interceptedDetail(lockD);
    }
    /**
     * Use to verify read.
     */
    bool unchanged() {
#if 1
        LockData lockD = mutex_->loadBottomHalf();
#else
        LockData lockD = mutex_->load();
#endif
        return !lockD.nState.get(Mode::X) && uVersion_ == lockD.uVersion;
    }
    /**
     * Try to make the lock non-transferable.
     */
    bool protect() {
        if (protected_) return true;
        for (;;) {
            if (intercepted_) return false;
            LockData lockD0 = mutex_->load();
            if (interceptedDetail(lockD0)) return false;
            LockData lockD1 = lockD0;
            assert(lockD1.tState.canClear(mode_));
            lockD1.tState.clear(mode_);
            assert(lockD1.nState.canSet(mode_));
            lockD1.nState.set(mode_);
            if (mutex_->compareAndSwap(lockD0, lockD1)) {
                protected_ = true;
                return true;
            }
        }
    }
    void update() {
        if (!protected_) return;
        updated_ = true;
    }
    Mode mode() const { return mode_; }
    void writeFence() const {
        __atomic_thread_fence(__ATOMIC_RELEASE);
    }
    /**
     * Debug
     */
    std::string str() const {
        const LockData lockD = mutex_->load();
        return lockD.str();
    }
    uintptr_t getMutexId() const {
        return reinterpret_cast<uintptr_t>(mutex_);
    }
private:
    void swap(TransferableLockT& rhs) {
        std::swap(mutex_, rhs.mutex_);
        std::swap(txId_, rhs.txId_);
        std::swap(iVersion_, rhs.iVersion_);
        std::swap(uVersion_, rhs.uVersion_);
        std::swap(mode_, rhs.mode_);
        std::swap(protected_, rhs.protected_);
        std::swap(intercepted_, rhs.intercepted_);
        std::swap(updated_, rhs.updated_);
    }
    bool isUnlockedOrShared(const LockData& lockD) const {
        return lockD.tState.canSet(mode_) && lockD.nState.canSet(mode_);
    }
    bool canIntercept(const LockData& lockD) const {
        return txId_ < lockD.txId && lockD.nState.isUnlocked();
    }
    bool canLock(const LockData& lockD) const {
        return isUnlockedOrShared(lockD) || canIntercept(lockD);
    }
#ifdef USE_TRLOCK_PQMCS
    /**
     * A thread must have the MCS lock to spin on the lock data,
     * so that the thread with top priority will try to lock
     * in order to reduce interception.
     */
    void waitFor(LockData& lockD) {
        for (;;) {
            PQLock lk(mutex_->pqMutexP.get(), txId_);
            for (;;) {
                lockD = mutex_->load();
                if (txId_ > lk.getTopPriorityInWaitQueue()) {
                    // Give the MCS lock to the prior thread.
                    break;
                }
                if (canLock(lockD)) {
                    // Try to lock again.
                    return;
                }
                _mm_pause();
            }
        }
    }
#else
    /**
     * Very simple version.
     */
    void waitFor(LockData& lockD) {
        for (;;) {
            lockD = mutex_->load();
            if (canLock(lockD)) return;
            _mm_pause();
        }
    }
#endif
    bool interceptedDetail(LockData& lockD0) {
        if (protected_) {
#ifndef NDEBUG
            assert(lockD0.iVersion == iVersion_);
#endif
            return false;
        }
        if (lockD0.iVersion == iVersion_) return false;

        // re-tlock or re-intercept if possible.
        while (lockD0.uVersion == uVersion_) {
            LockData lockD1;
            if (isUnlockedOrShared(lockD0)) {
                prepareTLock(lockD0, lockD1);
            } else if (canIntercept(lockD0)) {
                prepareIntercept(lockD0, lockD1);
            } else {
                // give up.
                break;
            }
            if (mutex_->compareAndSwap(lockD0, lockD1)) {
                // t-lock succeeded.
                lockD0 = lockD1;
                iVersion_ = lockD0.iVersion;
                return false;
            }
        }
        intercepted_ = true;
        return true;
    }
    void prepareTLock(const LockData& lockD0, LockData& lockD1) {
        lockD1 = lockD0;
        lockD1.tState.set(mode_);
        if (lockD0.tState.isUnlocked() || txId_ < lockD0.txId) {
            lockD1.txId = txId_;
        }
    }
    void prepareIntercept(const LockData& lockD0, LockData& lockD1) {
        lockD1 = lockD0;
        lockD1.tState.clearAll();
        lockD1.tState.set(mode_);
        lockD1.txId = txId_;
        lockD1.iVersion++;
    }
};


template <typename PQLock, typename LockData>
class TransferableLockReaderT
{
private:
    using TLock = TransferableLockT<PQLock, LockData>;
    using Mutex = typename TLock::Mutex;
    using Mode = typename LockData::LockState::Mode;
    Mutex *mutex_;
    LockData lockD_;
public:
    TransferableLockReaderT() : mutex_(), lockD_() {}
    TransferableLockReaderT(const TransferableLockReaderT&) = delete;
    TransferableLockReaderT(TransferableLockReaderT&& rhs) : TransferableLockReaderT() { swap(rhs); }
    TransferableLockReaderT& operator=(TransferableLockReaderT&& rhs) { swap(rhs); return *this; }
    TransferableLockReaderT& operator=(const TransferableLockReaderT&) = delete;
    void prepare(Mutex *mutex) {
        mutex_ = mutex;
        for (;;) {
            lockD_ = mutex_->loadBottomHalf();
            if (!lockD_.nState.get(Mode::X)) break;
            _mm_pause();
        }
    }
    void readFence() const {
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
    }
    bool verifyAll(LockData *p = nullptr) const {
        assert(mutex_);
        LockData lockD = mutex_->loadBottomHalf();
        if (p) *p = lockD; // QQQ
        return !lockD.nState.get(Mode::X) && lockD_.uVersion == lockD.uVersion;
    }
    bool verifyVersion(LockData *p = nullptr) const {
        assert(mutex_);
        LockData lockD = mutex_->loadBottomHalf();
        if (p) *p = lockD; // QQQ
        return lockD_.uVersion == lockD.uVersion;
    }
    uintptr_t getMutexId() const {
        return reinterpret_cast<uintptr_t>(mutex_);
    }
    /**
     * for debug.
     */
    const LockData& getLockData() const {
        return lockD_;
    }
private:
    void swap(TransferableLockReaderT& rhs) {
        std::swap(mutex_, rhs.mutex_);
        std::swap(lockD_, rhs.lockD_);
    }
};


struct LockData64
{
    enum class Mode : uint8_t { S = 0, X = 1, INVALID = 2, };

    uint8_t writeReserve:1;
    uint8_t writeProtect:1;
    uint8_t readReserve:6;
    uint64_t priId:12;
    uint64_t iVersion:7;
    uint64_t uVersion:37;

    constexpr static const uint64_t MAX_PRI_ID = GetMaxValue(12);

    LockData64()
        : writeReserve(0)
	, writeProtect(0)
	, readReserve(0)
        , priId(MAX_PRI_ID)
        , iVersion(0), uVersion(0) {
    }
    LockData64(uint64_t x) {
        ::memcpy(this, &x, sizeof(*this));
    }
    operator uint64_t() const {
        uint64_t x;
        ::memcpy(&x, this, sizeof(x));
        return x;
    }
    bool operator==(const LockDataXS& rhs) const {
        return ::memcmp(this, &rhs, sizeof(*this)) == 0;
    }
    bool operator!=(const LockDataXS& rhs) const {
        return !operator==(rhs);
    }
    std::string str() const {
        return cybozu::util::formatString(
            "priId %03x  protect %u  reserve %u  read %2u  iVer %3u  uVer %11" PRIu64 ""
            , priId, writeProtect, writeReserve, readReserve, iVersion, uVersion);
    }
    bool isUnlocked() const { return writeReserve == 0 && writeProtect == 0 && readReserve == 0; }
    bool isProtected() const { return writeProtect != 0; }
    bool isWriteReserved() const { return writeReserve != 0; }
    bool isWriteLocked() const { return isProtected() || isWriteReserved(); }
    bool canReserveRead() const {
        return writeReserve == 0 && writeProtect == 0 && readReserve != GetMaxValue(6);
    }
    bool isReadReserved() const {
#ifndef NDEBUG
        const bool ret = readReserve > 0;
        if (ret) {
            assert(writeReserve == 0);
            assert(writeProtect == 0);
        }
        return ret;
#else
        return readReserve > 0;
#endif
    }
    void clearLock() { writeProtect = 0; writeReserve = 0; readReserve = 0; }
    void reserve(const LockData64& before, Mode mode, uint32_t priId) {
        *this = before;
        if (mode == Mode::X) {
            assert(isUnlocked());
            writeReserve = 1;
        } else {
            assert(canReserveRead());
            readReserve++;
        }
        if (before.isUnlocked() || priId < before.priId) {
            this->priId = priId;
        }
    }
    void intercept(const LockData64& before, Mode mode, uint32_t priId) {
        *this = before;
        if (mode == Mode::X) {
            writeReserve = 1;
            writeProtect = 0;
            readReserve = 0;
        } else {
            writeReserve = 0;
            writeProtect = 0;
            readReserve = 1;
            iVersion++;
        }
        this->priId = priId;
    }
};

static_assert(sizeof(LockData64) == sizeof(uint64_t), "LockData size must be 64bit.");

template <typename PQLock>
class InterceptibleLock64T
{
public:
    using Mode = LockData64::Mode;

    struct Mutex {
#ifdef MUTEX_ON_CACHELINE
        alignas(CACHE_LINE_SIZE)
#else
        alignas(8)
#endif
        uint64_t lockObj;
#ifdef USE_TRLOCK_PQMCS
        std::unique_ptr<typename PQLock::Mutex> pqMutexP;
#endif
        Mutex() : lockObj()
#ifdef USE_TRLOCK_PQMCS
                , pqMutexP(new typename PQLock::Mutex())
#endif
        {
            __atomic_store_n(&lockObj, 0, __ATOMIC_RELAXED);
        }
        LockData64 atomicLoad(int mode = __ATOMIC_ACQUIRE) const {
            return __atomic_load_n(&lockObj, mode);
        }
        LockData64 nonAtomicLoad() const {
            return lockObj;
        }
        bool compareAndSwap(LockData64& before, const LockData64& after,
                            int mode0 = __ATOMIC_ACQ_REL, int mode1 = __ATOMIC_RELAXED) {
            return __atomic_compare_exchange(
                &lockObj, (uint64_t *)&before, (uint64_t *)&after, false, mode0, mode1);
        }
        // You must be carefully to use this method.
        void nonAtomicSet(const LockData64& after) {
            lockObj = after;
        }
        void atomicSet(const LockData64& after, int mode = __ATOMIC_RELEASE) {
            __atomic_store_n(&lockObj, after, mode);
        }
    };
private:
    Mutex *mutex_;
    union {
        uint64_t bitObjects_;
        struct {
            // LSB
            uint64_t uVersion_:37;
            uint64_t iVersion_:7;
            uint64_t priId_:12;
            // MSB
        };
    };
    Mode mode_;
    bool protected_;
    bool intercepted_;
    bool updated_;
    bool isOptimisticRead_;
public:
    InterceptibleLock64T()
        : mutex_(nullptr), uVersion_(), iVersion_(), priId_(), mode_(Mode::INVALID)
        , protected_(false), intercepted_(false), updated_(false), isOptimisticRead_(false) {
    }
    InterceptibleLock64T(Mutex& mutex, Mode mode, uint32_t priId)
        : InterceptibleLock64T() {
        lock(mutex, mode, priId);
    }
    ~InterceptibleLock64T() noexcept {
        unlock();
    }
    InterceptibleLock64T(const InterceptibleLock64T&) = delete;
    InterceptibleLock64T(InterceptibleLock64T&& rhs) : InterceptibleLock64T() { swap(rhs); }
    InterceptibleLock64T& operator=(const InterceptibleLock64T&) = delete;
    InterceptibleLock64T& operator=(InterceptibleLock64T&& rhs) { swap(rhs); return *this; }
    bool operator<(const InterceptibleLock64T& rhs) const {
        return getMutexId() < rhs.getMutexId();
    }

    /* Optimistic read. */
    void prepareOptimisticRead(Mutex &mutex) {
        mutex_ = &mutex;
        mode_ = Mode::S;
        isOptimisticRead_ = true;
        for (;;) {
            const LockData64 ld = mutex_->atomicLoad();
            uVersion_ = ld.uVersion;
            if (!ld.isProtected()) break;
            _mm_pause();
        }
    }
    bool verifyAll() const {
        assert(mutex_);
        assert(isOptimisticRead_);
        const LockData64 ld = mutex_->atomicLoad();
        return !ld.isProtected() && ld.uVersion == uVersion_;
    }
    bool verifyVersion() const {
        assert(mutex_);
        assert(isOptimisticRead_);
        const LockData64 ld = mutex_->atomicLoad();
        return ld.uVersion == uVersion_;
    }

    // This is lock reserve.
    void lock(Mutex &mutex, Mode mode, uint32_t priId) {
        assert(!mutex_);
        assert(mode != Mode::INVALID);
        assert(priId < LockData64::MAX_PRI_ID);
        mutex_ = &mutex;
        mode_ = mode;
        priId_ = priId;
        protected_ = false;
        intercepted_ = false;
        updated_ = false;
        isOptimisticRead_ = false;

        LockData64 ld0 = mutex_->atomicLoad();
        for (;;) {
            LockData64 ld1;
            if (isUnlockedOrShared(ld0)) {
                prepareReserve(ld0, ld1);
            } else if (canIntercept(ld0)) {
                prepareIntercept(ld0, ld1);
            } else {
                waitFor(ld0);
                continue;
            }
            if (!mutex_->compareAndSwap(ld0, ld1)) {
                continue;
            }
            // Reserved.
            iVersion_ = ld1.iVersion;
            uVersion_ = ld1.uVersion;
            break;
        }
    }
    void unlock() {
        if (!mutex_) return;
        if (isOptimisticRead_) {
            mutex_ = nullptr;
            return;
        }
        for (;;) {
            if (intercepted_) {
                assert(!protected_);
                mutex_ = nullptr;
                return;
            }
            LockData64 ld0 = mutex_->atomicLoad();
            if (interceptedDetail(ld0)) {
                mutex_ = nullptr;
                return;
            }
            LockData64 ld1 = ld0;
            if (protected_) {
                assert(mode_ == Mode::X);
                ld1.writeProtect = 0;
                assert(ld1.isUnlocked());
                if (updated_) {
                    /*
                     * In practical long-run systems,
                     * you must carefully manage uVersion overflow,
                     * especially when the uVersion is used as FOID,
                     * considering crash recovery.
                     */
                    ld1.uVersion++;
                }
                /*
                 * Any other threads can not change the mutex here.
                 * You must write fence before really unlock.
                 * In x86_64 write-write order does not change, so we need compiler fence only.
                 */
                __atomic_thread_fence(__ATOMIC_RELEASE);
                mutex_->nonAtomicSet(ld1);
                mutex_ = nullptr;
                return;
            }
            if (mode_ == Mode::X) {
                ld1.writeReserve = 0;
                assert(ld1.isUnlocked());
                assert(!updated_);
                if (mutex_->compareAndSwap(ld0, ld1)) {
                    break;
                } else {
                    continue;
                }
            }
            assert(mode_ == Mode::S);
            if (ld1.readReserve > 0) {
                ld1.readReserve--;
            } else {
                /*
                 * This is a rare case.
                 * The lock was intercepted by others really
                 * while mode, priId, and iVersion seems not to be changed.
                 * (i.e. iVersion has been wrap-rounded.)
                 * In this case, its read reservation did not work,
                 * works as opptimistic read.
                 * Even if this case, system is valid.
                 */
#if 1
                ::printf("rare case occurred: %s.\n", ld1.str().c_str());
#endif
            }
            /*
             * If we keeps all priIds who have shared locks,
             * next priId of the lock object will be determined correctly.
             * Currently we choose not to maintain such priIds,
             * so next priId must be MAX_PRI_ID in order to avoid dead-lock.
             * This means everyone can intercept the lock reservation.
             */
            if (ld1.priId == priId_ && !ld1.isUnlocked()) {
                ld1.priId = LockData64::MAX_PRI_ID;
            } else {
                /* Keep ld1's priId because the corresponding Tx
                   still has read reservation on the mutex. */
            }
            if (mutex_->compareAndSwap(ld0, ld1)) {
                mutex_ = nullptr;
                return;
            } else {
                continue;
            }
        }
    }
    void relock() {
        assert(mutex_);
        Mutex *mutex = mutex_;
        Mode mode = mode_;
        uint32_t priId = priId_;
        unlock();
        lock(*mutex, mode, priId);
    }
    bool intercepted() {
        if (intercepted_) return true;
        assert(mutex_);
        LockData64 ld = mutex_->atomicLoad();
        return interceptedDetail(ld);
    }
    bool unchanged() {
        assert(mutex_);
        LockData64 ld = mutex_->atomicLoad();
        // If protected by myself, uVersion may have been incremented.
        return !ld.isProtected() && uVersion_ == ld.uVersion;
    }
    bool protect() {
        assert(mode_ == Mode::X);
        if (protected_) return true;
        for (;;) {
            assert(mutex_);
            LockData64 ld0 = mutex_->atomicLoad();
            if (interceptedDetail(ld0)) return false;
            LockData64 ld1 = ld0;
            assert(ld1.writeReserve == 1);
            assert(ld1.writeProtect == 0);
            ld1.writeReserve = 0;
            ld1.writeProtect = 1;
            if (mutex_->compareAndSwap(ld0, ld1)) {
                protected_ = true;
                return true;
            }
        }
    }
    bool upgrade() {
        assert(mutex_);
        assert(mode_ == Mode::S);
        LockData64 ld0 = mutex_->atomicLoad();
        if (!isNotIntercepted(ld0)) return false;
        while (ld0.priId == priId_) {
            // Try to intercept
            LockData64 ld1;
            ld1.intercept(ld0, Mode::X, priId_);
            if (mutex_->compareAndSwap(ld0, ld1)) {
                mode_ = Mode::X;
                // Other fields do not change.
                return true;
            }
        }
        // Try to relock and check version unchanged.
        Mutex *mutex = mutex_;
        uint32_t priId = priId_;
        uint64_t uVersion = uVersion_;
        unlock();
        lock(*mutex, Mode::X, priId);
        return uVersion == uVersion_;
    }
    void update() {
        if (!protected_) return;
        updated_ = true;
    }
    Mode mode() const { return mode_; }
    void writeFence() const {
        __atomic_thread_fence(__ATOMIC_RELEASE);
    }
    void readFence() const {
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
    }
    /* debug */
    std::string str() const {
        std::stringstream ss;
        ss << cybozu::util::formatString(
            "mutex:%p uVer:%lu iVer:%u priId:%0x mode:%d "
            "protected:%d intercepted:%d updated:%d optimistic:%d"
            , mutex_, uVersion_, iVersion_, priId_
            , mode_, protected_
            , intercepted_, updated_, isOptimisticRead_);
        if (mutex_) {
            ss << " (" << mutex_->atomicLoad().str() << ")";
        }
        return ss.str();
    }
    uintptr_t getMutexId() const {
        return uintptr_t(mutex_);
    }
    uint64_t uVersion() const {
        return uVersion_;
    }
    bool isOptimisticRead() const {
        return isOptimisticRead_;
    }
private:
    void swap(InterceptibleLock64T& rhs) {
        std::swap(mutex_, rhs.mutex_);
        std::swap(bitObjects_, rhs.bitObjects_); // priId_, iVersion_, uVersion_.
        std::swap(mode_, rhs.mode_);
        std::swap(protected_, rhs.protected_);
        std::swap(intercepted_, rhs.intercepted_);
        std::swap(updated_, rhs.updated_);
        std::swap(isOptimisticRead_, rhs.isOptimisticRead_);
    }
    bool isUnlockedOrShared(const LockData64& ld) const {
        if (mode_ == Mode::X) {
            return ld.isUnlocked();
        } else {
            return ld.canReserveRead();
        }
    }
    bool canIntercept(const LockData64& ld) const {
        return priId_ < ld.priId && !ld.isProtected();
    }
    bool canLock(const LockData64& ld) const {
        return isUnlockedOrShared(ld) || canIntercept(ld);
    }
    void prepareReserve(const LockData64& ld0, LockData64& ld1) {
        ld1.reserve(ld0, mode_, priId_);
    }
    void prepareIntercept(const LockData64& ld0, LockData64& ld1) {
        ld1.intercept(ld0, mode_, priId_);
    }
    void prepareProtect(LockData64& ld) {
        assert(ld.writeReserve == 1);
        assert(ld.writeProtect == 0);
        ld.writeReserve = 0;
        ld.writeProtect = 1;
    }
    void waitFor(LockData64& ld) {
#ifdef USE_TRLOCK_PQMCS
        for (;;) {
            PQLock lk(mutex_->pqMutexP.get(), priId_);
            for (;;) {
                if (priId_ > lk.getTopPriorityInWaitQueue()) {
                    break; // Give the PQLock to the prior thread.
                }
#if 0 // like TTAS lock.
                __atomic_thread_fence(__ATOMIC_ACQUIRE);
                ld = mutex_->nonAtomicLoad();
                if (canLock(ld)) {
                    ld = mutex_->atomicLoad();
                    if (canLock(ld)) {
                        // Retry lock reservation again.
                        return;
                    }
                }
#else
                ld = mutex_->atomicLoad();
                if (canLock(ld)) {
                    // Retry lock reservation again.
                    return;
                }
#endif
                _mm_pause();
            }
        }
#else
#if 0 // like TTAS lock.
        for (;;) {
            __atomic_thread_fence(__ATOMIC_ACQUIRE);
            ld = mutex_->nonAtomicLoad();
            if (canLock(ld)) {
                ld = mutex_->atomicLoad();
                if (canLock(ld)) return;
            }
            _mm_pause();
        }
#else
        for (;;) {
            ld = mutex_->atomicLoad();
            if (canLock(ld)) return;
            _mm_pause();
        }
#endif
#endif
    }
    bool interceptedDetail(LockData64& ld0) {
        if (protected_) {
            assert(ld0.iVersion == iVersion_);
            return false;
        }
        if (isNotIntercepted(ld0)) return false;

        /* Try to reserve or intercept again */
        while (ld0.uVersion == uVersion_) {
            LockData64 ld1;
            if (isUnlockedOrShared(ld0)) {
                prepareReserve(ld0, ld1);
            } else if (canIntercept(ld0)) {
                prepareIntercept(ld0, ld1);
            } else {
                break; /* give up */
            }
            if (mutex_->compareAndSwap(ld0, ld1)) {
                ld0 = ld1;
                iVersion_ = ld1.iVersion;
                return false;
            }
        }
        intercepted_ = true;
        return true;
    }
    bool isNotIntercepted(const LockData64& ld) const {
        assert(!protected_);
        if (mode_ == Mode::X) {
            /* It is enough to check priId. */
            return ld.priId == priId_;
        } else {
            /*
             * This may return True-negative in little probability.
             * This condition may be true but
             * it is intercepted really when the intercepted sequence
             * was S -> X -> S -> X -> ... after reserved,
             * iVersion was just wrap-arounded and became the same value again,
             * and uVersion was not changed or wrap-around also.
             * It's very very rare.
             * If such situations occur, the system is valid. See unlock() code.
             */
            return ld.isReadReserved() && ld.iVersion == iVersion_ && ld.uVersion == uVersion_;
        }
    }
};


template <typename PQLock>
class InterceptibleLock64ReaderT
{
private:
    using ILock = InterceptibleLock64T<PQLock>;
    using Mutex = typename ILock::Mutex;
    using Mode = typename ILock::Mode;
    Mutex *mutex_;
    LockData64 ld_;
public:
    InterceptibleLock64ReaderT() : mutex_(), ld_() {}
    InterceptibleLock64ReaderT(const InterceptibleLock64ReaderT&) = delete;
    InterceptibleLock64ReaderT(InterceptibleLock64ReaderT&& rhs) : InterceptibleLock64ReaderT() {
        swap(rhs);
    }
    InterceptibleLock64ReaderT& operator=(const InterceptibleLock64ReaderT&) = delete;
    InterceptibleLock64ReaderT& operator=(InterceptibleLock64ReaderT&& rhs) {
        swap(rhs);
        return *this;
    }
    void prepare(Mutex &mutex) {
        mutex_ = &mutex;
        for (;;) {
            ld_ = mutex_->atomicLoad();
            if (!ld_.isProtected()) break;
            _mm_pause();
        }
    }
    bool verifyAll() const {
        assert(mutex_);
        const LockData64 ld = mutex_->atomicLoad();
        return !ld.isProtected() && ld_.uVersion == ld.uVersion;
    }
    bool verifyVersion() const {
        assert(mutex_);
        const LockData64 ld = mutex_->atomicLoad();
        return ld_.uVersion == ld.uVersion;
    }
    void readFence() const {
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
    }
    uintptr_t getMutexId() const {
        return uintptr_t(mutex_);
    }
    uint64_t uVersion() const {
        return ld_.uVersion;
    }

private:
    void swap(InterceptibleLock64ReaderT& rhs) {
        std::swap(mutex_, rhs.mutex_);
        std::swap(ld_, rhs.ld_);
    }
};



template <typename PQLock>
class ILockSet
{
public:
    using ILock = InterceptibleLock64T<PQLock>;
    using Reader = InterceptibleLock64ReaderT<PQLock>;
    using Mutex = typename ILock::Mutex;
    using Mode = typename ILock::Mode;

    using VecL = std::vector<ILock>;

    // key: mutex addr, value: index in the lock vector.
#if 0
    using Map = std::unordered_map<uintptr_t, size_t>;
#else
    using Map = std::unordered_map<
        uintptr_t, size_t,
        std::hash<uintptr_t>,
        std::equal_to<uintptr_t>,
        LowOverheadAllocatorT<std::pair<const uintptr_t, size_t> > >;
#endif

    VecL vecL_;
    Map map_;
    uint32_t priId_;

public:
    // You must set priId before read/write.
    void setPriorityId(uint32_t priId) {
        priId_ = priId;
    }
    bool optimisticRead(Mutex& mutex) {
        const uintptr_t key = uintptr_t(&mutex);
        typename VecL::iterator it = findInVec(key);
        if (it != vecL_.end()) {
            // read local version.
            return true;
        }
        vecL_.emplace_back();
        ILock& lk = vecL_.back();
        for (;;) {
            lk.prepareOptimisticRead(mutex);
            // read shared data.
            lk.readFence();
            if (lk.unchanged()) break;
        }
        return true;
    }
    bool pessimisticRead(Mutex& mutex) {
        const uintptr_t key = uintptr_t(&mutex);
        typename VecL::iterator it0 = findInVec(key);

        if (it0 == vecL_.end()) {
            vecL_.emplace_back(mutex, Mode::S, priId_);
            ILock& lk = vecL_.back();
            for (;;) {
                // read shared version.
                if (lk.unchanged()) return true;
                lk.unlock();
                lk.lock(mutex, Mode::S, priId_);
            }
        } else {
            ILock& lk = *it0;
            if (lk.isOptimisticRead()) {
                const uint64_t uVersion = lk.uVersion();
                lk.unlock();
                lk.lock(mutex, Mode::S, priId_);
#if 0
                ::printf("%p mutex:%p optimistic-->read_reserve\n"
                         , this, &mutex); // QQQQQ
#endif
                if (lk.uVersion() != uVersion) return false;
            }
            // read local version.
            return true;
        }
    }
    bool write(Mutex& mutex) {
        const uintptr_t key = uintptr_t(&mutex);
        typename VecL::iterator it0 = findInVec(key);
        if (it0 == vecL_.end()) {
            vecL_.emplace_back(mutex, Mode::X, priId_);
            // write local version;
            return true;
        }
        ILock& lk = *it0;
        if (lk.isOptimisticRead()) {
            const uint64_t uVersion = lk.uVersion();
            lk.unlock();
            lk.lock(mutex, Mode::X, priId_);
            if (lk.uVersion() != uVersion) return false;
        } else if (lk.mode() == Mode::S) {
            if (!lk.upgrade()) return false;
        }
        // write local version.
        return true;
    }
    bool protect() {
#if 0
        std::sort(vecL_.begin(), vecL_.end());
#endif

        for (ILock& lk : vecL_) {
            if (lk.mode() == Mode::X) {
                if (!lk.protect()) return false;
            }
        }
        // Here is serialization point.
        __atomic_thread_fence(__ATOMIC_ACQ_REL);
        return true;
    }
    bool verify() {
        for (ILock& lk : vecL_) {
            if (lk.mode() == Mode::X) continue;
            if (!lk.unchanged()) return false;
            /* This is S2PL protocol, so we can release read locks here. */
            lk.unlock();
        }
        return true;
    }
    void updateAndUnlock() {
        for (ILock& lk : vecL_) {
            if (lk.mode() != Mode::X) continue;
            lk.update();
            lk.writeFence();
            lk.unlock();
        }
        vecL_.clear();
        map_.clear();
    }
    /**
     * Call this to reuse the object.
     */
    void clear() {
        vecL_.clear();
        map_.clear();
    }
    bool isEmpty() const {
        return vecL_.empty();
    }

    // debug
    void print() const {
        ::printf("%p BEGIN\n", this);
        for (const ILock& lk : vecL_) {
            ::printf("%p %s\n", this, lk.str().c_str());
        }
        ::printf("%p END\n", this);
        ::fflush(::stdout);
    }
private:
    typename VecL::iterator findInVec(uintptr_t key) {
        // 4KiB scan in average.
        const size_t threshold = 4096 * 2 / sizeof(ILock);
        if (vecL_.size() > threshold) {
            // create indexes.
            for (size_t i = map_.size(); i < vecL_.size(); i++) {
                map_[vecL_[i].getMutexId()] = i;
            }
            // use indexes.
            Map::iterator it = map_.find(key);
            if (it == map_.end()) {
                return vecL_.end();
            } else {
                size_t idx = it->second;
                return vecL_.begin() + idx;
            }
        }
        return std::find_if(
            vecL_.begin(), vecL_.end(),
            [&](const ILock& lk) {
                return lk.getMutexId() == key;
            });
    }
};


}} //namespace cybozu::lock
