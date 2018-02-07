#pragma once
/**
 * Lock Interception Concurrency Control (LICC).
 *
 * A improved version of ILockSet of trlock.hpp.
 */
#include "constexpr_util.hpp"
#include "allocator.hpp"
#include "util.hpp"
#include "pqlock.hpp"
#include "arch.hpp"
#include "vector_payload.hpp"
#include "cache_line_size.hpp"


namespace cybozu {
namespace lock {

/**
 * This is very simple epoch generator.
 * This is suitable for benchmark only.
 * because this code does not consider epoch_id overflow or synchronization.
 * Use an instance in each thread independently.
 */
class SimpleEpochGenerator
{
private:
    uint32_t epochId:22;
    uint32_t counter:10;

public:
    SimpleEpochGenerator() : epochId(0), counter(0) {}

    uint32_t get() {
        if (++counter == 0) ++epochId;
        return epochId;
    }
};


const uint32_t MAX_WORKER_ID = GetMaxValue(10);
const uint32_t MAX_EPOCH_ID = GetMaxValue(22);
const uint32_t MAX_ORD_ID = uint32_t(-1);
const uint8_t MAX_READERS = GetMaxValue(7);

// This is for benchmark. The practical version supports wrap-around behavior.
const uint32_t MIN_EPOCH_ID = 0;
const uint32_t RESERVABLE_EPOCHS = MAX_EPOCH_ID - MIN_EPOCH_ID;
//const uint32_t RESERVABLE_EPOCHS = 10;



union OrdIdGen
{
    uint32_t ordId;
    struct {
        // lower bits with little endian.
        uint32_t workerId:10; // MAX_WORKER_ID is reserved.
        uint32_t epochId:22;
    };
};


struct ILockData
{
    /*
     * 64bit data in total.
     */
    union {
        uint32_t ordId;
        struct {
            uint32_t workerId:10;
            uint32_t epochId:22;
        };
    };
    uint8_t readers:7;
    uint8_t protected_:1;
    uint8_t iVer;
    uint16_t uVer;

    ILockData() {}
    ILockData(uint64_t x) {
        ::memcpy(this, &x, sizeof(*this));
    }
    operator uint64_t() const {
        uint64_t x;
        ::memcpy(&x, this, sizeof(x));
        return x;
    }
    void init() {
        ordId = MAX_ORD_ID;
        readers = 0;
        protected_ = 0;
        iVer = 0;
        uVer = 0;
    }
    std::string str() const {
        return cybozu::util::formatString(
            "ord:%x worker:%x epoch:%x readers:%u protected:%u iver:%u uver:%u"
            , ordId, workerId, epochId, readers, protected_, iVer, uVer);
    }
};


template <typename PQLock>
struct IMutex
{
    alignas(8)
    uint64_t obj;
#if 0
    typename PQLock::Mutex pqMutex;
#else
    std::unique_ptr<typename PQLock::Mutex> pqMutexPtr;
#endif

    IMutex() {
        ILockData ld;
        ld.init();
        atomicStore(ld);
#if 1
        if constexpr (!std::is_same<PQLock, PQNoneLock>::value) {
            pqMutexPtr.reset(new typename PQLock::Mutex());
        }
#endif
    }
    bool compareAndSwap(ILockData &before, const ILockData& after,
                        int mode0 = __ATOMIC_ACQ_REL, int mode1 = __ATOMIC_ACQUIRE) {
#if 1
        return __atomic_compare_exchange(
            &obj, (uint64_t *)&before, (uint64_t *)&after, false, mode0, mode1);
#else // debug code.
        ILockData before0 = before;
        bool ret = __atomic_compare_exchange(
            &obj, (uint64_t *)&before, (uint64_t *)&after, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
        if (ret) {
            ::printf("CAS_OK %p %s\n"
                     "       %p %s\n"
                     , this, before.str().c_str(), this, after.str().c_str());
        } else {
            ::printf("CAS_NG %p %s\n"
                     "       %p %s\n"
                     "       %p %s\n"
                     , this, before0.str().c_str()
                     , this, before.str().c_str()
                     , this, after.str().c_str());
        }
        return ret;
#endif
    }
    bool compareAndSwapBegin(ILockData &before, const ILockData& after) {
        return compareAndSwap(before, after, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
    }
    bool compareAndSwapMid(ILockData &before, const ILockData& after) {
        return compareAndSwap(before, after, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
    }
    bool compareAndSwapEnd(ILockData &before, const ILockData& after) {
        return compareAndSwap(before, after, __ATOMIC_RELEASE, __ATOMIC_RELAXED);
    }

    ILockData atomicLoad(int mode = __ATOMIC_RELAXED) const {
#if 1
        return __atomic_load_n(&obj, mode);
#else // debug code.
        thread_local ILockData prev;
        ILockData ret = __atomic_load_n(&obj, mode);
        if (prev != ret) {
            ::printf("LOAD   %p %s\n", this, ret.str().c_str());
            prev = ret;
        }
        return ret;
#endif
    }
    void atomicStore(const ILockData& after) {
        __atomic_store_n(&obj, after, __ATOMIC_RELAXED);
#if 1
#else // debug code.
        ::printf("STORE  %p %s\n", this, after.str().c_str());
#endif
    }
};


/**
 * State transition:
 *   EMPTY --> INVISIBLE_READ or RESERVED_READ or BLIND_WRITE or (WRITE)
 *   INVISIBLE_READ --> RESERVED_READ or WRITE or EMPTY
 *   RESERVED_READ --> WRITE or EMPTY
 *   WRITE --> EMPTY
 *   BLIND_WRITE --> WRITE or EMPTY
 */
enum class AccessMode : uint8_t
{
    EMPTY = 0,
    INVISIBLE_READ = 1,
    RESERVED_READ = 2,
    WRITE = 3,
    BLIND_WRITE = 4,
    MAX = 5,
};


struct ILockState
{
    uint16_t uVer;
    uint8_t iVer;
    AccessMode mode;
    bool protected_;
    bool updated;

    void init() {
        // uVer = 0;
        // iVer = 0;
        mode = AccessMode::EMPTY;
        protected_ = false;
        updated = false;
    }
    std::string str() const {
        return cybozu::util::formatString(
            "STATE mode:%u protected:%u updated:%u iver:%u uver:%u"
            , mode, protected_, updated, iVer, uVer);
    }
};


template <typename PQLock>
class ILock
{
private:
    using Mutex = IMutex<PQLock>;

    Mutex *mutex_;
    union {
        uint32_t ordId_;
        struct {
            uint32_t workerId_:10;
            uint32_t epochId_:22;
        };
    };
    ILockState state_;

public:
    ILock(Mutex &mutex, uint32_t ordId) : mutex_(&mutex), ordId_(ordId), state_() {
        state_.init();
    }
    ~ILock() {
        unlock();
    }

    ILock(const ILock&) = delete;
    ILock(ILock&&) = default;
    ILock& operator=(const ILock&) = delete;
    ILock& operator=(ILock&&) = default;
    bool operator<(const ILock& rhs) const {
        return getMutexId() < rhs.getMutexId();
    }

    void initInvisibleRead() {
        state_.mode = AccessMode::INVISIBLE_READ;
    }
    void waitForInvisibleRead() {
        ILockData ld;
        for (;;) {
            // with read fence.
            ld = mutex_->atomicLoad(__ATOMIC_ACQUIRE);
            if (!ld.protected_) {
                state_.uVer = ld.uVer;
                break;
            }
            _mm_pause();
        }
    }

private:
    static const uint PREPARE_READ = 0;
    static const uint PREPARE_UNLOCK_READ = 1;
    static const uint PREPARE_WRITE = 2;
    static const uint PREPARE_UNLOCK_WRITE = 3;
    static const uint PREPARE_MAX = 4;

    template <uint type>
    void waitFor(ILockData& ld0) {
        ILockData ld1;
        ILockState st0 = state_;
        ILockState st1;

        for (;;) {
#if 0
            PQLock lk(&mutex_->pqMutex, ordId_);
#else
            PQLock lk(mutex_->pqMutexPtr.get(), ordId_);
#endif
            for (;;) {
                if (ordId_ > lk.getTopPriorityInWaitQueue()) {
                    break; // Give the PQLock to the prior thread.
                }
                ld0 = mutex_->atomicLoad();
                bool canReserve;
                if (type == PREPARE_READ) {
                    canReserve = prepareReadReserveOrIntercept<0>(ld0, st0, ld1, st1);
                } else if (type == PREPARE_UNLOCK_READ) {
                    canReserve = prepareUnlockAndReadReserveOrIntercept<1>(ld0, st0, ld1, st1);
                } else if (type == PREPARE_WRITE) {
                    canReserve = prepareWriteReserveOrIntercept<0>(ld0, st0, ld1, st1);
                } else if (type == PREPARE_UNLOCK_WRITE) {
                    canReserve = prepareUnlockAndWriteReserveOrIntercept<1>(ld0, st0, ld1, st1);
                }
                static_assert(type < PREPARE_MAX, "type is not supported.");
                if (canReserve) {
                    return; // Retry lock reservation.
                }
                _mm_pause();
            }
        }
    }
public:
    /*
     * State change: EMPTY --> RESERVED_READ.
     */
    void readAndReadReserve(const void *shared, void *local, size_t size) {
        unused(shared); unused(local); unused(size);
        ILockData ld0 = mutex_->atomicLoad();
        ILockState st0 = state_;
        assert(st0.mode == AccessMode::EMPTY);
        for (;;) {
            ILockData ld1;
            ILockState st1;
            if (!prepareReadReserveOrIntercept<0>(ld0, st0, ld1, st1)) {
                waitFor<PREPARE_READ>(ld0);
                continue;
            }
            __atomic_thread_fence(__ATOMIC_ACQUIRE);
            // read shared memory.
#ifndef NO_PAYLOAD
            ::memcpy(local, shared, size);
#endif
            if (mutex_->compareAndSwapBegin(ld0, ld1)) {
                state_ = st1;
                return;
            }
            // continue;
        }
    }
    /*
     * This is called for read-modify-write.
     * This is more efficient than readAndReserveRead() and then upgrade().
     *
     * State change: EMPTY --> READ_MODIYF_WRITE.
     */
    void readAndWriteReserve(const void *shared, void *local, size_t size) {
        unused(shared); unused(local); unused(size);
        ILockData ld0 = mutex_->atomicLoad();
        ILockState st0 = state_;
        assert(st0.mode == AccessMode::EMPTY);
        for (;;) {
            ILockData ld1;
            ILockState st1;
            if (!prepareWriteReserveOrIntercept<0>(ld0, st0, ld1, st1)) {
                waitFor<PREPARE_WRITE>(ld0);
                continue;
            }
            __atomic_thread_fence(__ATOMIC_ACQUIRE);
            // read shared memory.
#ifndef NO_PAYLOAD
            ::memcpy(local, shared, size);
#endif
            if (mutex_->compareAndSwapBegin(ld0, ld1)) {
                state_ = st1;
                return;
            }
            // continue;
        }
    }
    /*
     * Just change the internal state.
     * Reservation and protection will be deferred.
     * blindWriteReserve() will be called at the pre-commit phase
     * in blindWriteReserveAll().
     */
    void blindWrite() {
        assert(state_.mode == AccessMode::EMPTY);
        state_.mode = AccessMode::BLIND_WRITE;
    }

    /*
     * State change: BLIND_WRITE --> WRITE.
     * This is called by the blindWriteReserveAll().
     */
    void blindWriteReserve() {
        ILockData ld0 = mutex_->atomicLoad();
        ILockState st0 = state_;
        assert(st0.mode == AccessMode::BLIND_WRITE);
        for (;;) {
            ILockData ld1;
            ILockState st1;
            if (!prepareWriteReserveOrIntercept<0>(ld0, st0, ld1, st1)) {
                waitFor<PREPARE_WRITE>(ld0);
                continue;
            }
            if (mutex_->compareAndSwapBegin(ld0, ld1)) {
                state_ = st1;
                return;
            }
            // continue;
        }
    }
    /**
     * This is called only when the state change INVISIBLE_READ --> RESERVED_READ.
     * You need not wait for unprotected here.
     */
    void unlockAndReadReserve() {
        ILockData ld0 = mutex_->atomicLoad();
        ILockState st0 = state_;
        assert(st0.mode == AccessMode::INVISIBLE_READ);
        for (;;) {
            ILockData ld1;
            ILockState st1;
            if (!prepareUnlockAndReadReserveOrIntercept<1>(ld0, st0, ld1, st1)) {
                waitFor<PREPARE_UNLOCK_READ>(ld0);
                continue;
            }
            if (mutex_->compareAndSwapMid(ld0, ld1)) {
                state_ = st0;
                return;
            }
            // continue;
        }
    }

    /**
     * You need not wait for unprotected here.
     */
    void unlockAndWriteReserve() {
        ILockData ld0 = mutex_->atomicLoad();
        ILockState st0 = state_;
        assert(st0.mode != AccessMode::EMPTY);
        for (;;) {
            ILockData ld1;
            ILockState st1;
            if (!prepareUnlockAndWriteReserveOrIntercept<1>(ld0, st0, ld1, st1)) {
                waitFor<PREPARE_UNLOCK_WRITE>(ld0);
                continue;
            }
            if (mutex_->compareAndSwapMid(ld0, ld1)) {
                state_ = st1;
                return;
            }
        }
    }

    bool upgrade() {
        assert(state_.mode == AccessMode::INVISIBLE_READ || state_.mode == AccessMode::RESERVED_READ);
        const ILockData ld0 = mutex_->atomicLoad();
        if (ld0.uVer != state_.uVer) return false;
        unlockAndWriteReserve();
        return ld0.uVer == state_.uVer;
    }

    bool protect() {
        assert(state_.mode == AccessMode::WRITE);
        ILockData ld0 = mutex_->atomicLoad();
        ILockState st0 = state_;
        for (;;) {
            ILockData ld1;
            ILockState st1;
            if (!prepareProtect(ld0, st0, ld1, st1)) return false;
            if (mutex_->compareAndSwapMid(ld0, ld1)) {
                state_ = st1;
                return true;
            }
        }
    }

    void unlock() {
        if (!mutex_) return;
        ILockData ld0 = mutex_->atomicLoad();
        ILockState st0 = state_;
        for (;;) {
            ILockData ld1;
            ILockState st1;
            bool doCas = prepareUnlock(ld0, st0, ld1, st1);
            if (doCas && !mutex_->compareAndSwapEnd(ld0, ld1)) continue;
            state_ = st1;
            return; // mutex_ is not cleared.
        }
    }

    bool unchanged() {
        assert(mutex_);
        const ILockData ld = mutex_->atomicLoad();
        return !ld.protected_ && state_.uVer == ld.uVer;
    }
    void update() {
        assert(state_.protected_);
        state_.updated = true;
    }
    AccessMode mode() const { return state_.mode; }
    uintptr_t getMutexId() const { return uintptr_t(mutex_); }
    const ILockState& state() const { return state_; }

private:
    /**
     * RETURN:
     *   true: reserve or intercept is capable.
     */
    template <bool allowProtected>
    bool prepareReadReserveOrIntercept(
        const ILockData& ld0, const ILockState& st0, ILockData& ld1, ILockState& st1) {
        //::printf("prepareReadReserveOrIntercept\n"); // debug code
        if (!allowProtected && ld0.protected_) return false;

        ld1 = ld0;
        st1 = st0;
        assert(st0.mode == AccessMode::EMPTY);
        st1.mode = AccessMode::RESERVED_READ;

        bool c0 = ld0.ordId == MAX_ORD_ID && ld0.readers == 0; // not reserved.
        bool c1 = 0 < ld0.readers && ld0.readers < MAX_READERS; // can be shared.
        bool c2 = epochId_ < MIN_EPOCH_ID + RESERVABLE_EPOCHS; // for wait-free property.
        if ((c0 || c1) && c2) {
            // reserve
            ld1.readers++;
            if (ordId_ < ld0.ordId) ld1.ordId = ordId_;
            st1.iVer = ld1.iVer;
            st1.uVer = ld1.uVer;
            return true;
        }
        if (ordId_ < ld0.ordId) {
            // intercept
            ld1.readers = 1;
            ld1.ordId = ordId_;
            ld1.iVer++;
            st1.iVer = ld1.iVer;
            st1.uVer = ld1.uVer;
            return true;
        }
        return false;
    }
    /**
     * RETURN:
     *   true: reserve or intercept is capable.
     */
    template <bool allowProtected>
    bool prepareWriteReserveOrIntercept(
        const ILockData& ld0, const ILockState& st0, ILockData& ld1, ILockState& st1) {
        //::printf("prepareWriteReserveOrIntercept\n"); // debug code
        if (!allowProtected && ld0.protected_) return false;
        ld1 = ld0;
        st1 = st0;
        assert(st0.mode == AccessMode::EMPTY || st0.mode == AccessMode::BLIND_WRITE);
        st1.mode = AccessMode::WRITE;
#if 0
        if (ld0.ordId == MAX_ORD_ID && ld0.readers == 0) {
            // reserve
            ld1.ordId = ordId_;
            st1.uVer = ld0.uVer;
            return true;
        }
        if (ordId_ < ld0.ordId) {
            // intercept
            ld1.readers = 0;
            ld1.ordId = ordId_;
            st1.uVer = ld0.uVer;
            return true;
        }
#else
        bool c0 = ld0.ordId == MAX_ORD_ID && ld0.readers == 0;
        bool c1 = epochId_ < MIN_EPOCH_ID + RESERVABLE_EPOCHS; // for wait-free property.
        if ((c0 && c1) || ordId_ < ld0.ordId) {
            ld1.readers = 0;
            ld1.ordId = ordId_;
            st1.uVer = ld0.uVer;
            return true;
        }
#endif
        return false;
    }
    /**
     * RETURN:
     *   true/false: CAS is required / not required.
     */
    bool prepareUnlock(const ILockData& ld0, const ILockState& st0, ILockData& ld1, ILockState& st1) {
        //::printf("prepareUnlock %p\n", this); // debug code
        ld1 = ld0;
        st1 = st0;
        if (st0.mode == AccessMode::EMPTY) {
            return false;
        }
        st1.mode = AccessMode::EMPTY;
        if (st0.mode == AccessMode::INVISIBLE_READ ||
            st0.mode == AccessMode::BLIND_WRITE) {
            return false;
        }
        if (st0.mode == AccessMode::RESERVED_READ) {
            if (ld0.readers > 0 && ld0.iVer == st0.iVer) {
                // self-reserved
                ld1.readers--;
                if (ld0.ordId == ordId_ || ld1.readers == 0) {
                    ld1.ordId = MAX_ORD_ID;
                }
                return true;
            }
            return false; // Intercepted so CAS is not required.
        }
        //::printf("st0 %s\n", st0.str().c_str()); // debug code

        assert(st0.mode == AccessMode::WRITE);
        if (st0.protected_) {
            // Another transaction may have reservation,
            // so do not touch the related fields.
            ld1.protected_ = 0;
            if (st0.updated) {
                ld1.uVer++;
            }
            st1.protected_ = false;
            st1.updated = false;
            return true;
        }
        if (ld0.ordId == ordId_) {
            // self-reserved
            ld1.ordId = MAX_ORD_ID;
            return true;
        }
        return false; // Intercepted so CAS is not required.
    }
    template <bool allowProtected>
    bool prepareUnlockAndReadReserveOrIntercept(
        const ILockData& ld0, const ILockState& st0, ILockData& ld1, ILockState& st1) {
        ILockData ld2;
        ILockState st2;
        prepareUnlock(ld0, st0, ld2, st2);
        return prepareReadReserveOrIntercept<allowProtected>(ld2, st2, ld1, st1);
    }
    template <bool allowProtected>
    bool prepareUnlockAndWriteReserveOrIntercept(
        const ILockData& ld0, const ILockState& st0, ILockData& ld1, ILockState& st1) {
        ILockData ld2;
        ILockState st2;
        prepareUnlock(ld0, st0, ld2, st2);
        return prepareWriteReserveOrIntercept<allowProtected>(ld2, st2, ld1, st1);
    }
    bool prepareProtect(
        const ILockData& ld0, const ILockState& st0, ILockData& ld1, ILockState& st1) {
        //::printf("prepareProtect\n"); // debug code.
        assert(st0.mode == AccessMode::WRITE);
        if (ordId_ > ld0.ordId) {
            // Intercepted by a prior transaction.
            return false;
        }
        if (ordId_ < ld0.ordId) {
            // Intercepted but we can re-intercept.
            if (!prepareUnlockAndWriteReserveOrIntercept<0>(ld0, st0, ld1, st1)) {
                // protected by other transaction.
                // We can not wait for other threads in pre-commit phase.
                return false;
            }
        } else {
            // Reserved.
            ld1 = ld0;
            st1 = st0;
        }
        ld1.protected_ = 1;
        ld1.ordId = MAX_ORD_ID;
        st1.protected_ = true;
        return true;
    }
};


template <typename PQLock>
class ILockSet
{
public:
    using Lock = ILock<PQLock>;

    //using Vec = std::vector<Lock>;
    using Vec = VectorWithPayload<Lock>;
#if 1
    using Map = SingleThreadUnorderedMap<uintptr_t, size_t>;
#else
    using Map = std::unordered_map<uintptr_t, size_t>;
#endif

    using Mutex = IMutex<PQLock>;

private:
    // each item consists of (1) Lock object, (2) pointer to shared value, and (3) local value.
    Vec vec_;

    // key: mutex pointer.  value: index in vec_.
    Map map_;

    uint32_t ordId_;
    size_t valueSize_;

public:
    struct Payload {
        void *sharedValue;
        uint8_t localValue[0];

        void loadLocalValue(void *value, size_t size) {
            ::memcpy(value, localValue, size);
        }
        void storeLocalValue(const void *value, size_t size) {
            ::memcpy(localValue, value, size);
        }
        void loadSharedValue(size_t size) {
            ::memcpy(localValue, sharedValue, size);
        }
        void storeSharedValue(size_t size) {
            ::memcpy(sharedValue, localValue, size);
        }
    };
    size_t payloadSize() const {
        return sizeof(uintptr_t) + valueSize_;
    }

    // You must call this method before read/write operations.
    void init(uint32_t ordId, size_t valueSize) {
        ordId_ = ordId;
        valueSize_ = valueSize;
        vec_.setPayloadSize(payloadSize());
    }
    /**
     * This is invisible read which does not modify the mutex so
     * it can be executed with low overhead.
     * However, this function does not preserve progress guarantee.
     */
    void invisibleRead(Mutex& mutex, void *sharedValue, void *value) {
        unused(sharedValue); unused(value);
        const uintptr_t key = uintptr_t(&mutex);
        typename Vec::iterator it = findInVec(key);
        if (it != vec_.end()) {
            // read local version.
#ifndef NO_PAYLOAD
            ((Payload *)it->payload)->loadLocalValue(value, valueSize_);
#endif
            return;
        }
        vec_.emplace_back(mutex, ordId_);

        //Lock& lk = vec_.back();
        Lock& lk = vec_.back().value;
#ifndef NO_PAYLOAD
        Payload *payload = (Payload *)vec_.back().payload;
        payload->sharedValue = sharedValue;
#endif

        lk.initInvisibleRead();
        for (;;) {
            lk.waitForInvisibleRead();
            // read shared version.
#ifndef NO_PAYLOAD
            payload->loadSharedValue(valueSize_);
#endif
            __atomic_thread_fence(__ATOMIC_ACQUIRE);
            if (lk.unchanged()) break;
        }
#ifndef NO_PAYLOAD
        payload->loadLocalValue(value, valueSize_);
#endif
    }
    /**
     * You should use this function to read records
     * to preserve progress guarantee.
     */
    bool reservedRead(Mutex& mutex, void *sharedValue, void *value) {
        unused(sharedValue); unused(value);
        const uintptr_t key = uintptr_t(&mutex);
        typename Vec::iterator it0 = findInVec(key);
        if (it0 == vec_.end()) {
            vec_.emplace_back(mutex, ordId_);
            //Lock& lk = vec_.back();
            Lock& lk = vec_.back().value;
#ifndef NO_PAYLOAD
            Payload *payload = (Payload *)vec_.back().payload;
            payload->sharedValue = sharedValue;
            lk.readAndReadReserve(payload->sharedValue, payload->localValue, valueSize_);
            payload->loadLocalValue(value, valueSize_);
#else
            lk.readAndReadReserve(nullptr, nullptr, 0);
#endif
            return true;
        }
        Lock& lk = it0->value;
        if (lk.mode() == AccessMode::INVISIBLE_READ) {
            const uint32_t uVer = lk.state().uVer;
            lk.unlockAndReadReserve();
            if (lk.state().uVer != uVer) return false;
        }
        // read local version.
#ifndef NO_PAYLOAD
        ((Payload *)it0->payload)->loadLocalValue(value, valueSize_);
#endif
        return true;
    }
    /**
     * You should use this function to write records.
     */
    bool write(Mutex& mutex, void *sharedValue, void *value) {
        unused(sharedValue); unused(value);
        const uintptr_t key = uintptr_t(&mutex);
        typename Vec::iterator it0 = findInVec(key);
        if (it0 == vec_.end()) {
            vec_.emplace_back(mutex, ordId_);
            //Lock& lk = vec_.back();
            Lock& lk = vec_.back().value;
            lk.blindWrite();

            // write local version.
#ifndef NO_PAYLOAD
            Payload *payload = (Payload *)vec_.back().payload;
            payload->sharedValue = sharedValue;
            payload->storeLocalValue(value, valueSize_);
#endif
            return true;
        }
        Lock& lk = it0->value;
        if (lk.mode() == AccessMode::INVISIBLE_READ || lk.mode() == AccessMode::RESERVED_READ) {
            if (!lk.upgrade()) return false;
        }
        // write local version.
#ifndef NO_PAYLOAD
        ((Payload *)it0->payload)->storeLocalValue(value, valueSize_);
#endif
        return true;
    }
    /*
     * You call this to read modify write in a transaction.
     * If you know the transaction will call reservedRead() then
     * write() which will call upgrade() internally,
     * it is more efficient to call readForUpdate() first.
     *
     * (1) reservedRead(),  write(), write(), ...
     * (2) readForUpdate(), write(), write(), ...
     *
     * (2) is more efficient than (1).
     */
    bool readForUpdate(Mutex& mutex, void *sharedValue, void *value) {
        unused(sharedValue); unused(value);
        const uintptr_t key = uintptr_t(&mutex);
        typename Vec::iterator it0 = findInVec(key);
        if (it0 == vec_.end()) {
            vec_.emplace_back(mutex, ordId_);
            //Lock& lk = vec_.back();
            Lock& lk = vec_.back().value;
#ifndef NO_PAYLOAD
            Payload *payload = (Payload *)vec_.back().payload;
            payload->sharedValue = sharedValue;
            lk.readAndWriteReserve(payload->sharedValue, payload->localValue, valueSize_);
            payload->loadLocalValue(value, valueSize_); // read local copy.
#else
            lk.readAndWriteReserve(nullptr, nullptr, 0);
#endif
            return true;
        }
        Lock& lk = it0->value;
        if (lk.mode() == AccessMode::INVISIBLE_READ || lk.mode() == AccessMode::RESERVED_READ) {
            if (!lk.upgrade()) return false;
        }
#ifndef NO_PAYLOAD
        Payload *payload = (Payload *)it0->payload;
        payload->loadLocalValue(value, valueSize_); // read local copy.
#endif
        return true;
    }

    /*
     * Pre-commit phase:
     * You must call blindWriteReserveAll(), protectAll(), verifyAndUnlock(), updateAndUnlock()
     * in sequence.
     * The point between protectAll() and verifyAndUnlock() is the serialization point.
     * The point between verifyAndUnlock() and updateAndUnlock() is the strictness point.
     */

    void blindWriteReserveAll() {
        for (auto& item : vec_) {
            Lock& lk = item.value;
            if (lk.mode() == AccessMode::BLIND_WRITE) {
                lk.blindWriteReserve();
            }
        }
    }
    bool protectAll() {
#if 0
        std::sort(vec_.begin(), vec_.end());
        // map_ is invalidated here. Do not use it.
#endif
        for (auto& item : vec_) {
            Lock& lk = item.value;
            assert(lk.mode() != AccessMode::BLIND_WRITE);
            if (lk.mode() == AccessMode::WRITE) {
                if (!lk.protect()) return false;
            }
        }
        // Here is serialization point.
        __atomic_thread_fence(__ATOMIC_ACQ_REL);
        return true;
    }
    bool verifyAndUnlock() {
        for (auto& item : vec_) {
            Lock& lk = item.value;
            if (lk.mode() == AccessMode::INVISIBLE_READ ||
                lk.mode() == AccessMode::RESERVED_READ) {
                if (!lk.unchanged()) return false;
                // S2PL allow unlocking of read locks.
                lk.unlock();
            }
        }
        return true;
    }
    void updateAndUnlock() {
        for (auto& item : vec_) {
            Lock& lk = item.value;
            assert(lk.mode() != AccessMode::BLIND_WRITE);
            if (lk.mode() == AccessMode::WRITE) {
                lk.update();
#ifndef NO_PAYLOAD
                Payload *payload = (Payload *)item.payload;
                payload->storeSharedValue(valueSize_);
#endif
                lk.unlock();
            }
        }
        clear();
    }
    void clear() {
        map_.clear();
        vec_.clear();
    }
    bool isEmpty() const { return vec_.empty(); }

private:
    typename Vec::iterator findInVec(uintptr_t key) {
        // at most 4KiB scan.
        const size_t threshold = 4096 / (sizeof(Lock) + payloadSize());
        if (vec_.size() > threshold) {
            // create indexes.
            for (size_t i = map_.size(); i < vec_.size(); i++) {
                map_[vec_[i].value.getMutexId()] = i;
            }
            // use indexes.
            Map::iterator it = map_.find(key);
            if (it == map_.end()) {
                return vec_.end();
            } else {
                size_t idx = it->second;
                return vec_.begin() + idx;
            }
        }
        return std::find_if(
            vec_.begin(), vec_.end(),
            [&](const auto& item) {
                return item.value.getMutexId() == key;
            });
    }
};


}} //namespace cybozu::lock
