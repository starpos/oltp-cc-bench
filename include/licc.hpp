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


namespace cybozu {
namespace lock {

namespace licc_local {

constexpr size_t CACHE_LINE_SIZE = 64;

} // namespace licc_local


/**
 * This is very simple epoch generator.
 * This is suitable for benchmark only.
 * because this code does not consider epoch_id overflow or synchronization.
 * Use an instance in each thread independently.
 */
class EpochGenerator
{
private:
    uint32_t epochId:22;
    uint32_t counter:10;

public:
    EpochGenerator() : epochId(0), counter(0) {}

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
const uint32_t READABLE_EPOCHS = MAX_EPOCH_ID - MIN_EPOCH_ID;



union OrdIdGen
{
    uint32_t ordId;
    struct {
        // lower bits with little endian.
        uint32_t workerId:10; // MAX_WORKER_ID is reserved.
        uint32_t epochId:22;
    };
};


uint32_t genOrdId(uint32_t workerId, uint32_t epochId)
{
    assert(workerId < MAX_WORKER_ID);
    assert(epochId <= MAX_EPOCH_ID);

    OrdIdGen gen;
    gen.workerId = workerId;
    gen.epochId = epochId;
    return gen.ordId;
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
#ifdef MUTEX_ON_CACHELINE
    alignas(licc_local::CACHE_LINE_SIZE)
#else
    alignas(8)
#endif
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
    bool compareAndSwap(ILockData &before, const ILockData& after) {
#if 1
        return __atomic_compare_exchange(
            &obj, (uint64_t *)&before, (uint64_t *)&after, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
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


enum class AccessMode : uint8_t
{
    EMPTY = 0, INVISIBLE_READ = 1, RESERVED_READ = 2, WRITE = 3, MAX = 4,
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
                if constexpr (type == PREPARE_READ) {
                    canReserve = prepareReadReserveOrIntercept(ld0, st0, ld1, st1);
                } else if constexpr (type == PREPARE_UNLOCK_READ) {
                    canReserve = prepareUnlockAndReadReserveOrIntercept(ld0, st0, ld1, st1);
                } else if constexpr (type == PREPARE_WRITE) {
                    canReserve = prepareWriteReserveOrIntercept(ld0, st0, ld1, st1);
                } else if constexpr (type == PREPARE_UNLOCK_WRITE) {
                    canReserve = prepareUnlockAndWriteReserveOrIntercept(ld0, st0, ld1, st1);
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

    void readReserve() {
        ILockData ld0 = mutex_->atomicLoad();
        ILockState st0 = state_;
        assert(st0.mode == AccessMode::EMPTY);
        bool changed = false;
        for (;;) {
            if (changed && ld0.readers > 0 && ld0.iVer == st0.iVer) {
                // self-reserved.
                if (!ld0.protected_) break;
                ld0 = mutex_->atomicLoad();
                _mm_pause();
                continue;
            }
            // Consider the case that mode is reserved but intercpeted after that.
            changed = false;
            st0.mode = AccessMode::EMPTY;
            ILockData ld1;
            ILockState st1;
            if (!prepareReadReserveOrIntercept(ld0, st0, ld1, st1)) {
#if 1
                waitFor<PREPARE_READ>(ld0);
#else
                ld0 = mutex_->atomicLoad();
                _mm_pause();
#endif
                continue;
            }
            if (mutex_->compareAndSwap(ld0, ld1)) {
                changed = true;
                ld0 = ld1;
                st0 = st1;
            }
        }
        assert(st0.mode == AccessMode::RESERVED_READ);
        state_ = st0;
    }
    void unlockAndReadReserve() {
        ILockData ld0 = mutex_->atomicLoad();
        ILockState st0 = state_;
        assert(st0.mode == AccessMode::RESERVED_READ || st0.mode == AccessMode::WRITE);
        bool changed = false;
        for (;;) {
            if (changed && ld0.readers > 0 && ld0.iVer == st0.iVer) {
                // self-reserved.
                if (!ld0.protected_) break;
                ld0 = mutex_->atomicLoad();
                _mm_pause();
                continue;
            }
            changed = false;
            ILockData ld1;
            ILockState st1;
            if (!prepareUnlockAndReadReserveOrIntercept(ld0, st0, ld1, st1)) {
#if 1
                waitFor<PREPARE_UNLOCK_READ>(ld0);
#else
                ld0 = mutex_->atomicLoad();
                _mm_pause();
#endif
                continue;
            }
            if (ld0 == ld1) {
                changed = true;
                st0 = st1;
            } else if (mutex_->compareAndSwap(ld0, ld1)) {
                changed = true;
                ld0 = ld1;
                st0 = st1;
            }
        }
        assert(st0.mode == AccessMode::RESERVED_READ);
        state_ = st0;
        //::printf("unlockAndReadReserve %p %s\n", this, state_.str().c_str()); // debug code
    }
    void writeReserve() {
        ILockData ld0 = mutex_->atomicLoad();
        ILockState st0 = state_;
        assert(st0.mode == AccessMode::EMPTY);
        bool changed = false;
        for (;;) {
            if (changed && ld0.ordId == ordId_) {
                // self-reserved
                if (!ld0.protected_) break;
                ld0 = mutex_->atomicLoad();
                _mm_pause();
                continue;
            }
            // Consider the case that mode is reserved but intercpeted after that.
            changed = false;
            st0.mode = AccessMode::EMPTY;
            ILockData ld1;
            ILockState st1;
            if (!prepareWriteReserveOrIntercept(ld0, st0, ld1, st1)) {
#if 1
                waitFor<PREPARE_WRITE>(ld0);
#else
                ld0 = mutex_->atomicLoad();
                _mm_pause();
#endif
                continue;
            }
            if (mutex_->compareAndSwap(ld0, ld1)) {
                changed = true;
                ld0 = ld1;
                st0 = st1;
            }
        }
        assert(st0.mode == AccessMode::WRITE);
        state_ = st0;
        //::printf("writeReserve %p %s\n", this, state_.str().c_str()); // debug code
    }
    void unlockAndWriteReserve() {
        ILockData ld0 = mutex_->atomicLoad();
        ILockState st0 = state_;
        assert(st0.mode != AccessMode::EMPTY);
        bool changed = false;
        for (;;) {
            if (changed && ld0.ordId == ordId_) {
                // self-reserved.
                if (!ld0.protected_) break;
                ld0 = mutex_->atomicLoad();
                _mm_pause();
                continue;
            }
            changed = false;
            ILockData ld1;
            ILockState st1;
            if (!prepareUnlockAndWriteReserveOrIntercept(ld0, st0, ld1, st1)) {
#if 1
                waitFor<PREPARE_UNLOCK_WRITE>(ld0);
#else
                ld0 = mutex_->atomicLoad();
                _mm_pause();
#endif
                continue;
            }
            if (ld0 == ld1) {
                changed = true;
                st0 = st1;
            } else if (mutex_->compareAndSwap(ld0, ld1)) {
                changed = true;
                ld0 = ld1;
                st0 = st1;
            }
        }
        assert(st0.mode == AccessMode::WRITE);
        state_ = st0;
        // ::printf("unlockAndWriteReserve %p %s\n", this, state_.str().c_str()); // debug code
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
            if (mutex_->compareAndSwap(ld0, ld1)) {
                st0 = st1;
                break;
            }
        }
        state_ = st0;
        return true;
    }

    void unlock() {
        if (!mutex_) return;
        ILockData ld0 = mutex_->atomicLoad();
        ILockState st0 = state_;
        for (;;) {
            ILockData ld1;
            ILockState st1;
            bool doCas = prepareUnlock(ld0, st0, ld1, st1);
            if (doCas && !mutex_->compareAndSwap(ld0, ld1)) continue;
            st0 = st1;
            break;
        }
        state_ = st0;
        // mutex_ is not cleared.
    }

    bool unchanged() {
        assert(mutex_);
        const ILockData ld = mutex_->atomicLoad(__ATOMIC_ACQUIRE); // with read fence.
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
    bool prepareReadReserveOrIntercept(
        const ILockData& ld0, const ILockState& st0, ILockData& ld1, ILockState& st1) {
        //::printf("prepareReadReserveOrIntercept\n"); // debug code
        ld1 = ld0;
        st1 = st0;
        assert(st0.mode == AccessMode::EMPTY);
        st1.mode = AccessMode::RESERVED_READ;

        bool c0 = ld0.ordId == MAX_ORD_ID && ld0.readers == 0; // not reserved.
        bool c1 = 0 < ld0.readers && ld0.readers < MAX_READERS; // can be shared.
        bool c2 = epochId_ < MIN_EPOCH_ID + READABLE_EPOCHS; // for wait-free property.
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
    bool prepareWriteReserveOrIntercept(
        const ILockData& ld0, const ILockState& st0, ILockData& ld1, ILockState& st1) {
        //::printf("prepareWriteReserveOrIntercept\n"); // debug code
        ld1 = ld0;
        st1 = st0;
        assert(st0.mode == AccessMode::EMPTY);
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
        if ((ld0.ordId == MAX_ORD_ID && ld0.readers == 0) || ordId_ < ld0.ordId) {
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
        if (st0.mode == AccessMode::INVISIBLE_READ) {
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
    bool prepareUnlockAndReadReserveOrIntercept(
        const ILockData& ld0, const ILockState& st0, ILockData& ld1, ILockState& st1) {
        ILockData ld2;
        ILockState st2;
        prepareUnlock(ld0, st0, ld2, st2);
        return prepareReadReserveOrIntercept(ld2, st2, ld1, st1);
    }
    bool prepareUnlockAndWriteReserveOrIntercept(
        const ILockData& ld0, const ILockState& st0, ILockData& ld1, ILockState& st1) {
        ILockData ld2;
        ILockState st2;
        prepareUnlock(ld0, st0, ld2, st2);
        return prepareWriteReserveOrIntercept(ld2, st2, ld1, st1);
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
            bool ret = prepareUnlockAndWriteReserveOrIntercept(ld0, st0, ld1, st1);
            unusedVar(ret);
            assert(ret);
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


    using Vec = std::vector<Lock>;
    using Map = std::unordered_map<
        uintptr_t, size_t,
        std::hash<uintptr_t>,
        std::equal_to<uintptr_t>,
        LowOverheadAllocatorT<std::pair<const uintptr_t, size_t> > >;

    using Mutex = IMutex<PQLock>;

private:
    Vec vec_;
    Map map_;
    uint32_t ordId_;

public:
    // You must call this method before read/write operations.
    void init(uint32_t ordId) {
        ordId_ = ordId;
    }
    void invisibleRead(Mutex& mutex) {
        const uintptr_t key = uintptr_t(&mutex);
        typename Vec::iterator it = findInVec(key);
        if (it != vec_.end()) {
            // read local version.
            return;
        }
        vec_.emplace_back(mutex, ordId_);
        Lock& lk = vec_.back();
        lk.initInvisibleRead();
        for (;;) {
            lk.waitForInvisibleRead();
            // read shared version.
            if (lk.unchanged()) break;
        }
    }
    bool reservedRead(Mutex& mutex) {
        const uintptr_t key = uintptr_t(&mutex);
        typename Vec::iterator it0 = findInVec(key);
        if (it0 == vec_.end()) {
            vec_.emplace_back(mutex, ordId_);
            Lock& lk = vec_.back();
            lk.readReserve();
            for (;;) {
                // read shared version.
                if (lk.unchanged()) return true;
                lk.unlockAndReadReserve();
            }
        }
        Lock& lk = *it0;
        if (lk.mode() == AccessMode::INVISIBLE_READ) {
            const uint32_t uVer = lk.state().uVer;
            lk.unlockAndReadReserve();
            if (lk.state().uVer != uVer) return false;
        }
        // read local version.
        return true;
    }
    bool blindWrite(Mutex& mutex) {
        // TODO: a bit optimization is capable for blind write.
        return write(mutex);
    }
    bool write(Mutex& mutex) {
        const uintptr_t key = uintptr_t(&mutex);
        typename Vec::iterator it0 = findInVec(key);
        if (it0 == vec_.end()) {
            vec_.emplace_back(mutex, ordId_);
            Lock& lk = vec_.back();
            lk.writeReserve();
            // write local version.
            return true;
        }
        Lock& lk = *it0;
        if (lk.mode() == AccessMode::INVISIBLE_READ || lk.mode() == AccessMode::RESERVED_READ) {
            if (!lk.upgrade()) return false;
        }
        // write local version.
        return true;
    }
    bool protectAll() {
#if 0
        std::sort(vec_.begin(), vec_.end());
        // map_ is invalidated here. Do not use it.
#endif
        for (Lock& lk : vec_) {
            if (lk.mode() != AccessMode::WRITE) continue;
            if (!lk.protect()) return false;
        }
        // Here is serialization point.
        __atomic_signal_fence(__ATOMIC_ACQ_REL); // for x86.
        return true;
    }
    bool verifyAndUnlock() {
        for (Lock& lk : vec_) {
            if (lk.mode() == AccessMode::WRITE) continue;
            if (!lk.unchanged()) return false;
            // S2PL allow unlocking of read locks.
            lk.unlock();
        }
        return true;
    }
    void updateAndUnlock() {
        for (Lock& lk : vec_) {
            if (lk.mode() != AccessMode::WRITE) continue;
            lk.update();
            __atomic_signal_fence(__ATOMIC_RELEASE); // for x86.
            lk.unlock();
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
        // 4KiB scan in average.
        const size_t threshold = 4096 * 2 / sizeof(Lock);
        if (vec_.size() > threshold) {
            // create indexes.
            for (size_t i = map_.size(); i < vec_.size(); i++) {
                map_[vec_[i].getMutexId()] = i;
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
            [&](const Lock& lk) {
                return lk.getMutexId() == key;
            });
    }
};


}} //namespace cybozu::lock
