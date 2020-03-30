#pragma once
/**
 * 2PL wait and die for deadlock prevension.
 */
#include "lock_data.hpp"
#include "arch.hpp"
#include "vector_payload.hpp"
#include "write_set.hpp"
#include "inline.hpp"


namespace cybozu {
namespace wait_die {

using uint128_t = __uint128_t;

#ifndef USE_64BIT_TXID
using TxId = uint32_t;
#else
using TxId = uint64_t;
#endif

const TxId MAX_TXID = TxId(-1);
const uint16_t MAX_READERS = (1 << 15) - 1;


struct WaitDieData
{
    using Mode = cybozu::lock::LockStateXS::Mode;

    // To avoid the BUG: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=90511
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
    alignas(sizeof(uint64_t))
    union {
#ifndef USE_64BIT_TXID
        uint64_t obj;
        struct {
            // txId that holds X lock or mininum txId (that means prior tx) that holds S lock.
            uint32_t txId;
            union {
                uint16_t lockObj;
                struct {
                    uint16_t locked:1;
                    uint16_t readers:15;
                };
            };
            uint16_t reserved0;
        };
#else
        // obsolete code.
        uint128_t obj;
        struct {
            uint64_t txId;
            uint8_t lkstObj;
            uint8_t reserved0;
            uint16_t reserved1;
            uint32_t reserved2;
        };
#endif
    };
#pragma GCC diagnostic pop

    WaitDieData() = default;
    void init() {
        txId = MAX_TXID;
        lockObj = 0;
#ifndef NDEBUG
        reserved0 = 0;
#endif
    }
#ifndef USE_64BIT_TXID
    WaitDieData(uint64_t obj0) {
        obj = obj0;
    }
    operator uint64_t() const {
        return obj;
    }
#else
    WaitDieData(uint128_t obj0) {
        obj = obj0;
    }
    operator uint128_t() const {
        return obj;
    }
#endif

    std::string str() const {
        return cybozu::util::formatString(
#ifndef USE_64BIT_TXID
            "txId %u X %u S %u",
#else
            "txId %" PRIu64 " X %u S %u",
#endif
            txId, locked, readers);
    }
};


class WaitDieLock
{
public:
    using Mode = cybozu::lock::LockStateXS::Mode;
    struct Mutex {
        alignas(sizeof(uintptr_t))
        WaitDieData wd;

        INLINE Mutex() { wd.init(); }

        DEPRECATED WaitDieData atomicRead() const {
            return __atomic_load_n(&wd.obj, __ATOMIC_RELAXED);
        }
        DEPRECATED bool compareAndSwap(WaitDieData& expected, WaitDieData desired, int mode) {
            bool ret = __atomic_compare_exchange_n(
                &wd.obj, &expected.obj, desired.obj,
                false, mode, __ATOMIC_RELAXED);
            return ret;
        }

        INLINE WaitDieData load() const {
            return __atomic_load_n(&wd.obj, __ATOMIC_ACQUIRE);
        }
        INLINE void store(WaitDieData value) {
            __atomic_store_n(&wd.obj, value.obj, __ATOMIC_RELEASE);
        }
        INLINE bool cas(WaitDieData& expected, WaitDieData desired, int mode = __ATOMIC_ACQ_REL) {
            return __atomic_compare_exchange_n(
                &wd.obj, &expected.obj, desired.obj, false, mode, __ATOMIC_ACQUIRE);
        }
    };

private:
    Mutex *mutex_;
    Mode mode_;
    TxId txId_;

public:
    INLINE WaitDieLock() noexcept : mutex_(nullptr), mode_(Mode::INVALID), txId_(MAX_TXID) {}
    INLINE ~WaitDieLock() noexcept {
        unlock();
    }
    INLINE WaitDieLock(const WaitDieLock& ) = delete;
    INLINE WaitDieLock(WaitDieLock&& rhs) noexcept : WaitDieLock() { swap(rhs); }
    INLINE WaitDieLock& operator=(const WaitDieLock& rhs) = delete;
    INLINE WaitDieLock& operator=(WaitDieLock&& rhs) noexcept { swap(rhs); return *this; }

    /**
     * This is for blind-write.
     */
    INLINE void setMutex(Mutex& mutex) noexcept { mutex_ = &mutex; }

    /**
     * If true, locked.
     * If false, you must abort your running transaction.
     */
    INLINE bool readLock(Mutex& mutex, TxId txId) noexcept {
        assert(txId != MAX_TXID);
        WaitDieData wd0 = mutex.load();
        for (;;) {
            _mm_pause();
            if (wd0.locked == 0 && wd0.readers < MAX_READERS) {
                // try to lock
                WaitDieData wd1 = wd0;
                wd1.readers++;
                if (txId < wd1.txId) wd1.txId = txId;
                if (mutex.cas(wd0, wd1, __ATOMIC_ACQUIRE)) {
                    mutex_ = &mutex;
                    mode_ = Mode::S;
                    txId_ = txId;
                    return true;
                }
                continue; // retry
            }
            assert(wd0.txId != txId);
            if (wd0.txId < txId) {
                return false; // die
            }
            wd0 = mutex.load();
            // wait
        }
    }
    INLINE bool writeLock(Mutex& mutex, TxId txId) noexcept {
        assert(txId != MAX_TXID);
        WaitDieData wd0 = mutex.load();
        for (;;) {
            _mm_pause();
            if (wd0.locked || wd0.readers != 0) {
                assert(wd0.txId != txId);
                if (wd0.txId < txId) {
                    return false; // die
                }
                wd0 = mutex.load();
                continue; // wait
            }
            // try to lock
            WaitDieData wd1 = wd0;
            wd1.locked = 1;
            wd1.txId = txId;
            if (mutex.cas(wd0, wd1, __ATOMIC_ACQUIRE)) {
                mutex_ = &mutex;
                mode_ = Mode::X;
                txId_ = txId;
                return true;
            }
            // retry
        }
    }
    INLINE void unlock() noexcept {
        // mutex_ may not be nullptr due to setMutex().
        switch (mode_) {
        case Mode::INVALID: return;
        case Mode::S: readUnlock(); return;
        case Mode::X: writeUnlock(); return;
        default: assert(false);
        }
    }
    INLINE void readUnlock() noexcept {
        assert(mode_ == Mode::S);
        assert(mutex_ != nullptr);
        assert(txId_ != MAX_TXID);
        WaitDieData wd0 = mutex_->load();
        for (;;) {
            _mm_pause();
            WaitDieData wd1 = wd0;
            assert(wd1.readers > 0);
            wd1.readers--;
            if (wd1.readers == 0) {
                wd1.txId = MAX_TXID;
            }
            if (mutex_->cas(wd0, wd1, __ATOMIC_RELEASE)) {
                init();
                return;
            }
            // retry
        }
    }
    INLINE void writeUnlock() noexcept {
        assert(mode_ == Mode::X);
        assert(mutex_ != nullptr);
        assert(txId_ != MAX_TXID);
        WaitDieData wd0 = mutex_->load();
        WaitDieData wd1 = wd0;
        assert(wd1.locked);
        wd1.locked = 0;
        wd1.txId = MAX_TXID;
        mutex_->store(wd1);
        init();
    }
    INLINE bool upgrade() noexcept {
        assert(mutex_);
        assert(mode_ == Mode::S);
        assert(txId_ != MAX_TXID);
        WaitDieData wd0 = mutex_->load();
        while (txId_ == wd0.txId && wd0.readers == 1) {
            _mm_pause();
            WaitDieData wd1 = wd0;
            wd1.locked = 1;
            wd1.readers = 0;
            if (mutex_->cas(wd0, wd1)) {
                mode_ = Mode::X;
                return true;
            }
            // retry
        }
        return false;
    }
    INLINE Mode mode() const noexcept {
        return mode_;
    }
    INLINE uintptr_t getMutexId() const noexcept {
        return uintptr_t(mutex_);
    }
private:
    INLINE void init() noexcept {
        mutex_ = nullptr;
        mode_ = Mode::INVALID;
        txId_ = MAX_TXID;
    }
    INLINE void swap(WaitDieLock& rhs) noexcept {
        std::swap(mutex_, rhs.mutex_);
        std::swap(mode_, rhs.mode_);
        std::swap(txId_, rhs.txId_);
    }
};


class LockSet
{
    using Mode = cybozu::lock::LockStateXS::Mode;
    using Mutex = WaitDieLock::Mutex;
    using Lock = WaitDieLock;
    using OpEntryL = OpEntry<Lock>;
    using Vec = std::vector<OpEntryL>;

    // key: mutex addr, value: index in the vector.
#if 1
    using Index = SingleThreadUnorderedMap<uintptr_t, size_t>;
#else
    using Index = std::unordered_map<uintptr_t, size_t>;
#endif

    Vec vec_;
    Index index_;

    TxId txId_;

    MemoryVector local_;
    size_t valueSize_;

    struct BlindWriteInfo {
        Mutex *mutex;
        size_t idx; // index of blind-write entry in vec_.

        BlindWriteInfo() {
        }
        BlindWriteInfo(Mutex* mutex0, size_t idx0) : mutex(mutex0), idx(idx0) {
        }
    };
    std::vector<BlindWriteInfo> bwV_;

public:
    // Call this at first once.
    void init(size_t valueSize, size_t nrReserve) {
        valueSize_ = valueSize;
        if (valueSize == 0) valueSize++;
        local_.setSizes(valueSize);

        vec_.reserve(nrReserve);
        local_.reserve(nrReserve);
        bwV_.reserve(nrReserve); // This may be too conservative and memory eater.
    }
    /* call this before read/write just after a transaction trial starts. */
    void setTxId(TxId txId) { txId_ = txId; }

    INLINE bool read(Mutex& mutex, void *sharedVal, void *dst) {
        Vec::iterator it = find(uintptr_t(&mutex));
        if (it != vec_.end()) {
            Lock& lk = it->lock;
            if (lk.mode() == Mode::S) {
                copyValue(dst, sharedVal); // read shared data.
                return true;
            }
            assert(lk.mode() == Mode::X || lk.mode() == Mode::INVALID);
            copyValue(dst, getLocalValPtr(it->info)); // read local data.
            return true;
        }
        // Try to read lock.
        vec_.emplace_back();
        OpEntryL &ope = vec_.back();
        Lock& lk = ope.lock;
        if (!lk.readLock(mutex, txId_)) {
            // should die.
            return false;
        }
        copyValue(dst, sharedVal); // read shared data.
        return true;
    }
    INLINE bool write(Mutex& mutex, void *sharedVal, void *src) {
        Vec::iterator it = find(uintptr_t(&mutex));
        if (it != vec_.end()) {
            Lock& lk = it->lock;
            if (lk.mode() == Mode::S) {
                if (!lk.upgrade()) return false;
                it->info.set(allocateLocalVal(), sharedVal);
            }
            assert(lk.mode() == Mode::X || lk.mode() == Mode::INVALID);
            copyValue(getLocalValPtr(it->info), src); // write local data.
            return true;
        }
        // This is blind write.
        vec_.emplace_back();
        OpEntryL& ope = vec_.back();
        // Lock will be tried later. See blindWriteLockAll().
        ope.lock.setMutex(mutex); // for search.
        bwV_.emplace_back(&mutex, vec_.size() - 1);
        ope.info.set(allocateLocalVal(), sharedVal);
        copyValue(getLocalValPtr(ope.info), src); // write local data.
        return true;
    }
    INLINE bool readForUpdate(Mutex& mutex, void *sharedVal, void *dst) {
        Vec::iterator it = find(uintptr_t(&mutex));
        if (it != vec_.end()) {
            // Found.
            Lock& lk = it->lock;
            LocalValInfo& info = it->info;
            if (lk.mode() == Mode::X) {
                copyValue(dst, getLocalValPtr(info)); // read local data.
                return true;
            }
            if (lk.mode() == Mode::S) {
                if (!lk.upgrade()) return false;
                info.set(allocateLocalVal(), sharedVal);
                void* localVal = getLocalValPtr(info);
                copyValue(localVal, sharedVal); // for next read.
                copyValue(dst, localVal); // read local data.
                return true;
            }
            assert(lk.mode() == Mode::INVALID);
            copyValue(dst, getLocalValPtr(info)); // read local data.
            return true;
        }
        // Not found. Try to write lock.
        vec_.emplace_back();
        OpEntryL &ope = vec_.back();
        Lock& lk = ope.lock;
        LocalValInfo& info = ope.info;
        if (!lk.writeLock(mutex, txId_)) {
            // should die.
            return false;
        }
        info.set(allocateLocalVal(), sharedVal);
        void* localVal = getLocalValPtr(info);
        copyValue(localVal, sharedVal); // for next read.
        copyValue(dst, localVal); // read local data.
        return true;
    }

    INLINE bool blindWriteLockAll() {
        for (BlindWriteInfo& bwInfo : bwV_) {
            OpEntryL& ope = vec_[bwInfo.idx];
            assert(ope.lock.mode() == Mode::INVALID);
            if (!ope.lock.writeLock(*bwInfo.mutex, txId_)) {
                // should die.
                return false;
            }
        }
        return true;
    }
    INLINE void updateAndUnlock() {
        // serialization point.

        for (OpEntryL& ope : vec_) {
            Lock& lk = ope.lock;
            if (lk.mode() == Mode::X) {
                // update.
                LocalValInfo& info = ope.info;
                copyValue(info.sharedVal, getLocalValPtr(info));
            } else {
                assert(lk.mode() == Mode::S);
            }
#if 1 // unlock one by one.
            ope.lock.unlock();
#endif
        }
        vec_.clear();
        index_.clear();
        local_.clear();
        bwV_.clear();
    }
    INLINE void unlock() {
        vec_.clear(); // unlock.
        index_.clear();
        local_.clear();
        bwV_.clear();
    }
    bool empty() const {
        return vec_.empty() && index_.empty();
    }
private:
    INLINE Vec::iterator find(uintptr_t key) {
        // at most 4KiB scan.
        const size_t threshold = 4096 / sizeof(OpEntryL);
        if (vec_.size() > threshold) {
            for (size_t i = index_.size(); i < vec_.size(); i++) {
                index_[vec_[i].lock.getMutexId()] = i;
            }
            Index::iterator it = index_.find(key);
            if (it == index_.end()) {
                return vec_.end();
            } else {
                size_t idx = it->second;
                return vec_.begin() + idx;
            }
        }
        return std::find_if(
            vec_.begin(), vec_.end(),
            [&](const OpEntryL& ope) {
                return ope.lock.getMutexId() == key;
            });
    }
    void* getLocalValPtr(const LocalValInfo& info) {
#ifdef NO_PAYLOAD
        unused(info);
        return nullptr;
#else
        if (info.localValIdx == UINT64_MAX) {
            return nullptr;
        } else {
            return &local_[info.localValIdx];
        }
#endif
    }
    void copyValue(void* dst, const void* src) {
#ifndef NO_PAYLOAD
        ::memcpy(dst, src, valueSize_);
#else
        unused(dst); unused(src);
#endif
    }
    INLINE size_t allocateLocalVal() {
        const size_t idx = local_.size();
#ifndef NO_PAYLOAD
        local_.resize(idx + 1);
#endif
        return idx;
    }
};


}} // namespace cybozu::wait_die
