#pragma once
/**
 * 2PL wait and die for deadlock prevension.
 */
#include "lock_data.hpp"
#include "arch.hpp"
#include "cache_line_size.hpp"
#include "vector_payload.hpp"
#include "write_set.hpp"


namespace cybozu {
namespace wait_die {

using uint128_t = __uint128_t;

#ifndef USE_64BIT_TXID
using TxId = uint32_t;
#else
using TxId = uint64_t;
#endif

const TxId MAX_TXID = TxId(-1);

struct WaitDieData
{
    using Mode = cybozu::lock::LockStateXS::Mode;

    union {
#ifndef USE_64BIT_TXID
        uint64_t obj;
        struct {
            uint32_t txId; // txId that holds X lock.
            uint8_t lkstObj;
            uint8_t reserved0;
            uint16_t reserved1;
        };
#else
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

    WaitDieData() = default;
    void init() {
        txId = MAX_TXID;
        getLockState()->clearAll();
        reserved0 = 0;
        reserved1 = 0;
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

    cybozu::lock::LockStateXS* getLockState() {
        return (cybozu::lock::LockStateXS *)(&lkstObj);
    }
    const cybozu::lock::LockStateXS* getLockState() const {
        return (cybozu::lock::LockStateXS *)(&lkstObj);
    }

    std::string str() const {
        return cybozu::util::formatString(
#ifndef USE_64BIT_TXID
            "txId %u %s",
#else
            "txId %" PRIu64 " %s",
#endif
            txId, getLockState()->str().c_str());
    }
};


class WaitDieLock
{
public:
    using Mode = cybozu::lock::LockStateXS::Mode;
    struct Mutex {
#if 0
#ifdef MUTEX_ON_CACHELINE
        alignas(CACHE_LINE_SIZE)
#endif
#else
        alignas(sizeof(uintptr_t))
#endif
        WaitDieData wd;

        Mutex() { wd.init(); }
        WaitDieData atomicRead() const {
            return __atomic_load_n(&wd.obj, __ATOMIC_RELAXED);
        }
        bool compareAndSwap(WaitDieData& expected, WaitDieData desired, int mode) {
            return __atomic_compare_exchange_n(
                &wd.obj, &expected.obj, desired.obj,
                false, mode, __ATOMIC_RELAXED);
        }
    };

private:
    Mutex *mutex_;
    Mode mode_;
    TxId txId_;

public:
    WaitDieLock() : mutex_(nullptr), mode_(Mode::INVALID), txId_(0) {}
    ~WaitDieLock() noexcept {
        unlock();
    }
    WaitDieLock(const WaitDieLock& ) = delete;
    WaitDieLock(WaitDieLock&& rhs) : WaitDieLock() { swap(rhs); }
    WaitDieLock& operator=(const WaitDieLock& rhs) = delete;
    WaitDieLock& operator=(WaitDieLock&& rhs) { swap(rhs); return *this; }

    /**
     * This is for blind-write.
     */
    void setMutex(Mutex *mutex) { mutex_ = mutex; }

    /**
     * If true, locked.
     * If false, you must abort your running transaction.
     */
    bool lock(Mutex *mutex, Mode mode, TxId txId) {
        assert(mode_ == Mode::INVALID);
        assert(mode != Mode::INVALID);
        assert(mutex);
        mutex_ = mutex;
        mode_ = mode;
        txId_ = txId;

        WaitDieData wd0 = mutex_->atomicRead();
        for (;;) {
            if (wd0.getLockState()->canSet(mode_)) {
#ifndef NDEBUG
                if (wd0.getLockState()->isUnlocked()) {
                    assert(wd0.txId == MAX_TXID);
                }
#endif

                WaitDieData wd1 = wd0;
                wd1.getLockState()->set(mode_);
                if (txId < wd0.txId) {
                    wd1.txId = txId;
                }
                if (mutex_->compareAndSwap(wd0, wd1, __ATOMIC_ACQUIRE)) {
#if 0 // debug
                    ::printf("lock    %p %s\n", mutex, wd1.str().c_str());
#endif
                    return true;
                }
                continue;
            } else if (wd0.txId <= txId) {
                break;
            } else {
                waitFor(wd0);
                continue;
            }
        }
        init();
        return false;
    }
    void unlock() noexcept {
        if (mode_ == Mode::INVALID) return;
        assert(mutex_);

        WaitDieData wd0 = mutex_->atomicRead();
        for (;;) {
            WaitDieData wd1 = wd0;
            assert(wd1.getLockState()->canClear(mode_));
            wd1.getLockState()->clear(mode_);
            if (wd1.getLockState()->isUnlocked()) {
                wd1.txId = MAX_TXID;
            }
            if (mutex_->compareAndSwap(wd0, wd1, __ATOMIC_RELEASE)) {
#if 0 // debug
                ::printf("unlock  %p %s\n", mutex_, wd1.str().c_str());
#endif
                break;
            }
        }
        init();
    }
    bool upgrade() {
        assert(mutex_);
        assert(mode_ == Mode::S);
        WaitDieData wd0 = mutex_->atomicRead();
        while (txId_ == wd0.txId && wd0.getLockState()->getCount(Mode::S) == 1) {
            WaitDieData wd1 = wd0;
            wd1.getLockState()->clearAll();
            wd1.getLockState()->set(Mode::X);
            if (mutex_->compareAndSwap(wd0, wd1, __ATOMIC_RELAXED)) {
                mode_ = Mode::X;
#if 0 // debug
                ::printf("upgrade %p %s\n", mutex_, wd1.str().c_str());
#endif
                return true;
            }
        }
        return false;
    }
    Mode mode() const {
        return mode_;
    }
    uintptr_t getMutexId() const {
        return uintptr_t(mutex_);
    }
private:
    void init() {
        mutex_ = nullptr;
        mode_ = Mode::INVALID;
        txId_ = 0;
    }
    void swap(WaitDieLock& rhs) {
        std::swap(mutex_, rhs.mutex_);
        std::swap(mode_, rhs.mode_);
        std::swap(txId_, rhs.txId_);
    }
    void waitFor(WaitDieData& wd) {
        assert(mutex_);
        for (;;) {
            wd = mutex_->atomicRead();
            if (wd.getLockState()->canSet(mode_) || wd.txId <= txId_) return;
            _mm_pause();
        }
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
    using Index = std::unordered_map<uintptr_t, size_t>;

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
    void init(size_t valueSize) {
        valueSize_ = valueSize;
        if (valueSize == 0) valueSize++;
#ifdef MUTEX_ON_CACHELINE
        local_.setSizes(valueSize, CACHE_LINE_SIZE);
#else
        local_.setSizes(valueSize);
#endif
    }
    /* call this before read/write just after a transaction trial starts. */
    void setTxId(TxId txId) { txId_ = txId; }

    bool read(Mutex& mutex, void *sharedVal, void *dst) {
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
        if (!lk.lock(&mutex, Mode::S, txId_)) {
            // should die.
            return false;
        }
        copyValue(dst, sharedVal); // read shared data.
        return true;
    }
    bool write(Mutex& mutex, void *sharedVal, void *src) {
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
        ope.lock.setMutex(&mutex); // for search.
        bwV_.emplace_back(&mutex, vec_.size() - 1);
        ope.info.set(allocateLocalVal(), sharedVal);
        copyValue(getLocalValPtr(ope.info), src); // write local data.
        return true;
    }
    bool readForUpdate(Mutex& mutex, void *sharedVal, void *dst) {
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
        if (!lk.lock(&mutex, Mode::X, txId_)) {
            // should die.
            return false;
        }
        info.set(allocateLocalVal(), sharedVal);
        void* localVal = getLocalValPtr(info);
        copyValue(localVal, sharedVal); // for next read.
        copyValue(dst, localVal); // read local data.
        return true;
    }

    bool blindWriteLockAll() {
        for (BlindWriteInfo& bwInfo : bwV_) {
            OpEntryL& ope = vec_[bwInfo.idx];
            assert(ope.lock.mode() == Mode::INVALID);
            if (!ope.lock.lock(bwInfo.mutex, Mode::X, txId_)) {
                // should die.
                return false;
            }
        }
        return true;
    }
    void updateAndUnlock() {
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
            OpEntryL garbage(std::move(ope)); // will be unlocked soon.
#endif
        }
        vec_.clear();
        index_.clear();
        local_.clear();
        bwV_.clear();
    }
    void unlock() {
        vec_.clear(); // unlock.
        index_.clear();
        local_.clear();
        bwV_.clear();
    }
    bool empty() const {
        return vec_.empty() && index_.empty();
    }
private:
    Vec::iterator find(uintptr_t key) {
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
#endif
    }
    size_t allocateLocalVal() {
        const size_t idx = local_.size();
#ifndef NO_PAYLOAD
        local_.resize(idx + 1);
#endif
        return idx;
    }
};


}} // namespace cybozu::wait_die
