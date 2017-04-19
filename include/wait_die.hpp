#pragma once
/**
 * 2PL wait and die for deadlock prevension.
 */
#include <immintrin.h>
#include "lock_data.hpp"


namespace cybozu {
namespace wait_die {

using uint128_t = __uint128_t;
constexpr size_t CACHE_LINE_SIZE = 64;

#ifndef USE_64BIT_TXID
using TxId = uint32_t;
#else
using TxId = uint64_t;
#endif

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
#ifndef USE_64BIT_TXID
        txId = UINT32_MAX;
#else
        txId = UINT64_MAX;
#endif
        getLockState()->clearAll();
        reserved0 = 0;
        reserved1 = 0;
    }
    WaitDieData(uint64_t obj0) {
        obj = obj0;
    }
    operator uint64_t() const {
        return obj;
    }

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
#ifdef MUTEX_ON_CACHELINE
        alignas(CACHE_LINE_SIZE)
#endif
        WaitDieData wd;

        Mutex() { wd.init(); }
        WaitDieData atomicRead() const {
            return __atomic_load_n(&wd.obj, __ATOMIC_RELAXED);
        }
        bool compareAndSwap(WaitDieData& expected, WaitDieData desired) {
            return __atomic_compare_exchange_n(
                &wd.obj, &expected.obj, desired.obj,
                false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
        }
        void set(WaitDieData desired) {
            wd.obj = desired.obj;
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
     * If true, locked.
     * If false, you must abort your running transaction.
     */
    bool lock(Mutex *mutex, Mode mode, TxId txId) {
        assert(!mutex_);
        assert(mutex);
        mutex_ = mutex;
        mode_ = mode;
        txId_ = txId;

        WaitDieData wd0 = mutex_->atomicRead();
        for (;;) {
            if (wd0.getLockState()->canSet(mode_)) {
                WaitDieData wd1 = wd0;
                wd1.getLockState()->set(mode_);
                if (txId < wd0.txId) {
                    wd1.txId = txId;
                }
                if (mutex_->compareAndSwap(wd0, wd1)) return true;
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
        if (!mutex_) return;

        WaitDieData wd0 = mutex_->atomicRead();
        for (;;) {
            WaitDieData wd1 = wd0;
            assert(wd1.getLockState()->canClear(mode_));
            wd1.getLockState()->clear(mode_);
            if (mutex_->compareAndSwap(wd0, wd1)) break;
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
            if (mutex_->compareAndSwap(wd0, wd1)) {
                mode_ = Mode::X;
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
    using LockV = std::vector<WaitDieLock>;

    // key: mutex addr, value: index in the vector.
    using Index = std::unordered_map<uintptr_t, size_t>;

    LockV lockV_;
    Index index_;

    TxId txId_;

public:
    /* call this before read/write. */
    void setTxId(TxId txId) { txId_ = txId; }

    bool lock(Mutex& mutex, Mode mode) {
        return mode == Mode::S ? read(mutex) : write(mutex);
    }
    bool read(Mutex& mutex) {
        LockV::iterator it = find(uintptr_t(&mutex));
        if (it != lockV_.end()) {
            // read shared data.
            return true;
        }
        lockV_.emplace_back();
        WaitDieLock &lk = lockV_.back();
        if (!lk.lock(&mutex, Mode::S, txId_)) {
            // should die.
            return false;
        }
        // read shared data.
        return true;
    }
    bool write(Mutex& mutex) {
        LockV::iterator it = find(uintptr_t(&mutex));
        if (it != lockV_.end()) {
            WaitDieLock& lk = *it;
            if (lk.mode() == Mode::S && !lk.upgrade()) {
                return false;
            }
            // write shared data.
            return true;
        }
        lockV_.emplace_back();
        WaitDieLock &lk = lockV_.back();
        if (!lk.lock(&mutex, Mode::X, txId_)) {
            // should die.
            return false;
        }
        // write shared data.
        return true;
    }
    void clear() {
        lockV_.clear(); // unlock.
        index_.clear();
    }
    bool empty() const {
        return lockV_.empty() && index_.empty();
    }
private:
    LockV::iterator find(uintptr_t key) {
        const size_t threshold = 4096 / sizeof(WaitDieLock);
        if (lockV_.size() > threshold) {
            for (size_t i = index_.size(); i < lockV_.size(); i++) {
                index_[lockV_[i].getMutexId()] = i;
            }
            Index::iterator it = index_.find(key);
            if (it == index_.end()) {
                return lockV_.end();
            } else {
                size_t idx = it->second;
                return lockV_.begin() + idx;
            }
        }
        return std::find_if(
            lockV_.begin(), lockV_.end(),
            [&](const WaitDieLock& lk) {
                return lk.getMutexId() == key;
            });
    }
};


}} // namespace cybozu::wait_die
