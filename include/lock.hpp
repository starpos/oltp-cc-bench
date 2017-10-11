#pragma once
/**
 * @file
 * @brief several kinds of spinlock and mutexlock wrapper
 * @author Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <mutex>
#include <condition_variable>
#include <cassert>
#include <cstdio>
#include <immintrin.h>
#include <algorithm>
#include <queue>
#include <stdexcept>
#include <memory>
#include <map>
#include <string>
#include <vector>
#include <unordered_map>
#include "util.hpp"


namespace cybozu {
namespace lock {

using uint128_t = __uint128_t;
constexpr size_t CACHE_LINE_SIZE = 64;

/**
 * Mutex lock wrapper.
 */
class Mutexlock
{
public:
    struct Mutex
    {
        std::mutex v;
    };
private:
    Mutex *mutex_;
public:
    Mutexlock(Mutex *mutex) : mutex_(mutex) {
        mutex_->v.lock();
    }
    ~Mutexlock() noexcept {
        mutex_->v.unlock();
    }
};

#ifndef __ATOMIC_HLE_ACQUIRE
#define __ATOMIC_HLE_ACQUIRE 0
#endif
#ifndef __ATOMIC_HLE_RELEASE
#define __ATOMIC_HLE_RELEASE 0
#endif

/**
 * Spinlock using test-and-test-and-set (ttas).
 *
 */
template <bool useHLE>
class TtasSpinlockT
{
public:
    struct Mutex
    {
        char v;
        Mutex() : v(0) {}
    };
private:
    Mutex *mutex_;
public:
    TtasSpinlockT(Mutex *mutex) : mutex_(mutex) {
        int flag = __ATOMIC_ACQUIRE | (useHLE ? __ATOMIC_HLE_ACQUIRE : 0);
        while (mutex_->v || __atomic_exchange_n(&mutex_->v, 1, flag))
            _mm_pause();
    }
    ~TtasSpinlockT() noexcept {
        int flag = __ATOMIC_RELEASE | (useHLE ? __ATOMIC_HLE_RELEASE : 0);
        __atomic_clear(&mutex_->v, flag);
    }
};

/**
 * Ticket spinlock.
 * This is sequence lock. Do not share a lock by too much threads.
 */
template <typename UintT>
class TicketSpinlockT
{
public:
    struct Mutex
    {
        UintT head;
        UintT tail;
        Mutex() : head(0), tail(0) {}
    };
private:
    Mutex *mutex_;
#ifdef DEBUG
    UintT v_;
#endif
public:
    explicit TicketSpinlockT(Mutex *mutex) : mutex_(mutex) {
        UintT v0 = __atomic_fetch_add(&mutex_->head, 1, __ATOMIC_ACQUIRE);
        while (v0 != __atomic_load_n(&mutex_->tail, __ATOMIC_CONSUME)) {
            _mm_pause();
        }
#ifdef DEBUG
        v_ = v0;
#endif
    }
    ~TicketSpinlockT() noexcept {
        __attribute__((unused)) UintT v1
            = __atomic_fetch_add(&mutex_->tail, 1, __ATOMIC_RELEASE);
#ifdef DEBUG
        assert(v_ == v1);
#endif
    }
};

/**
 * MCS spinlock.
 * This is sequence lock. Do not share a lock by too much threads.
 */
class McsSpinlock
{
private:
    struct Node {
        bool wait;
        Node *next;
        Node() : wait(false), next(nullptr) {}
#if 0
        void print() const {
            ::printf("%p wait %d next %p\n", this, wait, next);
        }
#endif
    };
public:
    struct Mutex {
        Node *tail;
        Mutex() : tail(nullptr) {}
    };
private:
    Mutex *mutex_;
    Node node_;

#if 0
    void print() const {
        mutex_.print();
        ::printf("mutexP %p\n", *mutexP_);
    }
#endif

public:
    McsSpinlock(Mutex *mutex) : mutex_(mutex) {
        assert(mutex_);
        Node *prev = __atomic_exchange_n(&mutex_->tail, &node_, __ATOMIC_RELAXED);
        if (prev) {
            node_.wait = true;
            prev->next = &node_;
            __atomic_thread_fence(__ATOMIC_RELEASE);
            while (node_.wait) _mm_pause();
        }
    }
    ~McsSpinlock() noexcept {
        if (!node_.next) {
            Node *node = &node_;
            if (__atomic_compare_exchange_n(
                    &mutex_->tail, &node, nullptr, false,
                    __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
                return;
            }
            while (!node_.next) _mm_pause();
        }
        node_.next->wait = false;
    }
};



/**
 * Simple writer-reader mutex.
 */
class XSMutex
{
public:
    enum class Mode : uint8_t { Invalid = 0, X, S, };
private:
#ifdef MUTEX_ON_CACHELINE
    alignas(CACHE_LINE_SIZE)
#endif
    int v_;
public:
    XSMutex() : v_(0) {}
    void lock(Mode mode) {
        switch(mode) {
        case Mode::X:
            lockX(); return;
        case Mode::S:
            lockS(); return;
        default:
            throw std::runtime_error("XSMutex: invalid mode");
        }
    }
    bool tryLock(Mode mode) {
        switch(mode) {
        case Mode::X:
            return tryLockX();
        case Mode::S:
            return tryLockS();
        default:
            throw std::runtime_error("XSMutex: invalid mode");
        }
    }
    /**
     * S --> X
     */
    bool tryUpgrade() {
        int v = __atomic_load_n(&v_, __ATOMIC_RELAXED);
        if (v > 1) return false;
        assert(v == 1);
        return __atomic_compare_exchange_n(&v_, &v, -1, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
    }
    void upgrade() {
        int v = __atomic_load_n(&v_, __ATOMIC_RELAXED);
        for (;;) {
            while (v > 1) {
                _mm_pause();
                v = __atomic_load_n(&v_, __ATOMIC_RELAXED);
            }
            assert(v == 1);
            if (__atomic_compare_exchange_n(&v_, &v, -1, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
                return;
            }
        }
    }
    void unlock(Mode mode) noexcept {
        switch(mode) {
        case Mode::X:
            unlockX(); return;
        case Mode::S:
            unlockS(); return;
        default:
            assert(false);
        }
    }
    std::string str() const {
        return cybozu::util::formatString(
            "XSMutex(%d)", __atomic_load_n(&v_, __ATOMIC_RELAXED));
    }
private:
    void lockX() {
        for (;;) {
            if (v_ != 0) {
                _mm_pause();
                __atomic_thread_fence(__ATOMIC_ACQUIRE);
                continue;
            }
            if (__atomic_load_n(&v_, __ATOMIC_RELAXED) != 0) {
                _mm_pause();
                continue;
            }
            int v = 0;
            if (!__atomic_compare_exchange_n(&v_, &v, -1, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
                _mm_pause();
                continue;
            }
            break;
        }
    }
    bool tryLockX() {
#if 0
        int v = __atomic_load_n(&v_, __ATOMIC_RELAXED);
        if (v != 0) return false;
        return __atomic_compare_exchange_n(&v_, &v, -1, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
#else
        // We should retry CAS.
        int v = __atomic_load_n(&v_, __ATOMIC_RELAXED);
        for (;;) {
            if (v != 0) return false;
            if (__atomic_compare_exchange_n(&v_, &v, -1, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
                break;
            }
            _mm_pause();
        }
        return true;
#endif
    }
    void unlockX() noexcept {
        __attribute__((unused)) int ret = __atomic_fetch_add(&v_, 1, __ATOMIC_RELEASE);
        assert(ret == -1);
    }
    bool tryLockS() {
#if 0
        int v = __atomic_load_n(&v_, __ATOMIC_RELAXED);
        int w = v + 1;
        if (v < 0) return false;
        return __atomic_compare_exchange_n(&v_, &v, w, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
#else
        // We should retry CAS.
        int v = __atomic_load_n(&v_, __ATOMIC_RELAXED);
        for (;;) {
            if (v < 0) return false;
            int w = v + 1;
            if (__atomic_compare_exchange_n(&v_, &v, w, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
                break;
            }
            _mm_pause();
        }
        return true;
#endif
    }
    void lockS() {
        for (;;) {
            if (v_ < 0) {
                _mm_pause();
                __atomic_thread_fence(__ATOMIC_ACQUIRE);
                continue;
            }
            int v = __atomic_load_n(&v_, __ATOMIC_RELAXED);
            if (v < 0) {
                _mm_pause();
                continue;
            }
            int w = v + 1;
            if (!__atomic_compare_exchange_n(&v_, &v, w, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
                _mm_pause();
                continue;
            }
            break;
        }
    }
    void unlockS() noexcept {
        __attribute__((unused)) int ret = __atomic_fetch_sub(&v_, 1, __ATOMIC_RELEASE);
        assert(ret > 0);
    }
};


/**
 * Simple writer-reader lock.
 */
class XSLock
{
public:
    using Mutex = XSMutex;
    using Mode = XSMutex::Mode;
private:
    XSMutex *mutex_;
    Mode mode_;
public:
    XSLock() : mutex_(nullptr), mode_(Mode::Invalid) {}
    XSLock(XSMutex *mutex, Mode mode) : XSLock() {
        lock(mutex, mode);
    }
    ~XSLock() noexcept {
        unlock();
    }
    XSLock(const XSLock&) = delete;
    XSLock(XSLock&& rhs) : XSLock() { swap(rhs); }
    XSLock& operator=(const XSLock&) = delete;
    XSLock& operator=(XSLock&& rhs) { swap(rhs); return *this; }
    void lock(XSMutex *mutex, Mode mode) {
        assert(!mutex_);
        mutex_ = mutex;
        mode_ = mode;
        mutex_->lock(mode);
    }
    bool tryLock(XSMutex *mutex, Mode mode) {
        assert(!mutex_);
        mutex_ = mutex;
        mode_ = mode;
        if (mutex_->tryLock(mode)) return true;
        init();
        return false;
    }
    bool isShared() const {
        return mode_ == Mode::S;
    }
    bool tryUpgrade() {
        assert(mutex_);
        assert(mode_ == Mode::S);
        if (!mutex_->tryUpgrade()) return false;
        mode_ = Mode::X;
        return true;
    }
    void upgrade() {
        assert(mutex_);
        assert(mode_ == Mode::S);
        mutex_->upgrade();
        mode_ = Mode::X;
    }
    void unlock() noexcept {
        if (!mutex_) return;
        mutex_->unlock(mode_);
        init();
    }
    const Mutex* mutex() const { return mutex_; }
    Mutex* mutex() { return mutex_; }
    uintptr_t getMutexId() const { return uintptr_t(mutex_); }
    Mode mode() const { return mode_; }

    /*
     * This is used for dummy object to comparison.
     */
    void setMutex(Mutex *mutex) { mutex_ = mutex; }
private:
    void init() {
        mutex_ = nullptr;
        mode_ = Mode::Invalid;
    }
    void swap(XSLock& rhs) {
        std::swap(mutex_, rhs.mutex_);
        std::swap(mode_, rhs.mode_);
    }
};


class NoWaitLockSet
{
    using Mutex = cybozu::lock::XSMutex;
    using Lock = cybozu::lock::XSLock;
    using Mode = Mutex::Mode;

    using LockV = std::vector<Lock>;
    using Index = std::unordered_map<uintptr_t, size_t>;

    LockV lockV_;
    Index index_;

public:
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
        Lock &lk = lockV_.back();
        if (!lk.tryLock(&mutex, Mode::S)) {
            // should die.
            return false;
        }
        // read shared data.
        return true;
    }
    bool write(Mutex& mutex) {
        LockV::iterator it = find(uintptr_t(&mutex));
        if (it != lockV_.end()) {
            Lock& lk = *it;
            if (lk.mode() == Mode::S && !lk.tryUpgrade()) {
                return false;
            }
            // write shared data.
            return true;
        }
        lockV_.emplace_back();
        Lock &lk = lockV_.back();
        if (!lk.tryLock(&mutex, Mode::X)) {
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
        const size_t threshold = 4096 / sizeof(Lock);
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
            [&](const Lock& lk) {
                return lk.getMutexId() == key;
            });
    }
};


}} //namespace cybozu::lock
