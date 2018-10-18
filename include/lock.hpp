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
#include <algorithm>
#include <queue>
#include <stdexcept>
#include <memory>
#include <map>
#include <string>
#include <vector>
#include <unordered_map>
#include "util.hpp"
#include "arch.hpp"
#include "atomic_wrapper.hpp"
#include "inline.hpp"


namespace cybozu {
namespace lock {

using uint128_t = __uint128_t;

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
#ifndef NDEBUG
    UintT v_;
#endif
public:
    explicit TicketSpinlockT(Mutex *mutex) : mutex_(mutex) {
        UintT v0 = __atomic_fetch_add(&mutex_->head, 1, __ATOMIC_ACQUIRE);
        while (v0 != __atomic_load_n(&mutex_->tail, __ATOMIC_CONSUME)) {
            _mm_pause();
        }
#ifndef NDEBUG
        v_ = v0;
#endif
    }
    ~TicketSpinlockT() noexcept {
        __attribute__((unused)) UintT v1
            = __atomic_fetch_add(&mutex_->tail, 1, __ATOMIC_RELEASE);
#ifndef NDEBUG
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
        Node *prev = exchange(mutex_->tail, &node_, __ATOMIC_ACQ_REL);
        if (prev) {
            store(node_.wait, true);
            storeRelease(prev->next, &node_);
            while (loadAcquire(node_.wait)) _mm_pause();
        }
    }
    ~McsSpinlock() noexcept {
        if (!load(node_.next)) {
            Node *node = &node_;
            if (compareExchange(mutex_->tail, node, nullptr, __ATOMIC_RELEASE)) {
                return;
            }
            while (!load(node_.next)) _mm_pause();
        }
        storeRelease(node_.next->wait, false);
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
    alignas(sizeof(uintptr_t))
    int v_;
public:
    XSMutex() : v_(0) {}
    INLINE void lock(Mode mode) {
        switch(mode) {
        case Mode::X:
            lockX(); return;
        case Mode::S:
            lockS(); return;
        default:
            throw std::runtime_error("XSMutex: invalid mode");
        }
    }
    INLINE bool tryLock(Mode mode) {
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
    INLINE bool tryUpgrade() {
        int v = load(v_);
        if (v > 1) return false;
        assert(v == 1);
        return compareExchange(v_, v, -1, __ATOMIC_RELAXED);
    }
    INLINE void upgrade() {
        int v = load(v_);
        for (;;) {
            while (v > 1) {
                _mm_pause();
                v = load(v_);
            }
            assert(v == 1);
            if (compareExchange(v_, v, -1, __ATOMIC_RELAXED)) {
                return;
            }
        }
    }
    INLINE void unlock(Mode mode) noexcept {
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
        return cybozu::util::formatString("XSMutex(%d)", load(v_));
    }

private:
    void lockX() {
        for (;;) {
            if (v_ != 0) {
                _mm_pause();
                continue;
            }
            if (loadAcquire(v_) != 0) {
                _mm_pause();
                continue;
            }
            int v = 0;
            if (!compareExchange(v_, v, -1, __ATOMIC_ACQUIRE)) {
                _mm_pause();
                continue;
            }
            break;
        }
    }
    bool tryLockX() {
#if 0
        int v = loadAcquire(v_);
        if (v != 0) return false;
        return compareExchange(v_, v, -1, __ATOMIC_ACQUIRE);
#else
        // We should retry CAS.
        int v = loadAcquire(v_);
        for (;;) {
            if (v != 0) return false;
            if (compareExchange(v_, v, -1, __ATOMIC_ACQUIRE)) {
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
        int v = loadAcquire(v_);
        if (v < 0) return false;
        return compareExchange(v_, v, v + 1, __ATOMIC_ACQUIRE);
#else
        // We should retry CAS.
        int v = loadAcquire(v_);
        for (;;) {
            if (v < 0) return false;
            if (compareExchange(v_, v, v + 1, __ATOMIC_ACQUIRE)) {
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
                continue;
            }
            int v = loadAcquire(v_);
            if (v < 0) {
                _mm_pause();
                continue;
            }
            if (!compareExchange(v_, v, v + 1, __ATOMIC_ACQUIRE)) {
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
    XSLock(XSLock&& rhs) noexcept : XSLock() { swap(rhs); }
    XSLock& operator=(const XSLock&) = delete;
    XSLock& operator=(XSLock&& rhs) noexcept { swap(rhs); return *this; }
    INLINE void lock(XSMutex *mutex, Mode mode) {
        assert(mode_ == Mode::Invalid);
        assert(mutex);
        mutex_ = mutex;
        mode_ = mode;
        mutex_->lock(mode);
    }
    INLINE bool tryLock(XSMutex *mutex, Mode mode) {
        assert(mode_ == Mode::Invalid);
        assert(mutex);
        mutex_ = mutex;
        mode_ = mode;
        if (mutex_->tryLock(mode)) return true;
        init();
        return false;
    }
    bool isShared() const {
        return mode_ == Mode::S;
    }
    INLINE bool tryUpgrade() {
        assert(mutex_);
        assert(mode_ == Mode::S);
        if (!mutex_->tryUpgrade()) return false;
        mode_ = Mode::X;
        return true;
    }
    INLINE void upgrade() {
        assert(mutex_);
        assert(mode_ == Mode::S);
        mutex_->upgrade();
        mode_ = Mode::X;
    }
    INLINE void unlock() noexcept {
        if (mode_ == Mode::Invalid) return;
        assert(mutex_);
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
    void swap(XSLock& rhs) noexcept {
        std::swap(mutex_, rhs.mutex_);
        std::swap(mode_, rhs.mode_);
    }
};


}} //namespace cybozu::lock
