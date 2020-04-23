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
    using Mutex = std::mutex;
private:
    Mutex* mutexp_;
public:
    INLINE explicit Mutexlock(Mutex& mutex) : mutexp_(&mutex) {
        mutex.lock();
    }
    INLINE ~Mutexlock() noexcept {
        mutexp_->unlock();
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
    INLINE explicit TtasSpinlockT(Mutex *mutex) : mutex_(mutex) {
        int flag = __ATOMIC_ACQUIRE | (useHLE ? __ATOMIC_HLE_ACQUIRE : 0);
        while (mutex_->v || __atomic_exchange_n(&mutex_->v, 1, flag))
            _mm_pause();
    }
    INLINE ~TtasSpinlockT() noexcept {
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
    INLINE explicit TicketSpinlockT(Mutex *mutex) : mutex_(mutex) {
        UintT v0 = __atomic_fetch_add(&mutex_->head, 1, __ATOMIC_ACQUIRE);
        while (v0 != __atomic_load_n(&mutex_->tail, __ATOMIC_CONSUME)) {
            _mm_pause();
        }
#ifndef NDEBUG
        v_ = v0;
#endif
    }
    INLINE ~TicketSpinlockT() noexcept {
        __attribute__((unused)) UintT v1
            = __atomic_fetch_add(&mutex_->tail, 1, __ATOMIC_RELEASE);
#ifndef NDEBUG
        assert(v_ == v1);
#endif
    }
};

/**
 * MCS spinlock.
 * This is a fair locking protocol.
 */
class McsSpinlock
{
private:
    struct Node {
        bool wait;
        Node *next;
        INLINE Node() { reset(); }
        INLINE void reset() { wait = false; next = nullptr; }
#if 0
        void print() const {
            ::printf("%p wait %d next %p\n", this, wait, next);
        }
#endif
    };
public:
    struct Mutex {
        Node *tail;
        INLINE Mutex() : tail(nullptr) {}
    };
private:
    Mutex *mutexp_;
    Node node_;

#if 0
    void print() const {
        mutex_.print();
        ::printf("mutexP %p\n", *mutexP_);
    }
#endif

public:
    INLINE McsSpinlock() : mutexp_(nullptr), node_() {
    }
    INLINE explicit McsSpinlock(Mutex& mutex) : McsSpinlock() {
        lock(mutex);
    }
    INLINE ~McsSpinlock() noexcept { if (mutexp_) unlock(); }

    INLINE bool try_lock(Mutex& mutex) {
        assert(mutexp_ == nullptr);
        Node* tail = ::load(mutex.tail);
        while (tail == nullptr) {
            _mm_pause();
            if (compare_exchange_acquire(mutex.tail, tail, &node_)) {
                mutexp_ = &mutex;
                return true;
            }
        }
        return false;
    }
    INLINE void lock(Mutex& mutex) {
        assert(mutexp_ == nullptr);
        Node *prev = exchange_acquire(mutex.tail, &node_);
        if (prev != nullptr) {
            store(node_.wait, true);
            store_release(prev->next, &node_);
            while (load_acquire(node_.wait)) _mm_pause();
        }
        mutexp_ = &mutex;
    }
    INLINE void unlock() {
        assert(mutexp_ != nullptr);
        Node* next = load(node_.next);
        if (next == nullptr) {
            Node *node = &node_;
            if (compare_exchange_release(mutexp_->tail, node, nullptr)) {
                mutexp_ = nullptr;
                node_.reset();
                return;
            }
            while ((next = load(node_.next)) == nullptr) _mm_pause();
        }
        assert(next != nullptr);
        store_release(next->wait, false);
        mutexp_ = nullptr;
        node_.reset();
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
    INLINE XSMutex() : v_(0) {}
    INLINE void lock(Mode mode) {
        switch(mode) {
        case Mode::X:
            write_lock(); return;
        case Mode::S:
            read_lock(); return;
        default:
            assert(false);
        }
    }
    INLINE bool tryLock(Mode mode) {
        switch(mode) {
        case Mode::X:
            return write_trylock();
        case Mode::S:
            return read_trylock();
        default:
            assert(false);
            return false;
        }
    }
    /**
     * S --> X
     */
    INLINE bool tryUpgrade() {
        int v0 = load(v_);
        if (unlikely(v0 != 1)) return false;
        return compare_exchange_acquire(v_, v0, -1);
    }
    INLINE void upgrade() {
        int v0 = load(v_);
        for (;;) {
            _mm_pause();
            while (unlikely(v0 != 1)) {
                v0 = load(v_);
                continue;
            }
            if (likely(compare_exchange_acquire(v_, v0, -1))) {
                return;
            }
        }
    }
    INLINE void unlock(Mode mode) noexcept {
        switch(mode) {
        case Mode::Invalid:
            return;
        case Mode::X:
            write_unlock(); return;
        case Mode::S:
            read_unlock(); return;
        }
    }
    std::string str() const {
        return cybozu::util::formatString("XSMutex(%d)", load(v_));
    }

    INLINE void write_lock() {
        int v0 = load(v_);
        for (;;) {
            _mm_pause();
            if (unlikely(v0 != 0)) {
                v0 = load(v_);
                continue;
            }
            if (likely(compare_exchange_acquire(v_, v0, -1))) {
                return;
            }
        }
    }
    INLINE bool write_trylock() {
        // We should retry CAS.
        int v = load(v_);
        while (likely(v == 0)) {
            if (likely(compare_exchange_acquire(v_, v, -1))) {
                return true;
            }
        }
        return false;
    }
    INLINE void write_unlock() noexcept {
        int ret = fetch_add_rel(v_, 1);
        assert(ret == -1); unused(ret);
    }
    INLINE bool read_trylock() {
        // We should retry CAS.
        int v = load(v_);
        while (likely(v >= 0)) {
            if (likely(compare_exchange_acquire(v_, v, v + 1))) {
                return true;
            }
        }
        return false;
    }
    INLINE void read_lock() {
        int v0 = load(v_);
        for (;;) {
            _mm_pause();
            if (unlikely(v0 < 0)) {
                v0 = load(v_);
                continue;
            }
            if (likely(compare_exchange_acquire(v_, v0, v0 + 1))) {
                return;
            }
        }
    }
    INLINE void read_unlock() noexcept {
        int ret = fetch_sub_rel(v_, 1);
        assert(ret > 0); unused(ret);
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
    INLINE XSLock() : mutex_(nullptr), mode_(Mode::Invalid) {}
    XSLock(XSMutex& mutex, Mode mode) : XSLock() {
        lock(mutex, mode);
    }
    INLINE ~XSLock() noexcept { unlock(); }

    XSLock(const XSLock&) = delete;
    XSLock& operator=(const XSLock&) = delete;
    INLINE XSLock(XSLock&& rhs) noexcept : XSLock() { swap(rhs); }
    INLINE XSLock& operator=(XSLock&& rhs) noexcept { swap(rhs); return *this; }

    INLINE void lock(XSMutex& mutex, Mode mode) {
        assert(mode_ == Mode::Invalid);
        mutex.lock(mode);
        mutex_ = &mutex;
        mode_ = mode;
    }
    INLINE bool tryLock(XSMutex& mutex, Mode mode) {
        assert(mode_ == Mode::Invalid);
        if (unlikely(!mutex.tryLock(mode))) return false;
        mutex_ = &mutex;
        mode_ = mode;
        return true;
    }
    INLINE bool write_trylock(XSMutex& mutex) {
        if (unlikely(!mutex.write_trylock())) return false;
        mutex_ = &mutex;
        mode_ = Mode::X;
        return true;
    }
    INLINE bool read_trylock(XSMutex& mutex) {
        if (unlikely(!mutex.read_trylock())) return false;
        mutex_ = &mutex;
        mode_ = Mode::S;
        return true;
    }

    INLINE bool isShared() const {
        return mode_ == Mode::S;
    }
    INLINE bool tryUpgrade() {
        assert(mutex_);
        assert(mode_ == Mode::S);
        if (unlikely(!mutex_->tryUpgrade())) return false;
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
        if (likely(mode_ == Mode::Invalid)) {
            mutex_ = nullptr;
            return;
        }
        assert(mutex_);
        mutex_->unlock(mode_);
        init();
    }
    INLINE void write_unlock() noexcept {
        assert(mode_ == Mode::X); assert(mutex_);
        mutex_->write_unlock();
        init();
    }
    INLINE void read_unlock() noexcept {
        assert(mode_ == Mode::S); assert(mutex_);
        mutex_->read_unlock();
        init();
    }

    INLINE const Mutex* mutex() const { return mutex_; }
    INLINE Mutex* mutex() { return mutex_; }
    INLINE uintptr_t getMutexId() const { return uintptr_t(mutex_); }
    INLINE Mode mode() const { return mode_; }

    /*
     * This is used for dummy object to comparison.
     */
    INLINE void setMutex(Mutex *mutex) { mutex_ = mutex; }

private:
    INLINE void init() {
        mutex_ = nullptr;
        mode_ = Mode::Invalid;
    }
    INLINE void swap(XSLock& rhs) noexcept {
        std::swap(mutex_, rhs.mutex_);
        std::swap(mode_, rhs.mode_);
    }
};


}} //namespace cybozu::lock
