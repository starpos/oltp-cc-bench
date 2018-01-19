#pragma once
/**
 * @file
 * @brief an optimistic concurrency control method of TicToc.
 * @author Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 *
 * (C) 2016 Cybozu Labs, Inc.
 */
#include <stdexcept>
#include <utility>
#include <vector>
#include <algorithm>
#include <cstring>
#include <unordered_map>
#include "lock.hpp"
#include "arch.hpp"
#include "vector_payload.hpp"


namespace cybozu {
namespace tictoc {


constexpr size_t CACHE_LINE_SIZE = 64;


struct TsWord
{
    union {
        uint64_t obj;
        struct {
            bool lock:1;
            uint16_t delta:15;
            uint64_t wts:48;
        };
    };

    TsWord() {}
    void init() {
        obj = 0;
    }
    TsWord(uint64_t v) : obj(v) {}
    operator uint64_t() const {
        return obj;
    }
    bool operator==(const TsWord& rhs) const {
        return obj == rhs.obj;
    }
    bool operator!=(const TsWord& rhs) const {
        return !operator==(rhs);
    }
    uint64_t rts() const { return wts + delta; }
};


struct Mutex
{
#if 0
#ifdef MUTEX_ON_CACHELINE
    alignas(CACHE_LINE_SIZE)
#endif
#else
    alignas(sizeof(uintptr_t))
#endif
    TsWord tsw;

    Mutex() : tsw() { tsw.init(); }
#if 0
    TsWord read() const {
        return tsw;
    }
#endif
#if 0
    TsWord atomicRead() const {
        return (TsWord)__atomic_load_n(&tsw.obj, __ATOMIC_RELAXED);
    }
#endif
    TsWord load() const {
        return (TsWord)__atomic_load_n(&tsw.obj, __ATOMIC_RELAXED);
    }
    TsWord loadAcquire() const {
        return (TsWord)__atomic_load_n(&tsw.obj, __ATOMIC_ACQUIRE);
    }
    // This is used in the write-lock phase.
    // Full fence is set at the last point of the phase.
    // So we need not fence with CAS.
    bool compareAndSwap(TsWord& expected, const TsWord desired) {
        return __atomic_compare_exchange_n(
            &tsw.obj, &expected.obj, desired.obj,
            false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
    }
#if 0
    void set(const TsWord& desired) {
        //tsw.obj = desired.obj;
        __atomic_store_n(&tsw.obj, desired.obj, __ATOMIC_RELAXED);
    }
#endif
    void storeRelease(const TsWord& desired) {
        __atomic_store_n(&tsw.obj, desired.obj, __ATOMIC_RELEASE);
    }
};


class Reader
{
    Mutex *mutex_;
    TsWord tsw_;
public:
    size_t localValIdx;

    Reader() : mutex_() {}
    Reader(Reader&& rhs) : Reader() { swap(rhs); }
    Reader& operator=(Reader&& rhs) { swap(rhs); return *this; }

    void set(Mutex *mutex, size_t localValIdx0) {
        mutex_ = mutex;
        localValIdx = localValIdx0;
    }

    uintptr_t getId() const {
        uintptr_t ret;
        ::memcpy(&ret, &mutex_, sizeof(uintptr_t));
        return ret;
    }
    uint64_t wts() const { return tsw_.wts; }

    void prepare() {
        assert(mutex_);
        spinForUnlocked();
    }
    void readFence() const {
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
    }
    bool isReadSucceeded() {
        assert(mutex_);
        const TsWord tsw = mutex_->load();
        assert(!tsw_.lock);
        const bool ret = tsw_ == tsw;
        tsw_ = tsw;
        return ret;
    }
    void prepareRetry() {
        assert(mutex_);
        if (!tsw_.lock) return;
        spinForUnlocked();
    }
    bool validate(uint64_t commitTs, bool isInWriteSet) {
        TsWord v1 = mutex_->loadAcquire();
        for (;;) {
            if (tsw_.wts != v1.wts || (v1.rts() <= commitTs && v1.lock && !isInWriteSet)) {
                return false;
            }
#if 1
            if (v1.rts() >= commitTs || isInWriteSet) break;
#else
            if (v1.rts() >= commitTs) break;
#endif
            uint64_t delta = commitTs - v1.wts;
            uint64_t shift = delta - (delta & 0x7fff);
            TsWord v2 = v1;
            v2.wts += shift;
            v2.delta = delta - shift;
            if (mutex_->compareAndSwap(v1, v2)) break;
        }
        return true;
    }
private:
    void spinForUnlocked() {
        for (;;) {
            tsw_ = mutex_->loadAcquire();
            if (!tsw_.lock) break;
            _mm_pause();
        }
    }
    void swap(Reader& rhs) {
        std::swap(mutex_, rhs.mutex_);
        std::swap(tsw_, rhs.tsw_);
        std::swap(localValIdx, rhs.localValIdx);
    }
};


class Writer
{
public:
    Mutex *mutex;
    // written data.
    void *sharedVal;
    size_t localValIdx;

    Writer() : mutex() {}
    explicit Writer(Mutex *mutex0) : mutex(mutex0) {}
    Writer(Writer&& rhs) : Writer() { swap(rhs); }
    Writer& operator=(Writer&& rhs) { swap(rhs); return *this; }

    void set(Mutex *mutex0, void *sharedVal0, size_t localValIdx0) {
        mutex = mutex0;
        sharedVal = sharedVal0;
        localValIdx = localValIdx0;
    }

    uintptr_t getId() const {
        uintptr_t ret;
        ::memcpy(&ret, &mutex, sizeof(uintptr_t));
        return ret;
    }
    operator uintptr_t() const { return getId(); }
    bool operator<(const Writer& rhs) { return getId() < rhs.getId(); }
private:
    void swap(Writer& rhs) {
        std::swap(mutex, rhs.mutex);
        std::swap(localValIdx, rhs.localValIdx);
        std::swap(sharedVal, rhs.sharedVal);
    }
};


class Lock
{
    Mutex *mutex_;
    TsWord tsw_; // locked state.
public:
    Lock() : mutex_(), tsw_() {}
    explicit Lock(Mutex *mutex) : Lock() {
        lock(mutex);
    }
    ~Lock() noexcept {
        unlock();
    }
    Lock(Lock&& rhs) : Lock() { swap(rhs); }
    Lock& operator=(Lock&& rhs) { swap(rhs); return *this; }

    uintptr_t getId() const {
        uintptr_t ret;
        ::memcpy(&ret, &mutex_, sizeof(uintptr_t));
        return ret;
    }
    operator uintptr_t() const { return getId(); }
    bool operator<(const Lock& rhs) { return getId() < rhs.getId(); }

    uint64_t rts() const { return tsw_.rts(); }

    bool tryLock(Mutex *mutex) {
        assert(!mutex_);
        assert(mutex);
        TsWord tsw0 = mutex->load();  // loadConsume
        if (tsw0.lock) return false;
        TsWord tsw1 = tsw0;
        tsw1.lock = true;
        if (!mutex->compareAndSwap(tsw0, tsw1)) return false;
        mutex_ = mutex;
        tsw_ = tsw1;
        return true;
    }
    void lock(Mutex *mutex) {
        assert(!mutex_);
        TsWord tsw0 = mutex->load();  // loadConsume
        TsWord tsw1;
        for (;;) {
            while (tsw0.lock) {
                _mm_pause();
                tsw0 = mutex->load(); // loadConsume
            }
            tsw1 = tsw0;
            tsw1.lock = true;
            if (mutex->compareAndSwap(tsw0, tsw1)) break;
        }
        tsw_ = tsw1;
        mutex_ = mutex;
    }
    void updateAndUnlock(uint64_t commitTs) {
        if (!mutex_) return;
        TsWord tsw0 = tsw_;
        assert(tsw0.lock);
        tsw0.lock = 0;
        tsw0.wts = commitTs;
        tsw0.delta = 0;
        mutex_->storeRelease(tsw0);
        mutex_ = nullptr;
    }
    void unlock() {
        if (!mutex_) return;
        TsWord tsw0 = tsw_;
        assert(tsw0.lock);
        tsw0.lock = 0;
        mutex_->storeRelease(tsw0);
        mutex_ = nullptr;
    }
private:
    void swap(Lock& rhs) {
        std::swap(mutex_, rhs.mutex_);
        std::swap(tsw_, rhs.tsw_);
    }
};

using ReadSet = std::vector<Reader>;
using WriteSet = std::vector<Writer>;
using LockSet = std::vector<Lock>;
using Flags = std::vector<bool>; // isInWriteSet array.



/**
 * Args:
 *   ls and flags are temporary data.
 *
 * Returns:
 *   true: you must commit.
 *   false: you must abort.
 */
bool preCommit(ReadSet& rs, WriteSet& ws, LockSet& ls, Flags& flags, MemoryVector& local, size_t valueSize)
{
    bool ret = false;

    // Lock Write Set.
    std::sort(ws.begin(), ws.end());
    assert(ls.empty());
    ls.reserve(ws.size());
    for (Writer& w : ws) {
        ls.emplace_back(w.mutex);
    }

    // Serialization point.
    __atomic_thread_fence(__ATOMIC_ACQ_REL);

    // Calculate isInWriteSet for all the readers.
    assert(flags.empty());
    flags.reserve(rs.size());
    for (size_t i = 0; i < rs.size(); i++) {
        const uintptr_t id = rs[i].getId();
        const bool found = std::binary_search(
            ws.begin(), ws.end(), id,
            [&](const uintptr_t& a, const uintptr_t& b) { return a < b; });
        flags.push_back(found);
    }

    // Compute the Commit Timestamp.
    uint64_t commitTs = 0;
    for (Lock& lk : ls) {
        commitTs = std::max(commitTs, lk.rts() + 1);
    }
    for (size_t i = 0; i < rs.size(); i++) {
        if (flags[i]) continue;
        commitTs = std::max(commitTs, rs[i].wts());
    }

    // Validate the Read Set.
    for (size_t i = 0; i < rs.size(); i++) {
        const bool validated = rs[i].validate(commitTs, flags[i]);
        if (!validated) goto fin;
    }

    // Write phase.
    {
        auto itLk = ls.begin();
        auto itW = ws.begin();
        while (itLk != ls.end()) {
            assert(itW != ws.end());
            // writeback
#ifndef NO_PAYLOAD
            ::memcpy(itW->sharedVal, &local[itW->localValIdx], valueSize);
#endif
            itLk->updateAndUnlock(commitTs);
            ++itLk;
            ++itW;
        }
    }
    ret = true;

  fin:
    ws.clear();
    rs.clear();
    ls.clear();
    flags.clear();
    local.clear();
    return ret;
}


class LocalSet
{
    ReadSet rs_;
    WriteSet ws_;
    LockSet ls_;
    Flags flags_;

    using Index = std::unordered_map<uintptr_t, size_t>;
    Index ridx_;
    Index widx_;

    MemoryVector local_; // stores local values of read/write set.
    size_t valueSize_;

public:
    void init(size_t valueSize) {
        valueSize_ = valueSize;

        // MemoryVector does not allow zero-size element.
        if (valueSize == 0) valueSize++;
#ifdef MUTEX_ON_CACHELINE
            local_.setSizes(valueSize, CACHE_LINE_SIZE);
#else
            local_.setSizes(valueSize);
#endif
    }

    void read(Mutex& mutex, void *sharedVal, void *localVal) {
        unused(sharedVal); unused(localVal);
        size_t localValIdx;
        ReadSet::iterator itR = findInReadSet(uintptr_t(&mutex));
        if (itR != rs_.end()) {
            localValIdx = itR->localValIdx;
        } else {
            WriteSet::iterator itW = findInWriteSet(uintptr_t(&mutex));
            if (itW == ws_.end()) {
                // allocate new local value area.
                localValIdx = local_.size();
#ifndef NO_PAYLOAD
                local_.resize(localValIdx + 1);
#endif
            } else {
                localValIdx = itW->localValIdx;
            }
            rs_.emplace_back();
            Reader& r = rs_.back();
            r.set(&mutex, localValIdx);
            r.prepare();
            for (;;) {
                // read shared data.
#ifndef NO_PAYLOAD
                ::memcpy(&local_[localValIdx], sharedVal, valueSize_);
#endif
                r.readFence();
                if (r.isReadSucceeded()) break;
                r.prepareRetry();
            }
        }
        // read local data.
#ifndef NO_PAYLOAD
        ::memcpy(localVal, &local_[localValIdx], valueSize_);
#endif
    }
    void write(Mutex& mutex, void *sharedVal, void *localVal) {
        unused(sharedVal); unused(localVal);
        size_t localValIdx;
        WriteSet::iterator itW = findInWriteSet(uintptr_t(&mutex));
        if (itW != ws_.end()) {
            localValIdx = itW->localValIdx;
        } else {
            ReadSet::iterator itR = findInReadSet(uintptr_t(&mutex));
            if (itR == rs_.end()) {
                // allocate new local value area.
                localValIdx = local_.size();
#ifndef NO_PAYLOAD
                local_.resize(localValIdx + 1);
#endif
            } else {
                localValIdx = itR->localValIdx;
            }
            ws_.emplace_back();
            Writer& w = ws_.back();
            w.set(&mutex, sharedVal, localValIdx);
        }
        // write local data.
#ifndef NO_PAYLOAD
        ::memcpy(&local_[localValIdx], localVal, valueSize_);
#endif
    }
    bool preCommit() {
        bool ret = cybozu::tictoc::preCommit(rs_, ws_, ls_, flags_, local_, valueSize_);
        ridx_.clear();
        widx_.clear();
        local_.clear();
        return ret;
    }
    void clear() {
        ws_.clear();
        rs_.clear();
        ls_.clear();
        flags_.clear();
        ridx_.clear();
        widx_.clear();
        local_.clear();
    }
private:
    ReadSet::iterator findInReadSet(uintptr_t key) {
        return findInSet(
            key, rs_, ridx_,
            [](const Reader& r) { return r.getId(); });
    }
    WriteSet::iterator findInWriteSet(uintptr_t key) {
        return findInSet(
            key, ws_, widx_,
            [](const Writer& w) { return w.getId(); });
    }
    template <typename Vector, typename Map, typename Func>
    typename Vector::iterator findInSet(uintptr_t key, Vector& vec, Map& map, Func&& func) {
        if (shouldUseIndex(vec)) {
            for (size_t i = map.size(); i < vec.size(); i++) {
                map[func(vec[i])] = i;
            }
            typename Map::iterator it = map.find(key);
            if (it == map.end()) {
                return vec.end();
            } else {
                size_t idx = it->second;
                return vec.begin() + idx;
            }
        }
        return std::find_if(
            vec.begin(), vec.end(),
            [&](const typename Vector::value_type& v) {
                return func(v) == key;
            });
    }
    template <typename Vector>
    bool shouldUseIndex(const Vector& vec) const {
        const size_t threshold = 2048 * 2 / sizeof(typename Vector::value_type);
        return vec.size() > threshold;
    }
};

}} // namespace cybozu::tictoc.
