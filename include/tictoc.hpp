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
#include "allocator.hpp"
#include "inline.hpp"


#if 0
#define USE_TICTOC_MCS
#else
#undef USE_TICTOC_MCS
#endif


namespace cybozu {
namespace tictoc {


struct TsWord
{
    union {
        uint64_t obj;
        struct {
            // This layout is for little endian.
            uint64_t lock:1;
            uint64_t delta:15;
            uint64_t wts:48;
        };
    };
    static constexpr uint64_t Shift_mask = (1U << 16) - 1;

    INLINE TsWord() = default;
    INLINE void init() { obj = 0; }
    INLINE TsWord(uint64_t v) : obj(v) {}
    INLINE operator uint64_t() const { return obj; }

    INLINE uint64_t rts() const { return wts + delta; }
};


static_assert(sizeof(TsWord) == sizeof(uint64_t));


struct Mutex
{
    TsWord tsw;
#ifdef USE_TICTOC_MCS
    cybozu::lock::McsSpinlock::Mutex mcs_mutex;
#endif

#ifdef USE_TICTOC_MCS
    INLINE Mutex() : tsw(), mcs_mutex() { tsw.init(); }
#else
    INLINE Mutex() : tsw() { tsw.init(); }
#endif

    INLINE TsWord load() const { return ::load(tsw); }
    INLINE TsWord load_acquire() const { return ::load_acquire(tsw); }
    INLINE void store_release(TsWord tsw0) { ::store_release(tsw, tsw0); }

    // This is used in the write-lock phase.
    // Full fence is set at the last point of the phase.
    // So we need not fence with CAS.
    INLINE bool cas_relaxed(TsWord& tsw0, TsWord tsw1) {
        return ::compare_exchange_relaxed(tsw, tsw0, tsw1);
    }
    INLINE bool cas_acq(TsWord& tsw0, TsWord tsw1) {
        return ::compare_exchange_acquire(tsw, tsw0, tsw1);
    }
    INLINE bool cas_rel(TsWord& tsw0, TsWord tsw1) {
        return ::compare_exchange_release(tsw, tsw0, tsw1);
    }
};


#if 0
#define USE_TICTOC_RTS_COUNT
#else
#undef USE_TICTOC_RTS_COUNT
#endif

#ifdef USE_TICTOC_RTS_COUNT
thread_local size_t update_rts_count_ = 0;
thread_local size_t read_count_ = 0;
#endif


struct Reader
{
private:
    Mutex *mutex_;
    TsWord tsw_;
public:
    size_t localValIdx;

    /**
     * You must call set() at first to set mutex_ and localValIdx.
     * prepare() will set tsw_.
     */
    INLINE Reader() = default;

    Reader(const Reader&) = delete;
    Reader& operator=(const Reader&) = delete;
    INLINE Reader(Reader&& rhs) noexcept : Reader() { swap(rhs); }
    INLINE Reader& operator=(Reader&& rhs) noexcept { swap(rhs); return *this; }

    INLINE void set(Mutex *mutex, size_t localValIdx0) {
        mutex_ = mutex;
        localValIdx = localValIdx0;
    }

    INLINE uintptr_t getId() const { return uintptr_t(mutex_); }
    INLINE uint64_t wts() const { return tsw_.wts; }

    INLINE void prepare() {
        assert(mutex_);
        spinForUnlocked();
    }
    INLINE void readFence() const { acquire_fence(); }
    INLINE bool isReadSucceeded() {
        assert(mutex_);
        const TsWord tsw = mutex_->load();
        assert(!tsw_.lock);
        const bool ret = (tsw_ == tsw);
        tsw_ = tsw;
        return ret;
    }
    INLINE void prepareRetry() {
        assert(mutex_);
        if (!tsw_.lock) return;
        spinForUnlocked();
    }
    INLINE bool validate(uint64_t commitTs, bool isInWriteSet) {
#if 0  // old algorithm until 20180222 (first check is missint...it's inefficient.)
        TsWord v1 = mutex_->load_acquire();
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
            uint64_t shift = delta - (delta & TsWord::Shift_mask);
            TsWord v2 = v1;
            v2.wts += shift;
            v2.delta = delta - shift;
            if (mutex_->cas_relaxed(v1, v2)) break;
        }
        return true;
#else  // algorithm from 20180222
        if (tsw_.rts() >= commitTs) {
            // tsw_.rts() <= v1.rts is invariant so we can avoid checking.
            assert(!isInWriteSet); // This must happen on read-only records.
#ifdef USE_TICTOC_RTS_COUNT
            read_count_++;
#endif
            return true;
        }
        TsWord v1 = mutex_->load_acquire();
        for (;;) {
            if (tsw_.wts != v1.wts || (v1.rts() < commitTs && v1.lock && !isInWriteSet)) {
                /* In the original tictoc paper,
                   the predicate is
                   (tsw_.wts != v1.wts || (v1.rts() <= commitTs && v1.lock && !isInWriteSet).
                   If v1.rts() == commitTs, its read-only entry is valid
                   because v1.wts() <= commitTs <= v1.rts() is satisfied. */
                return false;
            }
            if (v1.rts() >= commitTs || isInWriteSet) {
                /* In the original tictoc paper,
                 * the predicate is (v1.rts() > commitTs).
                 * However, if v1.rts() == commitTs then extend is not necessary.
                 * If isInWriteSet then it is not also
                 * because updateAndUnlock() will update the tsword.. */
#ifdef USE_TICTOC_RTS_COUNT
                read_count_++;
#endif
                return true;
            }
            // Try to extend rts.
            uint64_t delta = commitTs - v1.wts;
            uint64_t shift = delta - (delta & TsWord::Shift_mask);
            TsWord v2 = v1;
            v2.wts += shift;
            v2.delta = delta - shift;
            if (mutex_->cas_relaxed(v1, v2)) {
#ifdef USE_TICTOC_RTS_COUNT
                read_count_++;
                update_rts_count_++;
#endif
                return true;
            }
        }
#endif
    }
private:
    INLINE void spinForUnlocked() {
        TsWord tsw = mutex_->load_acquire();
        while (tsw.lock) {
            _mm_pause();
            tsw = mutex_->load_acquire();
        }
        tsw_ = tsw;
    }
    INLINE void swap(Reader& rhs) noexcept {
        std::swap(mutex_, rhs.mutex_);
        std::swap(tsw_, rhs.tsw_);
        std::swap(localValIdx, rhs.localValIdx);
    }
};


struct Writer
{
public:
    Mutex *mutex;
    // written data.
    void *sharedVal;
    size_t localValIdx;

    /**
     * All member fields are uninitialized.
     * Call set() at first.
     */
    INLINE Writer() = default;

    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;
    INLINE Writer(Writer&& rhs) noexcept : Writer() { swap(rhs); }
    INLINE Writer& operator=(Writer&& rhs) noexcept { swap(rhs); return *this; }

    INLINE void set(Mutex *mutex0, void *sharedVal0, size_t localValIdx0) {
        mutex = mutex0;
        sharedVal = sharedVal0;
        localValIdx = localValIdx0;
    }

    INLINE uintptr_t getId() const { return uintptr_t(mutex); }
    INLINE operator uintptr_t() const { return getId(); }
    INLINE bool operator<(const Writer& rhs) { return getId() < rhs.getId(); }
private:
    INLINE void swap(Writer& rhs) noexcept {
        std::swap(mutex, rhs.mutex);
        std::swap(localValIdx, rhs.localValIdx);
        std::swap(sharedVal, rhs.sharedVal);
    }
};


struct Lock
{
private:
    Mutex *mutex_;
    TsWord tsw_; // locked state.
public:
    /**
     * tsw_ is uninitialized at first.
     * lock() or tryLock() wil set tsw_.
     */
    INLINE Lock() : mutex_(nullptr) {}
    INLINE ~Lock() noexcept { unlock(); }

    Lock(const Lock&) = delete;
    Lock& operator=(const Lock&) = delete;
    INLINE Lock(Lock&& rhs) noexcept : Lock() { swap(rhs); }
    INLINE Lock& operator=(Lock&& rhs) noexcept { swap(rhs); return *this; }

    INLINE uintptr_t getId() const { return uintptr_t(mutex_); }
    INLINE operator uintptr_t() const { return getId(); }
    INLINE bool operator<(const Lock& rhs) { return getId() < rhs.getId(); }

    INLINE uint64_t rts() const { return tsw_.rts(); }

    INLINE bool tryLock(Mutex *mutex) {
        assert(!mutex_);
        assert(mutex);
        TsWord tsw0 = mutex->load();
        if (tsw0.lock) return false;
        TsWord tsw1 = tsw0;
        tsw1.lock = 1;
        if (!mutex->cas_acq(tsw0, tsw1)) return false;
        mutex_ = mutex;
        tsw_ = tsw1;
        return true;
    }
    INLINE void lock(Mutex *mutex) {
        assert(!mutex_);
        TsWord tsw0 = mutex->load();
        TsWord tsw1;
        for (;;) {
            while (tsw0.lock) tsw0 = waitFor(*mutex);
            tsw1 = tsw0;
            tsw1.lock = 1;
            if (mutex->cas_acq(tsw0, tsw1)) break;
        }
        tsw_ = tsw1;
        mutex_ = mutex;
    }
    INLINE void updateAndUnlock(uint64_t commitTs) {
        if (!mutex_) return;
        TsWord tsw0 = tsw_;
        assert(tsw0.lock);
        tsw0.lock = 0;
        tsw0.wts = commitTs;
        tsw0.delta = 0;
        mutex_->store_release(tsw0);
        mutex_ = nullptr;
    }
    INLINE void unlock() {
        if (!mutex_) return;
        TsWord tsw0 = tsw_;
        assert(tsw0.lock);
        tsw0.lock = 0;
        mutex_->store_release(tsw0);
        mutex_ = nullptr;
    }
private:
    INLINE void swap(Lock& rhs) noexcept {
        std::swap(mutex_, rhs.mutex_);
        std::swap(tsw_, rhs.tsw_);
    }
    INLINE TsWord waitFor(Mutex& mutex) {
#ifdef USE_TICTOC_MCS
        cybozu::lock::McsSpinlock lk(mutex.mcs_mutex);
#endif
        TsWord tsw0 = mutex.load();
        while (tsw0.lock) {
            _mm_pause();
            tsw0 = mutex.load();
        }
        return tsw0;
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
INLINE bool preCommit(
    ReadSet& rs, WriteSet& ws, LockSet& ls, Flags& flags,
    MemoryVector& local, size_t valueSize, bool nowait)
{
    bool ret = false;
    uint64_t commitTs = 0;

    // Lock Write Set.
    std::sort(ws.begin(), ws.end());
    assert(ls.empty());
    ls.reserve(ws.size());
    for (Writer& w : ws) {
        Lock& lk = ls.emplace_back();
        if (nowait) {
            if (!lk.tryLock(w.mutex)) goto fin;
        } else {
            lk.lock(w.mutex);
        }
    }

    // Serialization point.
    SERIALIZATION_POINT_BARRIER();

    // Calculate isInWriteSet for all the readers.
    assert(flags.empty());
    flags.reserve(rs.size());
    for (size_t i = 0; i < rs.size(); i++) {
        const uintptr_t id = rs[i].getId();
        const bool found = std::binary_search(
            ws.begin(), ws.end(), id,
            [&](uintptr_t a, uintptr_t b) { return a < b; });
        flags.push_back(found);
    }

    // Compute the Commit Timestamp.
    for (Lock& lk : ls) {
        commitTs = std::max(commitTs, lk.rts() + 1);
    }
    for (size_t i = 0; i < rs.size(); i++) {
        if (flags[i]) continue;
        commitTs = std::max(commitTs, rs[i].wts());
    }

    // Validate the Read Set.
    for (size_t i = 0; i < rs.size(); i++) {
        if (!rs[i].validate(commitTs, flags[i])) goto fin;
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
#else
            unused(valueSize);
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

#if 1
    using Index = SingleThreadUnorderedMap<uintptr_t, size_t>;
#else
    using Index = std::unordered_map<uintptr_t, size_t>;
#endif
    Index ridx_;
    Index widx_;

    MemoryVector local_; // stores local values of read/write set.
    size_t valueSize_;
    bool nowait_;

public:
    INLINE LocalSet() : rs_(), ws_(), ls_(), flags_(), ridx_(), widx_(), local_(), valueSize_(), nowait_(false) {}
    INLINE void init(size_t valueSize, size_t nrReserve) {
        valueSize_ = valueSize;

        // MemoryVector does not allow zero-size element.
        if (valueSize == 0) valueSize++;
        local_.setSizes(valueSize);

        // for long transactions.
        rs_.reserve(nrReserve);
        ws_.reserve(nrReserve);
        ls_.reserve(nrReserve);
        flags_.reserve(nrReserve);
        local_.reserve(nrReserve);
    }
    INLINE void setNowait(bool nowait) { nowait_ = nowait; }

    INLINE void read(Mutex& mutex, void *sharedVal, void *dst) {
        unused(sharedVal); unused(dst);
        size_t lvidx; // local value index.
        ReadSet::iterator itR = findInReadSet(uintptr_t(&mutex));
        if (itR != rs_.end()) {
            lvidx = itR->localValIdx;
        } else {
            WriteSet::iterator itW = findInWriteSet(uintptr_t(&mutex));
            if (itW != ws_.end()) {
                // This is blind-written entry.
                lvidx = itW->localValIdx;
            } else {
                // allocate new local value area.
                lvidx = allocateLocalVal();
                Reader& r = rs_.emplace_back();
                r.set(&mutex, lvidx);
                r.prepare();
                for (;;) {
                    copyValue(&local_[lvidx], sharedVal); // read shared
                    r.readFence();
                    if (r.isReadSucceeded()) break;
                    r.prepareRetry();
                }
            }
        }
        copyValue(dst, &local_[lvidx]); // read local
    }
    INLINE void write(Mutex& mutex, void *sharedVal, const void *src) {
        unused(sharedVal); unused(src);
        size_t lvidx;
        WriteSet::iterator itW = findInWriteSet(uintptr_t(&mutex));
        if (itW != ws_.end()) {
            lvidx = itW->localValIdx;
        } else {
            ReadSet::iterator itR = findInReadSet(uintptr_t(&mutex));
            if (itR == rs_.end()) {
                lvidx = allocateLocalVal();
            } else {
                lvidx = itR->localValIdx;
            }
            Writer& w = ws_.emplace_back();
            w.set(&mutex, sharedVal, lvidx);
        }
        copyValue(&local_[lvidx], src); // write local
    }
    INLINE bool preCommit() {
        bool ret = cybozu::tictoc::preCommit(rs_, ws_, ls_, flags_, local_, valueSize_, nowait_);
        ridx_.clear();
        widx_.clear();
        local_.clear();
        return ret;
    }
    INLINE void clear() {
        ws_.clear();
        rs_.clear();
        ls_.clear();
        flags_.clear();
        ridx_.clear();
        widx_.clear();
        local_.clear();
    }
private:
    INLINE ReadSet::iterator findInReadSet(uintptr_t key) {
        return findInSet(
            key, rs_, ridx_,
            [](const Reader& r) { return r.getId(); });
    }
    INLINE WriteSet::iterator findInWriteSet(uintptr_t key) {
        return findInSet(
            key, ws_, widx_,
            [](const Writer& w) { return w.getId(); });
    }
    template <typename Vector, typename Map, typename Func>
    INLINE typename Vector::iterator findInSet(uintptr_t key, Vector& vec, Map& map, Func&& func) {
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
    INLINE bool shouldUseIndex(const Vector& vec) const {
        constexpr size_t threshold = 4096 / sizeof(typename Vector::value_type);
        return vec.size() > threshold;
    }
    INLINE void copyValue(void* dst, const void* src) {
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

}} // namespace cybozu::tictoc.
