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
#include "cache_line_size.hpp"
#include "lock.hpp"
#include "arch.hpp"
#include "vector_payload.hpp"
#include "allocator.hpp"
#include "inline.hpp"
#include "sleep.hpp"


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
    TsWord tsw_; // This is uninitialized at beginning and will be set in read success.
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
    INLINE const TsWord& local_tsw() const { return tsw_; }

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
        if (likely(!tsw_.lock)) return;
        spinForUnlocked();
    }
    /**
     * commitTs is estimated one.
     */
    INLINE bool pre_validate(uint64_t commitTs, bool isInWriteSet) const {
        if (unlikely(tsw_.rts() >= commitTs)) {
            return true;
        }
        TsWord v1 = mutex_->load_acquire();
        if (unlikely(tsw_.wts != v1.wts || (v1.rts() < commitTs && v1.lock && !isInWriteSet))) {
            return false;
        }
        return true;
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
        if (unlikely(tsw_.rts() >= commitTs)) {
            // tsw_.rts() <= v1.rts is invariant so we can avoid checking.
            assert(!isInWriteSet); // This must happen on read-only records.
#ifdef USE_TICTOC_RTS_COUNT
            read_count_++;
#endif
            return true;
        }
        TsWord v1 = mutex_->load_acquire();
        for (;;) {
            if (unlikely(tsw_.wts != v1.wts || (v1.rts() < commitTs && v1.lock && !isInWriteSet))) {
                /* In the original tictoc paper,
                   the predicate is
                   (tsw_.wts != v1.wts || (v1.rts() <= commitTs && v1.lock && !isInWriteSet).
                   If v1.rts() == commitTs, its read-only entry is valid
                   because v1.wts() <= commitTs <= v1.rts() is satisfied. */
                return false;
            }
            if (likely(v1.rts() >= commitTs || isInWriteSet)) {
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
            if (likely(mutex_->cas_relaxed(v1, v2))) {
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

private:
    TsWord tsw_; // used for preemptive verify.

public:
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
        tsw_.init();
    }

    INLINE uintptr_t getId() const { return uintptr_t(mutex); }
    INLINE operator uintptr_t() const { return getId(); }
    INLINE bool operator<(const Writer& rhs) { return getId() < rhs.getId(); }

    /**
     * This reads the mutex object.
     */
    INLINE TsWord load_tsw() {
        assert(mutex);
        TsWord tsw = mutex->load();
        set_local_tsw(tsw);
        return tsw;
    }
    /**
     * This may return the initialized TsWord which wts/rts is zero.
     */
    INLINE TsWord local_tsw() const { return tsw_; }
    INLINE void set_local_tsw(TsWord tsw) { tsw_ = tsw; }
private:
    INLINE void swap(Writer& rhs) noexcept {
        std::swap(mutex, rhs.mutex);
        std::swap(localValIdx, rhs.localValIdx);
        std::swap(sharedVal, rhs.sharedVal);
        std::swap(tsw_, rhs.tsw_);
    }
};


struct Lock
{
private:
    Mutex *mutexp_;
    TsWord tsw_; // locked state.
public:
    /**
     * tsw_ is uninitialized at first.
     * lock() or tryLock() wil set tsw_.
     */
    INLINE Lock() : mutexp_(nullptr), tsw_() {}
    INLINE ~Lock() noexcept { unlock(); }

    Lock(const Lock&) = delete;
    Lock& operator=(const Lock&) = delete;
    INLINE Lock(Lock&& rhs) noexcept : Lock() { swap(rhs); }
    INLINE Lock& operator=(Lock&& rhs) noexcept { swap(rhs); return *this; }

    INLINE uintptr_t getId() const { return uintptr_t(mutexp_); }
    INLINE operator uintptr_t() const { return getId(); }
    INLINE bool operator<(const Lock& rhs) { return getId() < rhs.getId(); }

    const TsWord& local_tsw() const { return tsw_; }

    INLINE bool tryLock(Mutex& mutex) {
        assert(!mutexp_);
        TsWord tsw0 = mutex.load();
        if (unlikely(tsw0.lock)) return false;
        TsWord tsw1 = tsw0;
        tsw1.lock = 1;
        if (unlikely(!mutex.cas_acq(tsw0, tsw1))) return false;
        mutexp_ = &mutex;
        tsw_ = tsw1;
        return true;
    }
    INLINE void lock(Mutex& mutex) {
        assert(!mutexp_);
        TsWord tsw0 = mutex.load();
        TsWord tsw1;
        for (;;) {
            if (unlikely(tsw0.lock)) tsw0 = waitFor(mutex);
            tsw1 = tsw0;
            tsw1.lock = 1;
            if (likely(mutex.cas_acq(tsw0, tsw1))) break;
        }
        tsw_ = tsw1;
        mutexp_ = &mutex;
    }
    INLINE void updateAndUnlock(uint64_t commitTs) {
        if (unlikely(!mutexp_)) return;
        TsWord tsw0 = tsw_;
        assert(tsw0.lock);
        tsw0.lock = 0;
        tsw0.wts = commitTs;
        tsw0.delta = 0;
        mutexp_->store_release(tsw0);
        mutexp_ = nullptr;
    }
    INLINE void unlock() {
        if (unlikely(!mutexp_)) return;
        TsWord tsw0 = tsw_;
        assert(tsw0.lock);
        tsw0.lock = 0;
        mutexp_->store_release(tsw0);
        mutexp_ = nullptr;
    }
private:
    INLINE void swap(Lock& rhs) noexcept {
        std::swap(mutexp_, rhs.mutexp_);
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


INLINE bool preemptive_verify(const ReadSet& rs, const WriteSet& ws, const Flags& flags)
{
    if (rs.empty() || ws.empty()) {
        // Preemptive verify is not required.
        // If rs is empty then verify is not required.
        // If ws is empty then we should do normal verify only.
        return true;
    }
    uint64_t commitTs = 0;
    for (const Reader& r : rs) {
        commitTs = std::max(commitTs, r.local_tsw().wts);
    }
    for (const Writer& w : ws) {
#if 0
        commitTs = std::max(commitTs, w.load_tsw().rts() + 1);
#else
        commitTs = std::max(commitTs, w.local_tsw().rts() + 1);
#endif
    }
    for (size_t i = 0; i < rs.size(); i++) {
        if (!rs[i].pre_validate(commitTs, flags[i])) return false;
    }
    return true;
}


enum class NoWaitMode : int {
    Wait = 0,    // Wait log lock released.
    NoWait1 = 1, // When trylock failed, abort/retry.
    Nowait2 = 2, // when trylock failed, retry from beginning of verify phase.
};



struct MonitorData
{
    size_t nr_preemptive_aborts;

    MonitorData() : nr_preemptive_aborts(0) {
    }
};


INLINE MonitorData& get_thread_local_monitor_data()
{
    thread_local CacheLineAligned<MonitorData> local;
    return local.value;
}


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
    MemoryVector& local, size_t valueSize, NoWaitMode nowait_mode,
    bool do_preemptive_verify)
{
    bool ret = false;
    uint64_t commitTs = 0;

    // Calculate isInWriteSet for all the readers.
    // WriteSet sort is required to avoid deadlock for lock waiting
    // or to do binary search.
    std::sort(ws.begin(), ws.end());
    assert(flags.empty());
    flags.reserve(rs.size());
    for (size_t i = 0; i < rs.size(); i++) {
        const uintptr_t id = rs[i].getId();
        const bool found = std::binary_search(
            ws.begin(), ws.end(), id,
            [&](uintptr_t a, uintptr_t b) { return a < b; });
        flags.push_back(found);
    }

    // Lock/trylock Write Set.
    ls.reserve(ws.size());
  retry_verify:
    assert(ls.empty());
    if (do_preemptive_verify && unlikely(!preemptive_verify(rs, ws, flags))) {
        get_thread_local_monitor_data().nr_preemptive_aborts++;
        goto fin;
    }
    for (Writer& w : ws) {
        Lock& lk = ls.emplace_back();
        if (nowait_mode == NoWaitMode::NoWait1) {
            if (unlikely(!lk.tryLock(*w.mutex))) goto fin;
        } else if (nowait_mode == NoWaitMode::Nowait2) {
            if (unlikely(!lk.tryLock(*w.mutex))) {
                ls.clear();
#if 0
                // The original algorithm in the paper sleeps 1us here.
                // It must effect like backoff.
                sleep_us(1);
#endif
                goto retry_verify;
            }
            w.set_local_tsw(lk.local_tsw()); // for preemptive verify.
        } else {
            assert(nowait_mode == NoWaitMode::Wait);
            lk.lock(*w.mutex);
        }
    }

    // store-load fence is required here in design.
    //   x86_64: 'lock cmpxchg' and mov (load) is not reordered.
    //   aarch64: stlxr and ldaxr is not reordered.
    // so explicit fence is not required on both architectures.

    // Compute the Commit Timestamp.
    for (Lock& lk : ls) {
        commitTs = std::max(commitTs, lk.local_tsw().rts() + 1);
    }
    for (size_t i = 0; i < rs.size(); i++) {
        if (flags[i]) continue;
        commitTs = std::max(commitTs, rs[i].local_tsw().wts);
    }

    // Validate the Read Set.
    for (size_t i = 0; i < rs.size(); i++) {
        if (unlikely(!rs[i].validate(commitTs, flags[i]))) goto fin;
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
    NoWaitMode nowait_mode_;
    bool do_preemptive_verify_;

public:
    INLINE LocalSet()
        : rs_(), ws_(), ls_(), flags_(), ridx_(), widx_(), local_()
        , valueSize_(), nowait_mode_(NoWaitMode::Wait)
        , do_preemptive_verify_(false) {}
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
    INLINE void setNowait(NoWaitMode nowait_mode) { nowait_mode_ = nowait_mode; }
    INLINE void set_do_preemptive_verify(bool do_preemptive_verify) {
        do_preemptive_verify_ = do_preemptive_verify;
    }

    INLINE void read(Mutex& mutex, void *sharedVal, void *dst) {
        unused(sharedVal); unused(dst);
        size_t lvidx; // local value index.
        ReadSet::iterator itR = findInReadSet(uintptr_t(&mutex));
        if (unlikely(itR != rs_.end())) {
            lvidx = itR->localValIdx;
        } else {
            WriteSet::iterator itW = findInWriteSet(uintptr_t(&mutex));
            if (unlikely(itW != ws_.end())) {
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
                    if (likely(r.isReadSucceeded())) break;
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
        if (unlikely(itW != ws_.end())) {
            lvidx = itW->localValIdx;
        } else {
            ReadSet::iterator itR = findInReadSet(uintptr_t(&mutex));
            if (likely(itR == rs_.end())) {
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
        bool ret = cybozu::tictoc::preCommit(
            rs_, ws_, ls_, flags_, local_, valueSize_,
            nowait_mode_, do_preemptive_verify_);
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
        if (unlikely(shouldUseIndex(vec))) {
            for (size_t i = map.size(); i < vec.size(); i++) {
                map[func(vec[i])] = i;
            }
            typename Map::iterator it = map.find(key);
            if (unlikely(it == map.end())) {
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
