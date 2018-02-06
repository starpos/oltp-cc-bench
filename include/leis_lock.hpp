#pragma once
/**
 * @file
 * brief Leis2016 algorithm (reader-writer version).
 * @author Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 *
 * (C) 2016 Cybozu Labs, Inc.
 */

#include <map>
#include <vector>
#include "lock.hpp"
#include "sxql.hpp"
#include "cache_line_size.hpp"
#include "vector_payload.hpp"
#include "allocator.hpp"


namespace cybozu {
namespace lock {


struct MutexWithMcs
{
    using Mode = XSMutex::Mode;

    int obj;
    McsSpinlock::Mutex mcsMu;

    MutexWithMcs() : obj(0), mcsMu() {
    }

    // This is used for lock or upgrade.
    bool compareAndSwap(int& before, int after, int mode) {
        return __atomic_compare_exchange_n(
            &obj, &before, after, false, mode, __ATOMIC_RELAXED);
    }

    int atomicLoad() const {
        return __atomic_load_n(&obj, __ATOMIC_ACQUIRE);
    }

    // These are used for unlock.
    int atomicFetchAdd(int value, int mode) {
        return __atomic_fetch_add(&obj, value, mode);
    }
    int atomicFetchSub(int value, int mode) {
        return __atomic_fetch_sub(&obj, value, mode);
    }

    std::string str() const {
        return cybozu::util::formatString("MutexWithMcs(%d)", obj);
    }
};


#if 0
size_t getThreadId()
{
    std::hash<std::thread::id> hasher;
    return hasher(std::this_thread::get_id());
}
#endif


class LockWithMcs
{
public:
    using Mutex = MutexWithMcs;
    using Mode = Mutex::Mode;
private:
    Mutex *mutex_;
    Mode mode_;
public:
    LockWithMcs() : mutex_(nullptr), mode_(Mode::Invalid) {
    }
    LockWithMcs(Mutex* mutex, Mode mode) : LockWithMcs() {
        lock(mutex, mode);
        verify();
    }
    ~LockWithMcs() noexcept {
        unlock();
        verify();
    }
    LockWithMcs(const LockWithMcs&) = delete;
    LockWithMcs(LockWithMcs&& rhs) : LockWithMcs() { swap(rhs); verify(); }
    LockWithMcs& operator=(const LockWithMcs&) = delete;
    LockWithMcs& operator=(LockWithMcs&& rhs) { swap(rhs); verify(); return *this; }

    // debug
    void verify() const {
#if 0
        if (mode_ == Mode::X || mode_ == Mode::S) {
            assert(mutex_);
        } else {
            assert(mode_ == Mode::Invalid);
        }
#endif
    }
    std::string str() const {
        return cybozu::util::formatString("LockWithMcs mutex:%p mode:%hhu", mutex_, mode_);
    }

    void lock(Mutex* mutex, Mode mode) {
        verify();
        assert(mutex);
        assert(!mutex_);
        assert(mode_ == Mode::Invalid);

        mutex_ = mutex;
        mode_ = mode;
        int v = mutex_->atomicLoad();
        if (mode == Mode::X) {
            for (;;) {
                if (v != 0) waitForWrite(v);
                if (mutex_->compareAndSwap(v, -1, __ATOMIC_ACQUIRE)) {
                    return;
                }
            }
        }
        assert(mode == Mode::S);
        for (;;) {
            if (v < 0) waitForRead(v);
            if (mutex_->compareAndSwap(v, v + 1, __ATOMIC_ACQUIRE)) {
                return;
            }
        }
    }
    bool tryLock(Mutex* mutex, Mode mode) {
        verify();
        assert(mutex);
        assert(!mutex_);
        assert(mode_ == Mode::Invalid);
        assert(mode != Mode::Invalid);

        int v = mutex->atomicLoad();
        if (mode == Mode::X) {
            while (v == 0) {
                if (mutex->compareAndSwap(v, -1, __ATOMIC_ACQUIRE)) {
                    mutex_ = mutex;
                    mode_ = mode;
                    return true;
                }
                _mm_pause();
            }
            return false;
        }
        assert(mode == Mode::S);
        while (v >= 0) {
            if (mutex->compareAndSwap(v, v + 1, __ATOMIC_ACQUIRE)) {
                mutex_ = mutex;
                mode_ = mode;
                return true;
            }
            _mm_pause();
        }
        return false;
    }
    bool tryUpgrade() {
        verify();
        assert(mutex_);
        assert(mode_ == Mode::S);

        int v = mutex_->atomicLoad();
        while (v == 1) {
            if (mutex_->compareAndSwap(v, -1, __ATOMIC_RELAXED)) {
                mode_ = Mode::X;
                return true;
            }
            _mm_pause();
        }
        return false;
    }
    void unlock() noexcept {
        verify();
        if (mode_ == Mode::Invalid) {
            mutex_ = nullptr;
            return;
        }
        assert(mutex_);

        if (mode_ == Mode::X) {
            int ret = mutex_->atomicFetchAdd(1, __ATOMIC_RELEASE);
            unusedVar(ret);
            assert(ret == -1);
        } else {
            assert(mode_ == Mode::S);
            int ret = mutex_->atomicFetchSub(1, __ATOMIC_RELEASE);
            unusedVar(ret);
            assert(ret >= 1);
        }
        mode_ = Mode::Invalid;
        mutex_ = nullptr;
    }

    bool isShared() const { return mode_ == Mode::S; }
    const Mutex* mutex() const { return mutex_; }
    Mutex* mutex() { return mutex_; }
    uintptr_t getMutexId() const { return uintptr_t(mutex_); }
    Mode mode() const { return mode_; }

    // This is used for dummy object to comparison.
    void setMutex(Mutex *mutex) {
        mutex_ = mutex;
        mode_ = Mode::Invalid;
    }

private:
    void waitForWrite(int& v) {
        McsSpinlock lock(&mutex_->mcsMu);
        // Up to one thread can spin.
        v = mutex_->atomicLoad();
        while (v != 0) {
            _mm_pause();
            v = mutex_->atomicLoad();
        }
    }
    void waitForRead(int& v) {
        McsSpinlock lock(&mutex_->mcsMu);
        // Up to one thread can spin.
        v = mutex_->atomicLoad();
        while (v < 0) {
            _mm_pause();
            v = mutex_->atomicLoad();
        }
    }
    void swap(LockWithMcs& rhs) {
        rhs.verify();
        std::swap(mutex_, rhs.mutex_);
        std::swap(mode_, rhs.mode_);
    }
};


/**
 * Usage:
 *   Call lock() to lock a resource.
 *   If lock() returns true, continue your transaction.
 *   Otherwise, you must rollback transaction's change, then call recover(),
 *   then restart your transaction (some locks are kept).
 *   In the end of your transaction, call unlock() to release all locks.
 *
 * UseMap should be 0 or 1.
 * If 0, std::map will be used.
 * If 1, std::vector and sort will be used.
 *
 * Lock: XSLock, LockWithMcs, or SXQLock.
 *   XSLock is normal shared-exclusive lock.
 *   LockWithMcs is XSLock with a helper MCS lock.
 *   SXQLock is a shared eXclusive Queuing lock (original).
 */
template <bool UseMap, typename Lock>
class LeisLockSet
{
};


/**
 * Informatin for write set.
 */
struct LocalValInfo
{
    /*
     * If the lock is for read, the below values are empty.
     */
    size_t localValIdx; // UINT64_MAX means empty.
    void *sharedVal; // If localValIdx != UINT64_MAX, it must not be null.

    LocalValInfo() : localValIdx(UINT64_MAX), sharedVal(nullptr) {
    }
    LocalValInfo(size_t localValIdx0, void *sharedVal0)
        : localValIdx(localValIdx0), sharedVal(sharedVal0) {
    }
    LocalValInfo(const LocalValInfo&) = default;
    LocalValInfo& operator=(const LocalValInfo&) = default;
    LocalValInfo(LocalValInfo&& rhs) : LocalValInfo() {
        swap(rhs);
    }
    LocalValInfo& operator=(LocalValInfo&& rhs) {
        swap(rhs);
        return *this;
    }

    void set(size_t localValIdx0, void *sharedVal0) {
        localValIdx = localValIdx0;
        sharedVal = sharedVal0;
    }
    void reset() {
        localValIdx = UINT64_MAX;
        sharedVal = nullptr;
    }

    void swap(LocalValInfo& rhs) {
        std::swap(localValIdx, rhs.localValIdx);
        std::swap(sharedVal, rhs.sharedVal);
    }
};


/**
 * Lock object and localValInfo object.
 */
template <typename Lock>
struct OpEntry
{
    Lock lock;
    LocalValInfo info;

    OpEntry() : lock(), info() {
    }
    explicit OpEntry(Lock&& lock0) : OpEntry() {
        lock = std::move(lock0);
    }
    OpEntry(const OpEntry&) = delete;
    OpEntry& operator=(const OpEntry&) = delete;
    OpEntry(OpEntry&& rhs) : OpEntry() {
        swap(rhs);
    }
    OpEntry& operator=(OpEntry&& rhs) {
        swap(rhs);
        return *this;
    }
    void swap(OpEntry& rhs) {
        std::swap(lock, rhs.lock);
        std::swap(info, rhs.info);
    }
};


/*
 * Using std::map.
 */
template <typename Lock>
class LeisLockSet<1, Lock>
{
public:
    using Mutex = typename Lock::Mutex;
    using Mode = typename Mutex::Mode;
    using OpEntryL = OpEntry<Lock>;

private:
    using Map = std::map<Mutex*, OpEntryL>;
    Map map_;

    // Temporary used in recover().
    std::vector<bool> tmpIsReadV_;
    std::vector<LocalValInfo> tmpInfoV_;

    // Temporary variables for lock() and recover().
    Mutex *mutex_;
    Mode mode_;
    void *sharedVal_;
    typename Map::iterator bgn_; // begin of M.

    MemoryVector local_;
    size_t valueSize_;

public:
    LeisLockSet() = default;
    ~LeisLockSet() noexcept {
        unlock();
    }
    /**
     * You must call this at first.
     */
    void init(size_t valueSize) {
        valueSize_ = valueSize;

        if (valueSize == 0) valueSize++;
#ifdef MUTEX_ON_CACHELINE
        local_.setSizes(valueSize, CACHE_LINE_SIZE);
#else
        local_.setSizes(valueSize);
#endif
    }

    bool lock(Mutex *mutex, Mode mode, void* sharedVal, void*& localVal) {
        unused(sharedVal); unused(localVal);
        typename Map::iterator it = map_.lower_bound(mutex);
        if (it == map_.end()) {
            // M is empty.
            // Lock object constructor may block.
            auto pair = map_.emplace(mutex, OpEntryL(Lock(mutex, mode)));
            assert(pair.second);
            OpEntryL& ope = pair.first->second;
            if (mode == Mode::X) {
                ope.info.set(allocateLocalVal(), sharedVal);
            }
            localVal = getLocalValPtr(ope.info);
            return true;
        }
        Mutex *mu = it->first;
        if (mu == mutex) { // already exists
            OpEntryL& ope = it->second;
            Lock& lk = ope.lock;
            if (mode == Mode::X && lk.isShared()) {
                if (lk.tryUpgrade()) {
                    ope.info.set(allocateLocalVal(), sharedVal);
                    localVal = getLocalValPtr(ope.info);
                    return true;
                } else {
                    // goto retrospective mode.
                }
            } else {
                localVal = getLocalValPtr(ope.info);
                return true;
            }
        } else { // not found.
            Lock lk;
            if (lk.tryLock(mutex, mode)) { // non-blocking.
                auto pair = map_.emplace(mutex, OpEntryL(std::move(lk)));
                assert(pair.second);
                OpEntryL& ope = pair.first->second;
                if (mode == Mode::X) {
                    ope.info.set(allocateLocalVal(), sharedVal);
                }
                localVal = getLocalValPtr(ope.info);
                return true;
            } else {
                // goto retrospecitive mode.
            }
        }

        // Retrospective mode.
        mutex_ = mutex;
        mode_ = mode;
        sharedVal_ = sharedVal;
        bgn_ = it;
        return false;
    }
    void recover() {
        // release locks in M.
        assert(tmpIsReadV_.empty());
        assert(tmpInfoV_.empty());
        typename Map::iterator it = bgn_;
        while (it != map_.end()) {
            OpEntryL& ope = it->second;
            Lock& lk = ope.lock;
            LocalValInfo& info = ope.info;
            tmpIsReadV_.push_back(lk.isShared());
            lk.unlock();
            tmpInfoV_.emplace_back(info);
            info.reset();
            ++it;
        }

        // lock the target.
        it = bgn_;
        size_t i = 0;
        if (bgn_ != map_.end() && bgn_->first == mutex_) {
            OpEntryL& ope = it->second;
            Lock& lk = ope.lock;
            assert(tmpIsReadV_[0]); // isShared.
            assert(mode_ == Mode::X);
            lk.lock(mutex_, mode_); // blocking.
            ope.info.set(allocateLocalVal(), sharedVal_);
            ++it;
            i = 1;
        } else {
            auto pair = map_.emplace(mutex_, Lock(mutex_, mode_)); // blocking
            assert(pair.second);
            OpEntryL& ope = pair.first->second;
            if (!ope.lock.isShared()) {
                ope.info.set(allocateLocalVal(), sharedVal_);
            }
        }

        assert(tmpInfoV_.size() == tmpIsReadV_.size());

        // re-lock mutexes in M.
        while (it != map_.end()) {
            assert(i < tmpIsReadV_.size());
            assert(i < tmpInfoV_.size());
            Mutex *mu = it->first;
            OpEntryL& ope = it->second;
            Lock& lk = ope.lock;
            lk.lock(mu, tmpIsReadV_[i] ? Mode::S : Mode::X); // blocking
            if (!lk.isShared()) {
                ope.info = tmpInfoV_[i];
            }
            ++it;
            ++i;
        }
        tmpIsReadV_.clear();
        tmpInfoV_.clear();
    }
    /**
     * Writeback local writeset and unlock.
     */
    void updateAndUnlock() {
        // here is serialized point.

        typename Map::iterator it = map_.begin();
        while (it != map_.end()) {
            OpEntryL& ope = it->second;
            if (!ope.lock.isShared()) {
                // Update the shared value.
#ifndef NO_PAYLOAD
                LocalValInfo& info = ope.info;
                assert(info.localValIdx != UINT64_MAX);
                assert(info.sharedVal != nullptr);
                ::memcpy(info.sharedVal, &local_[info.localValIdx], valueSize_);
#endif
            }
            it = map_.erase(it); // unlock
        }
        assert(map_.empty());
        local_.clear();
    }
    /**
     * If you want to abort, use this instead of updateAndUnlock().
     */
    void unlock() noexcept {
#if 1
        map_.clear(); // automatically unlocked in their destructor.
#else
        // debug code.
        typename Map::iterator it = map_.begin();
        while (it != map_.end()) {
            Mutex *mu = it->first;
            it = map_.erase(it); // unlock
        }
#endif
        local_.clear();
    }
    bool empty() const {
        return map_.empty();
    }
    size_t size() const {
        return map_.size();
    }

    void printVec(const char *prefix) const {
        ::printf("%p %s BEGIN\n", this, prefix);
        for (const typename Map::value_type& pair : map_) {
            OpEntryL& ope = pair.second;
            const Lock& lk = ope.lock;
            ::printf("%p %s mutex:%p %s shared:%p localIdx:%zu\n"
                     , this, prefix, lk.mutex(), lk.mutex()->str().c_str()
                     , ope.sharedVal, ope.localValIdx);
        }
        ::printf("%p %s END\n", this, prefix);
    }
private:
    size_t allocateLocalVal() {
        const size_t idx = local_.size();
#ifndef NO_PAYLOAD
        local_.resize(idx + 1);
#endif
        return idx;
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
};


/*
 * Using std::vector and sort.
 */
template <typename Lock>
class LeisLockSet<0, Lock>
{
public:
    using Mutex = typename Lock::Mutex;
    using Mode = typename Mutex::Mode;
    using OpEntryL = OpEntry<Lock>;

private:
    using Vec = std::vector<OpEntryL>;
    Vec vec_;
    uintptr_t maxMutex_;
    size_t nrSorted_;

    MemoryVector local_;
    size_t valueSize_;

    std::vector<uintptr_t> tmpMutexV_; // mutex pointers.
    std::vector<bool> tmpIsReadV_; // true for Mode::S, false for Mode::X.
    std::vector<LocalValInfo> tmpInfoV_; // local val info for write set.
    void *tmpSharedVal_; // temporary shared val for recovery.

public:
    LeisLockSet() : vec_(), maxMutex_(0), nrSorted_(0)
                  , tmpMutexV_(), tmpIsReadV_(), tmpInfoV_()
                  , tmpSharedVal_(nullptr) {
    }
    ~LeisLockSet() noexcept {
        unlock();
    }

    /**
     * You must call this at first.
     */
    void init(size_t valueSize) {
        valueSize_ = valueSize;

        if (valueSize == 0) valueSize++;
#ifdef MUTEX_ON_CACHELINE
        local_.setSizes(valueSize, CACHE_LINE_SIZE);
#else
        local_.setSizes(valueSize);
#endif
    }

    bool lock(Mutex *mutex, Mode mode, void* sharedVal, void*& localVal) {
        unused(sharedVal); unused(localVal);
        if (maxMutex_ < uintptr_t(mutex)) {
            // Lock order is preserved.
            vec_.emplace_back(OpEntryL(Lock(mutex, mode)));
            maxMutex_ = uintptr_t(mutex);
            if (nrSorted_ + 1 == vec_.size()) {
                nrSorted_++;
            }
            OpEntryL& ope = vec_.back();
            if (mode == Mode::X) {
                ope.info.set(allocateLocalVal(), sharedVal);
            }
            localVal = getLocalValPtr(ope.info);
            return true;
        }
        typename Vec::iterator it = find(mutex);
        if (it != vec_.end()) {
            OpEntryL& ope = *it;
            Lock& lk = ope.lock;
            if (mode == Mode::X && lk.isShared()) {
                if (lk.tryUpgrade()) {
                    ope.info.set(allocateLocalVal(), sharedVal);
                    localVal = getLocalValPtr(ope.info);
                    return true;
                } else {
                    // Go to retrospective mode.
                }
            } else {
                // Already locked.
                localVal = getLocalValPtr(ope.info);
                return true;
            }
        } else {
            Lock lk;
            if (lk.tryLock(mutex, mode)) {
                vec_.push_back(OpEntry(std::move(lk)));
                OpEntryL& ope = vec_.back();
                if (mode == Mode::X) {
                    ope.info.set(allocateLocalVal(), sharedVal);
                }
                localVal = getLocalValPtr(ope.info);
                return true;
            }
            // Go to retrospective mode.
        }

        assert(tmpMutexV_.empty());
        assert(tmpIsReadV_.empty());
        assert(tmpInfoV_.empty());
        tmpMutexV_.push_back(uintptr_t(mutex));
        tmpIsReadV_.push_back(mode == Mode::S);
        if (mode == Mode::X) {
            tmpSharedVal_ = sharedVal;
        }
        return false;
    }
    void recover() {
        /*
         * Sort and get the position,
         * all mutexes after which is no more than mutex_.
         */
        std::sort(
            vec_.begin(), vec_.end(),
            [](const OpEntryL& a, const OpEntryL& b) {
                return a.lock.getMutexId() < b.lock.getMutexId();
            });
        Mutex *mutexCmp = (Mutex *)tmpMutexV_[0];
        typename Vec::iterator it = lower_bound(mutexCmp, vec_.end());
        assert(it != vec_.end());

        // QQQQQ


        // Release locks.
        assert(tmpMutexV_.size() == 1);
        assert(tmpIsReadV_.size() == 1);
        assert(tmpInfoV_.empty());
        const bool hasTarget = it->lock.getMutexId() == uintptr_t(mutexCmp);
        const size_t nr0 = std::distance(vec_.begin(), it);
        const size_t nr1 = vec_.size() - nr0;
        tmpMutexV_.reserve(nr1 + 1);
        tmpIsReadV_.reserve(nr1 + 1);
        tmpInfoV_.reserve(nr1 + 1); // nr1 + 1 is upper bound because only write locks uses it.
        if (hasTarget) {
            assert(it->lock.isShared()); // it had read lock.
            assert(!tmpIsReadV_[0]);  // it tried write lock and failed.
            tmpInfoV_.emplace_back(allocateLocalVal(), tmpSharedVal_);
            ++it; // the first item just after the target.
        } else if (!tmpIsReadV_[0]) {
            tmpInfoV_.emplace_back(allocateLocalVal(), tmpSharedVal_);
        }
        while (it != vec_.end()) {
            // Backup.
            Lock& lk = it->lock;
            tmpMutexV_.push_back(lk.getMutexId());
            tmpIsReadV_.push_back(lk.isShared());
            if (!lk.isShared()) tmpInfoV_.push_back(it->info);
            ++it;
        }
        vec_.resize(nr0); // unlock remaining objects.

        // Re-lock mutexes.
        if (!hasTarget) {
            // ....., target, it,.....
            //  nr0            nr1
            vec_.reserve(nr0 + nr1 + 1);
        } else {
            // ....., target, it,.....
            //  nr0           nr1-1
        }
        auto it1 = tmpMutexV_.begin();
        auto it2 = tmpIsReadV_.begin();
        auto it3 = tmpInfoV_.begin();
        while (it1 != tmpMutexV_.end()) {
            const Mode mode = *it2 ? Mode::S : Mode::X;
            vec_.emplace_back(Lock((Mutex *)*it1, mode));
            ++it1;
            ++it2;
            if (mode == Mode::X) {
                vec_.back().info = *it3;
                ++it3;
            }
        }
        assert(it2 == tmpIsReadV_.end());
        assert(it3 == tmpInfoV_.end());

        tmpMutexV_.clear();
        tmpIsReadV_.clear();
        tmpInfoV_.clear();
        maxMutex_ = vec_.back().lock.getMutexId();
        nrSorted_ = vec_.size();
    }
    void updateAndUnlock() noexcept {
        // here is serialized point.

        typename Vec::iterator it = vec_.begin();
        while (it != vec_.end()) {
            OpEntryL& ope = *it;
            if (!ope.lock.isShared()) {
                // Update the shared value.
#ifndef NO_PAYLOAD
                LocalValInfo& info = ope.info;
                assert(info.localValIdx != UINT64_MAX);
                assert(info.sharedVal != nullptr);
                ::memcpy(info.sharedVal, &local_[info.localValIdx], valueSize_);
#endif
            }
#if 1 // unlock one by one.
            OpEntry garbage(std::move(ope)); // will be unlocked.
#endif
            ++it;
        }
        vec_.clear();
        maxMutex_ = 0; // smaller than any valid pointers.
        nrSorted_ = 0;
        local_.clear();
    }
    void unlock() noexcept {
        vec_.clear();// automatically release the locks.
        maxMutex_ = 0; // smaller than any valid pointers.
        nrSorted_ = 0;
        local_.clear();
    }
    bool empty() const {
        return vec_.empty();
    }
    size_t size() const {
        return vec_.size();
    }
private:
    typename Vec::iterator find(Mutex *mutex) {
        const uintptr_t key = uintptr_t(mutex);
#if 1
        // Binary search in sorted area.
        typename Vec::iterator end = vec_.begin();
        std::advance(end, nrSorted_);
        typename Vec::iterator it = lower_bound(mutex, end);
        if (it != end && it->lock.getMutexId() == key) {
            // found.
            return it;
        }
        // Sequential search in unsorted area.
        it = end;
        while (it != vec_.end()) {
            if (it->lock.getMutexId() == key) return it;
            ++it;
        }
        return vec_.end();
#else
        return std::find_if(
            vec_.begin(), vec_.end(),
            [&](const OpEntryL& ope) {
                return ope.lock.getMutexId() == key;
            });
#endif
    }
    size_t allocateLocalVal() {
        const size_t idx = local_.size();
#ifndef NO_PAYLOAD
        local_.resize(idx + 1);
#endif
        return idx;
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
public:
    void printVec(const char *prefix) const {
        ::printf("%p %s BEGIN\n", this, prefix);
        for (const OpEntryL& ope : vec_) {
            ::printf("%p %s mutex:%p %s shared:%p localIdx:%zu\n"
                     , this, prefix, ope.lock.mutex(), ope.lock.mutex()->str().c_str()
                     , ope.sharedVal, ope.localValIdx);
        }
        ::printf("%p %s END\n", this, prefix);
    }
    typename Vec::iterator lower_bound(Mutex *mutex, typename Vec::iterator end) {
        Lock lkcmp;
        lkcmp.setMutex(mutex);
        OpEntryL ope(std::move(lkcmp));
        return std::lower_bound(
            vec_.begin(), end, ope,
            [](const OpEntryL& a, const OpEntryL& b) {
                return a.lock.getMutexId() < b.lock.getMutexId();
            });
    }
};


}} // namespace cybozu::lock
