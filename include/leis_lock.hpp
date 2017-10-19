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

#if 0
// XSLock with helper MCS lock
#define USE_LEIS_MCS
#undef USE_LEIS_SXQL
#elif 0
// Shared eXclusive Queuing Lock (original)
#include "sxql.hpp"
#undef USE_LEIS_MCS
#define USE_LEIS_SXQL
#else
// Normal XSLock
#endif



namespace cybozu {
namespace lock {



struct MutexWithMcs
{
    using Mode = XSMutex::Mode;

    int obj;
    McsSpinlock::Mutex mcsMu;

    MutexWithMcs() : obj(0), mcsMu() {
    }

    bool compareAndSwap(int& before, int after) {
        return __atomic_compare_exchange_n(&obj, &before, after, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
    }
    int atomicLoad() const {
        return __atomic_load_n(&obj, __ATOMIC_ACQUIRE);
    }
    void store(int after) {
        obj = after;
        __atomic_thread_fence(__ATOMIC_RELEASE);
    }
    int atomicFetchAdd(int value) {
        return __atomic_fetch_add(&obj, value, __ATOMIC_RELEASE);
    }
    int atomicFetchSub(int value) {
        return __atomic_fetch_sub(&obj, value, __ATOMIC_RELEASE);
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
                if (mutex_->compareAndSwap(v, -1)) {
                    return;
                }
            }
        }
        assert(mode == Mode::S);
        for (;;) {
            if (v < 0) waitForRead(v);
            if (mutex_->compareAndSwap(v, v + 1)) {
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
                if (mutex->compareAndSwap(v, -1)) {
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
            if (mutex->compareAndSwap(v, v + 1)) {
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
            if (mutex_->compareAndSwap(v, -1)) {
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
            int ret = mutex_->atomicFetchAdd(1);
            unusedVar(ret);
            //mutex_->store(0);
            assert(ret == -1);
        } else {
            assert(mode_ == Mode::S);
            int ret = mutex_->atomicFetchSub(1);
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
 */
template <bool UseMap>
class LeisLockSet
{
};


/*
 * Use std::map.
 */
template <>
class LeisLockSet<1>
{
public:
#ifdef USE_LEIS_MCS
    using Lock = LockWithMcs;
    using Mutex = Lock::Mutex;
    using Mode = Mutex::Mode;
#elif defined(USE_LEIS_SXQL)
    using Lock = SXQLock;
    using Mutex = Lock::Mutex;
    using Mode = Mutex::Mode;
#else
    using Lock = XSLock;
    using Mutex = Lock::Mutex;
    using Mode = Mutex::Mode;
#endif

private:
    using Map = std::map<Mutex*, Lock>;
    Map map_;

    // Temporary used in recover().
    std::vector<bool> tmp_;

    // Temporary variables for lock() and recover().
    Mutex *mutex_;
    Mode mode_;
    Map::iterator bgn_; // begin of M.

public:
    LeisLockSet() = default;
    ~LeisLockSet() noexcept {
        unlock();
    }
    bool lock(Mutex *mutex, Mode mode) {
        Map::iterator it = map_.lower_bound(mutex);
        if (it == map_.end()) {
            // M is empty.
            map_.emplace(mutex, Lock(mutex, mode)); // blocking
            return true;
        }
        Mutex *mu = it->first;
        if (mu == mutex) { // already exists
            Lock& lk = it->second;
            if (mode == Mode::X && lk.isShared()) {
                if (lk.tryUpgrade()) {
                    return true;
                }
            } else {
                return true;
            }
        } else {
            Lock lk;
            if (lk.tryLock(mutex, mode)) { // non-blocking.
                map_.emplace(mutex, std::move(lk));
                return true;
            }
        }

        // Retrospective mode.
        mutex_ = mutex;
        mode_ = mode;
        bgn_ = it;
        return false;
    }
    void recover() {
        // release locks in M.
        assert(tmp_.empty());
        Map::iterator it = bgn_;
        while (it != map_.end()) {
            Lock& lk = it->second;
            tmp_.push_back(lk.isShared());
            lk.unlock();
            ++it;
        }

        // lock the target.
        it = bgn_;
        size_t i = 0;
        if (bgn_ != map_.end() && bgn_->first == mutex_) {
            Lock& lk = it->second;
            assert(tmp_[0]); // isShared.
            assert(mode_ == Mode::X);
            lk.lock(mutex_, mode_); // blocking.
            ++it;
            i = 1;
        } else {
            map_.emplace(mutex_, Lock(mutex_, mode_)); // blocking
        }

        // re-lock mutexes in M.
        while (it != map_.end()) {
            Mutex *mu = it->first;
            Lock& lk = it->second;
            assert(i < tmp_.size());
            lk.lock(mu, tmp_[i] ? Mode::S : Mode::X); // blocking
            ++it;
            ++i;
        }

        tmp_.clear();
    }
    void unlock() noexcept {
#if 1
        map_.clear(); // automatically unlocked in their destructor.
#else
        // debug code.
        Map::iterator it = map_.begin();
        while (it != map_.end()) {
            Mutex *mu = it->first;
            it = map_.erase(it);
        }
#endif
    }
    bool empty() const {
        return map_.empty();
    }
    size_t size() const {
        return map_.size();
    }

    void printLockV(const char *prefix) const {
        ::printf("%p %s BEGIN\n", this, prefix);
        for (const Map::value_type& pair : map_) {
            const Lock& lk = pair.second;
            ::printf("%p %s mutex:%p %s\n"
                     , this, prefix, lk.mutex(), lk.mutex()->str().c_str());
        }
        ::printf("%p %s END\n", this, prefix);
    }
};


/*
 * Use std::vector and sort.
 */
template <>
class LeisLockSet<0>
{
public:
#ifdef USE_LEIS_MCS
    using Lock = LockWithMcs;
    using Mutex = Lock::Mutex;
    using Mode = Mutex::Mode;
#elif defined(USE_LEIS_SXQL)
    using Lock = SXQLock;
    using Mutex = Lock::Mutex;
    using Mode = Mutex::Mode;
#else
    using Lock = XSLock;
    using Mutex = Lock::Mutex;
    using Mode = Mutex::Mode;
#endif

private:
    using LockV = std::vector<Lock>;
    LockV lockV_;
    uintptr_t maxMutex_;
    size_t nrSorted_;

    std::vector<uintptr_t> tmpMutexV_; // mutex pointers.
    std::vector<bool> tmpIsReadV_; // true for Mode::S, false for Mode::X.

public:
    LeisLockSet() : lockV_(), maxMutex_(0), nrSorted_(0)
                  , tmpMutexV_(), tmpIsReadV_() {}
    ~LeisLockSet() noexcept {
        unlock();
    }
    bool lock(Mutex *mutex, Mode mode) {
        if (maxMutex_ < uintptr_t(mutex)) {
            // Lock order is preserved.
            lockV_.emplace_back(mutex, mode);
            maxMutex_ = uintptr_t(mutex);
            if (nrSorted_ + 1 == lockV_.size()) {
                nrSorted_++;
            }
            return true;
        }
        LockV::iterator it = find(mutex);
        if (it != lockV_.end()) {
            Lock& lk = *it;
            if (mode == Mode::X && lk.isShared()) {
                if (lk.tryUpgrade()) {
                    return true;
                } else {
                    // Go to retrospective mode.
                }
            } else {
                // Already locked.
                return true;
            }
        } else {
            Lock lk;
            if (lk.tryLock(mutex, mode)) {
                lockV_.push_back(std::move(lk));
                return true;
            }
            // Go to retrospective mode.
        }

        assert(tmpMutexV_.empty());
        assert(tmpIsReadV_.empty());
        tmpMutexV_.push_back(uintptr_t(mutex));
        tmpIsReadV_.push_back(mode == Mode::S);
        return false;
    }
    void recover() {
        /*
         * Sort and get the position,
         * all mutexes after which is no more than mutex_.
         */
        std::sort(
            lockV_.begin(), lockV_.end(),
            [](const Lock& a, const Lock& b) {
                return a.getMutexId() < b.getMutexId();
            });
        Mutex *mutexCmp = (Mutex *)tmpMutexV_[0];
        LockV::iterator it = lower_bound(mutexCmp, lockV_.end());
        assert(it != lockV_.end());

        // Release locks.
        assert(tmpMutexV_.size() == 1);
        assert(tmpIsReadV_.size() == 1);
        const bool hasTarget = it->getMutexId() == uintptr_t(mutexCmp);
        const size_t nr0 = std::distance(lockV_.begin(), it);
        const size_t nr1 = lockV_.size() - nr0;
        tmpMutexV_.reserve(nr1);
        tmpIsReadV_.reserve(nr1);
        if (hasTarget) {
            ++it; // skip.
        }
        while (it != lockV_.end()) {
            // Backup.
            tmpMutexV_.push_back(it->getMutexId());
            tmpIsReadV_.push_back(it->isShared());
            ++it;
        }
        lockV_.resize(nr0); // unlock remaining objects.

        // Re-lock mutexes.
        if (!hasTarget) {
            lockV_.reserve(nr0 + nr1 + 1);
        }
        auto it1 = tmpMutexV_.begin();
        auto it2 = tmpIsReadV_.begin();
        while (it1 != tmpMutexV_.end()) {
            lockV_.emplace_back((Mutex *)*it1, *it2 ? Mode::S : Mode::X);
            ++it1;
            ++it2;
        }
        assert(it2 == tmpIsReadV_.end());

        tmpMutexV_.clear();
        tmpIsReadV_.clear();
        maxMutex_ = lockV_.back().getMutexId();
        nrSorted_ = lockV_.size();

    }
    void unlock() noexcept {
        lockV_.clear();// automatically release the locks.
        maxMutex_ = 0; // smaller than any valid pointers.
        nrSorted_ = 0;
    }
    bool empty() const {
        return lockV_.empty();
    }
    size_t size() const {
        return lockV_.size();
    }
private:
    LockV::iterator find(Mutex *mutex) {
        const uintptr_t key = uintptr_t(mutex);
#if 1
        // Binary search in sorted area.
        LockV::iterator end = lockV_.begin();
        std::advance(end, nrSorted_);
        LockV::iterator it = lower_bound(mutex, end);
        if (it != end && it->getMutexId() == key) {
            // found.
            return it;
        }
        // Sequential search in unsorted area.
        it = end;
        while (it != lockV_.end()) {
            if (it->getMutexId() == key) return it;
            ++it;
        }
        return lockV_.end();
#else
        return std::find_if(
            lockV_.begin(), lockV_.end(),
            [&](const Lock& lk) {
                return lk.getMutexId() == key;
            });
#endif
    }
public:
    void printLockV(const char *prefix) const {
        ::printf("%p %s BEGIN\n", this, prefix);
        for (const Lock& lk : lockV_) {
            ::printf("%p %s mutex:%p %s\n"
                     , this, prefix, lk.mutex(), lk.mutex()->str().c_str());
        }
        ::printf("%p %s END\n", this, prefix);
    }
    LockV::iterator lower_bound(Mutex *mutex, LockV::iterator end) {
        Lock lkcmp;
        lkcmp.setMutex(mutex);
        return std::lower_bound(
            lockV_.begin(), end, lkcmp,
            [](const Lock& a, const Lock& b) {
                return a.getMutexId() < b.getMutexId();
            });
    }
};


}} // namespace cybozu::lock
