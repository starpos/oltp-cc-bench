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
#include "vector_payload.hpp"
#include "allocator.hpp"
#include "write_set.hpp"
#include "inline.hpp"


namespace cybozu {
namespace lock {


struct MutexWithMcs
{
    using Mode = XSMutex::Mode;

    int obj;
    McsSpinlock::Mutex mcsMu;

    INLINE MutexWithMcs() : obj(0), mcsMu() {
    }

    std::string str() const {
        return fmtstr("MutexWithMcs(%d)", obj);
    }

    INLINE int load() const { return ::load(obj); }
    INLINE int load_acquire() const { return ::load_acquire(obj); }
    INLINE bool cas_acq(int& obj0, int obj1) {
        return compare_exchange_acquire(obj, obj0, obj1);
    }
    INLINE int fetch_add_rel(int obj0) { return ::fetch_add_rel(obj, obj0); }
    INLINE int fetch_sub_rel(int obj0) { return ::fetch_sub_rel(obj, obj0); }
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
    Mutex *mutexp_;
    Mode mode_;
public:
    INLINE LockWithMcs() : mutexp_(nullptr), mode_(Mode::Invalid) {
    }
    INLINE LockWithMcs(Mutex& mutex, Mode mode) : LockWithMcs() {
        lock(mutex, mode);
        verify();
    }
    INLINE ~LockWithMcs() noexcept {
        unlock();
        verify();
    }
    LockWithMcs(const LockWithMcs&) = delete;
    LockWithMcs& operator=(const LockWithMcs&) = delete;
    INLINE LockWithMcs(LockWithMcs&& rhs) noexcept : LockWithMcs() { swap(rhs); verify(); }
    INLINE LockWithMcs& operator=(LockWithMcs&& rhs) noexcept { swap(rhs); verify(); return *this; }

    INLINE void reset(Mutex* mutexp = nullptr, Mode mode = Mode::Invalid) {
        mutexp_ = mutexp;
        mode_ = mode;
    }

    // debug
    void verify() const {
#if 0
        if (mode_ == Mode::X || mode_ == Mode::S) {
            assert(mutexp_);
        } else {
            assert(mode_ == Mode::Invalid);
        }
#endif
    }
    std::string str() const {
        return cybozu::util::formatString("LockWithMcs mutex:%p mode:%hhu", mutexp_, mode_);
    }

    INLINE void lock(Mutex& mutex, Mode mode) {
        verify();
        assert(!mutexp_);
        assert(mode_ == Mode::Invalid);

        if (mode == Mode::X) {
            write_lock(mutex);
        } else {
            read_lock(mutex);
        }
    }
    INLINE void write_lock(Mutex& mutex) {
        int v0 = mutex.load();
        for (;;) {
            if (unlikely(v0 != 0)) v0 = waitForWrite(mutex);
            if (likely(mutex.cas_acq(v0, -1))) {
                reset(&mutex, Mode::X);
                return;
            }
        }
    }
    INLINE void read_lock(Mutex& mutex) {
        int v0 = mutex.load();
        for (;;) {
            if (unlikely(v0 < 0)) v0 = waitForRead(mutex);
            if (likely(mutex.cas_acq(v0, v0 + 1))) {
                reset(&mutex, Mode::S);
                return;
            }
        }
    }

    INLINE bool tryLock(Mutex& mutex, Mode mode) {
        verify();
        assert(!mutexp_);
        assert(mode != Mode::Invalid);

        if (mode == Mode::X) {
            return write_trylock(mutex);
        } else {
            assert(mode == Mode::S);
            return read_trylock(mutex);
        }
    }
    INLINE bool write_trylock(Mutex& mutex) {
        assert(mode_ == Mode::Invalid); assert(!mutexp_);
        int v0 = mutex.load();
        while (likely(v0 == 0)) {
            if (likely(mutex.cas_acq(v0, -1))) {
                reset(&mutex, Mode::X);
                return true;
            }
        }
        return false;
    }
    INLINE bool read_trylock(Mutex& mutex) {
        assert(mode_ == Mode::Invalid); assert(!mutexp_);
        int v0 = mutex.load();
        while (likely(v0 >= 0)) {
            if (likely(mutex.cas_acq(v0, v0 + 1))) {
                reset(&mutex, Mode::S);
                return true;
            }
        }
        return false;
    }

    INLINE bool tryUpgrade() {
        verify();
        assert(mode_ == Mode::S); assert(mutexp_);
        int v0 = mutexp_->load();
        while (v0 == 1) {
            if (mutexp_->cas_acq(v0, -1)) {
                mode_ = Mode::X;
                return true;
            }
        }
        return false;
    }
    INLINE void unlock() noexcept {
        verify();
        if (mode_ == Mode::Invalid) {
            mutexp_ = nullptr;
            return;
        }
        assert(mutexp_);
        if (mode_ == Mode::X) {
            write_unlock();
        } else {
            read_unlock();
        }
    }
    INLINE void write_unlock() noexcept {
        assert(mode_ == Mode::X); assert(mutexp_);
        int ret = mutexp_->fetch_add_rel(1);
        assert(ret == -1); unused(ret);
        reset();
    }
    INLINE void read_unlock() noexcept {
        assert(mode_ == Mode::S); assert(mutexp_);
        int ret = mutexp_->fetch_sub_rel(1);
        unused(ret);
        assert(ret >= 1);
        reset();
    }

    INLINE bool isShared() const { return mode_ == Mode::S; }
    INLINE const Mutex* mutex() const { return mutexp_; }
    INLINE Mutex* mutex() { return mutexp_; }
    INLINE uintptr_t getMutexId() const { return uintptr_t(mutexp_); }
    INLINE Mode mode() const { return mode_; }

    // This is used for dummy object to comparison.
    INLINE void setMutex(Mutex *mutexp) {
        mutexp_ = mutexp;
        mode_ = Mode::Invalid;
    }

private:
    INLINE int waitForWrite(Mutex& mutex) {
        McsSpinlock lock(mutex.mcsMu);
        // Up to one thread can spin.
        int v0 = mutex.load();
        while (v0 != 0) {
            _mm_pause();
            v0 = mutex.load();
        }
        return v0;
    }
    INLINE int waitForRead(Mutex& mutex) {
        McsSpinlock lock(mutex.mcsMu);
        // Up to one thread can spin.
        int v0 = mutex.load();
        while (v0 < 0) {
            _mm_pause();
            v0 = mutex.load();
        }
        return v0;
    }
    INLINE void swap(LockWithMcs& rhs) noexcept {
        rhs.verify();
        std::swap(mutexp_, rhs.mutexp_);
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
 * Leis lock needs a special entry class.
 */
template <typename Lock>
struct OpEntryForLeis
{
    Lock lock;
    LocalValInfo info;
    bool isShared; /* used in retrospective mode.
                    * This is meaningful only when lock.mode() is Mode::Invalid. */
    bool isValid; /* localValInfo is valid or not.
                   * This is meaningful only when info is not empty. */

    INLINE OpEntryForLeis() : lock(), info(), isShared(false), isValid(false) {
    }
    INLINE explicit OpEntryForLeis(Lock&& lock0) : OpEntryForLeis() {
        lock = std::move(lock0);
    }
    OpEntryForLeis(const OpEntryForLeis&) = delete;
    OpEntryForLeis& operator=(const OpEntryForLeis&) = delete;
    INLINE OpEntryForLeis(OpEntryForLeis&& rhs) noexcept : OpEntryForLeis() { swap(rhs); }
    INLINE OpEntryForLeis& operator=(OpEntryForLeis&& rhs) noexcept { swap(rhs); return *this; }
    INLINE void swap(OpEntryForLeis& rhs) noexcept {
        std::swap(lock, rhs.lock);
        std::swap(info, rhs.info);
        std::swap(isShared, rhs.isShared);
        std::swap(isValid, rhs.isValid);
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
    using OpEntryL = OpEntryForLeis<Lock>;

private:
#if 1
    using Map = SingleThreadMap<Mutex*, OpEntryL>;
#else
    using Map = std::map<Mutex*, OpEntryL>;
#endif
    Map map_;

    MemoryVector local_;
    size_t valueSize_;

    std::vector<Mutex*> notYetV_; /* Mutexes that is not locked yet.
                                   * such as blind writes,
                                   * to be locked in recovery().
                                   * This is just cache data. */

public:
    INLINE LeisLockSet() = default;
    INLINE ~LeisLockSet() noexcept {
        unlock();
    }
    /**
     * You must call this at first.
     */
    INLINE void init(size_t valueSize, size_t nrReserve) {
        valueSize_ = valueSize;

        if (valueSize == 0) valueSize++;
        local_.setSizes(valueSize);
        local_.reserve(nrReserve);
    }

    INLINE bool read(Mutex& mutex, const void* sharedVal, void* dst) {
        typename Map::iterator it = map_.lower_bound(&mutex);
        if (unlikely(it == map_.end())) {
            // Lock order is preserved so the lock operation can block.
            auto pair = map_.emplace(&mutex, OpEntryL(Lock(mutex, Mode::S)));
            assert(pair.second);
            OpEntryL& ope = pair.first->second;
            ope.isShared = true;
            copyValue(dst, sharedVal); // read shared data.
            return true;
        }
        Mutex *mu = it->first;
        if (unlikely(mu == &mutex)) {
            // Already exists.
            OpEntryL& ope = it->second;
            Lock& lk = ope.lock;
            if (lk.mode() == Mode::S) {
                copyValue(dst, sharedVal); // read shared data.
                return true;
            }
            assert(lk.mode() == Mode::X || lk.mode() == Mode::Invalid);
            void* localVal = getValidLocalValPtr(ope, sharedVal);
            copyValue(dst, localVal); // read local data.
            return true;
        }
        // Lock order will not be preserved.
        // If trylock failed, we need to go to retrospective mode.
        auto pair = map_.emplace(&mutex, OpEntryL());
        assert(pair.second);
        OpEntryL& ope = pair.first->second;
        ope.isShared = true;
        if (likely(ope.lock.read_trylock(mutex))) {
            copyValue(dst, sharedVal); // read shared data.
            return true;
        } else {
            // Retrospective mode.
            notYetV_.push_back(&mutex);
            return false;
        }
    }
    INLINE bool write(Mutex& mutex, void* sharedVal, const void* src) {
        typename Map::iterator it = map_.lower_bound(&mutex);
        if (likely(it == map_.end() || it->first != &mutex)) {
            // Blind write.
            auto pair = map_.emplace(&mutex, OpEntryL());
            assert(pair.second);
            OpEntryL& ope = pair.first->second;
            ope.isShared = false;
            ope.info.set(allocateLocalVal(), sharedVal);
            writeLocalVal(ope, src);
            notYetV_.push_back(&mutex);
            return true;
        }
        // Already exists.
        OpEntryL& ope = it->second;
        Lock& lk = ope.lock;
        if (lk.mode() != Mode::S) {
            writeLocalVal(ope, src);
            return true;
        }
        // Try upgrade.
        ope.isShared = false;
        ope.info.set(allocateLocalVal(), sharedVal);
        if (likely(lk.tryUpgrade())) {
            writeLocalVal(ope, src);
            return true;
        } else {
            lk.unlock();
            notYetV_.push_back(&mutex);
            return false;
        }
    }
    INLINE bool readForUpdate(Mutex& mutex, void* sharedVal, void* dst) {
        typename Map::iterator it = map_.lower_bound(&mutex);
        if (unlikely(it == map_.end())) {
            // Lock order is preserved so the lock operation can block.
            auto pair = map_.emplace(&mutex, OpEntryL(Lock(mutex, Mode::X)));
            assert(pair.second);
            OpEntryL& ope = pair.first->second;
            ope.isShared = false;
            ope.info.set(allocateLocalVal(), sharedVal);
            copyValue(dst, getValidLocalValPtr(ope, sharedVal));
            return true;
        }
        Mutex *mu = it->first;
        if (unlikely(mu == &mutex)) {
            // Already exists.
            OpEntryL& ope = it->second;
            Lock& lk = ope.lock;
            if (lk.mode() == Mode::X) {
                copyValue(dst, getValidLocalValPtr(ope, sharedVal));
                return true;
            }
            if (lk.mode() == Mode::S) {
                ope.isShared = false;
                ope.info.set(allocateLocalVal(), sharedVal);
                if (lk.tryUpgrade()) {
                    copyValue(dst, getValidLocalValPtr(ope, sharedVal));
                    return true;
                } else {
                    lk.unlock();
                    notYetV_.push_back(&mutex);
                    return false;
                }
            }
            assert(lk.mode() == Mode::Invalid);
            assert(ope.info.sharedVal != nullptr);
            if (lk.write_trylock(mutex)) {
                copyValue(dst, getValidLocalValPtr(ope, sharedVal));
                return true;
            } else {
                // The mutex exists already in notYetV_.
                return false;
            }
        }
        // Lock order will not be preserved.
        // If trylock failed, we need to go to retrospective mode.
        auto pair = map_.emplace(&mutex, OpEntryL());
        assert(pair.second);
        OpEntryL& ope = pair.first->second;
        Lock& lk = ope.lock;
        ope.isShared = false;
        ope.info.set(allocateLocalVal(), sharedVal);
        if (likely(lk.write_trylock(mutex))) {
            copyValue(dst, getValidLocalValPtr(ope, sharedVal));
            return true;
        } else {
            notYetV_.push_back(&mutex);
            return false;
        }
    }
    INLINE bool blindWriteLockAll() {
#if 0
        std::sort(notYetV_.begin(), notYetV_.end());
#endif
#if 0
        // The version that does not use notYetV_.
        for (auto& pair : map_) {
            Mutex* mu = pair.first;
            OpEntryL& ope = pair.second;

            if (ope.lock.mode() == Mode::IsValid) {
                if (unlikely(!ope.lock.tryLock(mu, ope.isShared ? Mode::S : Mode::X))) {
                    return false;
                }
            }
        }
        return true;
#else
        // The version that uses notYetV_.
        for (size_t i = 0; i < notYetV_.size(); i++) {
            Mutex* mu = notYetV_[i];
            assert(mu != nullptr);
            typename Map::iterator it = map_.lower_bound(mu);
            assert(it != map_.end());
            assert(it->first == mu);
            OpEntryL& ope = it->second;
            if (unlikely(!ope.lock.tryLock(*mu, ope.isShared ? Mode::S : Mode::X))) {
                return false;
            }
            notYetV_[i] = nullptr;
        }
        notYetV_.clear();
        return true;
#endif
    }
    INLINE void recover() {
        // Find the smallest mutex to lock.
        assert(!notYetV_.empty());
        Mutex* minMu = (Mutex*)uintptr_t(-1);
        for (Mutex* mu : notYetV_) {
            if (mu == nullptr) continue;
            minMu = std::min(mu, minMu);
        }
        assert(uintptr_t(minMu) != uintptr_t(-1));
        notYetV_.clear();

        // Find the corresponding iterator.
        typename Map::iterator target = map_.lower_bound(minMu);
        assert(target != map_.end());
        assert(target->first == minMu);
        typename Map::iterator it = map_.begin();

        // Invalidate local values.
        while (it != target) {
            OpEntryL& ope = it->second;
            if (!ope.isShared) ope.isValid = false;
            ++it;
        }
        // Unlock and invalidate local values.
        assert(it == target);
        while (it != map_.end()) {
            OpEntryL& ope = it->second;
            ope.lock.unlock();
            if (!ope.isShared) ope.isValid = false;
            ++it;
        }
        // Re-lock in order.
        it = target;
        while (it != map_.end()) {
            Mutex& mu = *it->first;
            OpEntryL& ope = it->second;
            ope.lock.lock(mu, ope.isShared ? Mode::S : Mode::X); // block.
            ++it;
        }
    }

    /**
     * Writeback local writeset and unlock.
     */
    INLINE void updateAndUnlock() {
        // here is serialized point.
        assert(notYetV_.empty());

        typename Map::iterator it = map_.begin();
        while (it != map_.end()) {
            OpEntryL& ope = it->second;
            assert(ope.lock.mode() != Mode::Invalid);
            if (!ope.lock.isShared()) {
                // Update the shared value.
                copyValue(ope.info.sharedVal, getLocalValPtr(ope.info));
            }
            ope.lock.unlock();
            ++it;
        }
        map_.clear();
        local_.clear();
    }
    /**
     * If you want to abort, use this instead of updateAndUnlock().
     */
    INLINE void unlock() noexcept {
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
        notYetV_.clear();
    }
    INLINE bool empty() const { return map_.empty(); }
    INLINE size_t size() const { return map_.size(); }

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
    INLINE size_t allocateLocalVal() {
        const size_t idx = local_.size();
#ifndef NO_PAYLOAD
        local_.resize(idx + 1);
#endif
        return idx;
    }
    /**
     * DO NOT call this if the lock is not held.
     */
    INLINE void* getValidLocalValPtr(OpEntryL& ope, const void* sharedVal) {
        void* localVal = getLocalValPtr(ope.info);
        if (!ope.isValid) {
            assert(ope.info.sharedVal == sharedVal);
            copyValue(localVal, sharedVal);
            ope.isValid = true;
        }
        return localVal;
    }
    INLINE void* getLocalValPtr(const LocalValInfo& info) {
#ifdef NO_PAYLOAD
        unused(info);
        return nullptr;
#else
        if (info.localValIdx == UINT64_MAX) {
            return nullptr;
        } else {
            return &local_[info.localValIdx];
        }
#endif
    }
    INLINE void copyValue(void* dst, const void* src) {
#ifndef NO_PAYLOAD
        ::memcpy(dst, src, valueSize_);
#else
        unused(dst); unused(src);
#endif
    }
    INLINE void writeLocalVal(OpEntryL& ope, const void* src) {
        copyValue(getLocalValPtr(ope.info), src);
        ope.isValid = true;
    }

    // for debug
    void printMap() const {
        for (const auto& pair : map_) {
            const Mutex *mu = pair.first;
            const OpEntryL& ope = pair.second;
            ::printf("mutex:%p mode:%c localValIdx:%zu sharedVal:%p isValid:%d isShared:%d\n"
                     , mu
                     , (ope.lock.mode() == Mode::X) ? 'X' : (ope.lock.mode() == Mode::S ? 'S' : 'I')
                     , ope.info.localValIdx, ope.info.sharedVal
                     , ope.isValid, ope.isShared);
        }
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
    using OpEntryL = OpEntryForLeis<Lock>;

private:
    using Vec = std::vector<OpEntryL>;
    Vec vec_;
    uintptr_t maxMutex_;
    size_t nrSorted_;

    MemoryVector local_;
    size_t valueSize_;

public:
    INLINE LeisLockSet() : vec_(), maxMutex_(0), nrSorted_(0) {
    }
    INLINE ~LeisLockSet() noexcept {
        unlock();
    }

    /**
     * You must call this at first.
     */
    INLINE void init(size_t valueSize, size_t nrReserve) {
        valueSize_ = valueSize;

        if (valueSize == 0) valueSize++;
        local_.setSizes(valueSize);

        vec_.reserve(nrReserve);
        local_.reserve(nrReserve);
    }

    INLINE bool read(Mutex& mutex, const void* sharedVal, void* dst) {
        if (unlikely(maxMutex_ < uintptr_t(&mutex))) {
            // Lock order is preserved so the lock operation can block.
            OpEntryL& ope = vec_.emplace_back(OpEntryL(Lock(mutex, Mode::S)));
            maxMutex_ = uintptr_t(&mutex);
            if (nrSorted_ + 1 == vec_.size()) nrSorted_++;
            ope.isShared = true;
            copyValue(dst, sharedVal);
            return true;
        }
        typename Vec::iterator it = find(&mutex);
        if (unlikely(it != vec_.end())) {
            // Already exists.
            OpEntryL& ope = *it;
            Lock& lk = ope.lock;
            if (lk.mode() == Mode::S) {
                copyValue(dst, sharedVal);
                return true;
            }
            assert(lk.mode() == Mode::X || lk.mode() == Mode::Invalid);
#ifndef NDEBUG
            if (lk.mode() == Mode::Invalid) assert(ope.isValid);
#endif
            copyValue(dst, getValidLocalValPtr(ope, sharedVal));
            return true;
        }
        // Lock order is not preserved.
        // If tryLock failed, we must goto retrospective mode.
        OpEntryL& ope = vec_.emplace_back();
        ope.isShared = true;
        if (likely(ope.lock.read_trylock(mutex))) {
            copyValue(dst, sharedVal);
            return true;
        } else {
            // Retrospective mode.
            ope.lock.setMutex(&mutex);
            return false;
        }
    }
    INLINE bool write(Mutex& mutex, void* sharedVal, const void* src) {
        typename Vec::iterator it = find(&mutex);
        if (likely(it == vec_.end())) {
            // Blind write.
            OpEntryL& ope = vec_.emplace_back();
            ope.isShared = false;
            ope.lock.setMutex(&mutex);
            ope.info.set(allocateLocalVal(), sharedVal);
            writeLocalVal(ope, src);
            maxMutex_ = std::max(maxMutex_, uintptr_t(&mutex));
            return true;
        }
        // Already exists.
        OpEntryL& ope = *it;
        Lock& lk = ope.lock;
        assert(lk.getMutexId() == uintptr_t(&mutex));
        if (lk.mode() != Mode::S) {
            writeLocalVal(ope, src);
            return true;
        }
        // Try upgrade.
        ope.isShared = false;
        ope.info.set(allocateLocalVal(), sharedVal);
        if (likely(lk.tryUpgrade())) {
            writeLocalVal(ope, src);
            return true;
        } else {
            lk.unlock(); // mode S --> Invalid.
            lk.setMutex(&mutex);
            return false;
        }
    }
    INLINE bool readForUpdate(Mutex& mutex, void* sharedVal, void* dst) {
        if (unlikely(maxMutex_ < uintptr_t(&mutex))) {
            // Lock order is preserved so the lock operation can block.
            OpEntryL& ope = vec_.emplace_back(OpEntryL(Lock(mutex, Mode::X)));
            maxMutex_ = uintptr_t(&mutex);
            if (nrSorted_ + 1 == vec_.size()) nrSorted_++;
            ope.isShared = false;
            ope.info.set(allocateLocalVal(), sharedVal);
            copyValue(dst, getValidLocalValPtr(ope, sharedVal));
            return true;
        }
        typename Vec::iterator it = find(&mutex);
        if (unlikely(it != vec_.end())) {
            OpEntryL& ope = *it;
            Lock& lk = ope.lock;
            assert(lk.getMutexId() == uintptr_t(&mutex));
            if (lk.mode() == Mode::X) {
                copyValue(dst, getValidLocalValPtr(ope, sharedVal));
                return true;
            }
            if (lk.mode() == Mode::S) {
                ope.isShared = false;
                ope.info.set(allocateLocalVal(), sharedVal);
                if (lk.tryUpgrade()) {
                    copyValue(dst, getValidLocalValPtr(ope, sharedVal));
                    return true;
                } else {
                    lk.unlock();
                    lk.setMutex(&mutex);
                    return false;
                }
            }
            assert(lk.mode() == Mode::Invalid);
            assert(!ope.isShared);
            assert(ope.info.sharedVal != nullptr);
            if (lk.write_trylock(mutex)) {
                copyValue(dst, getValidLocalValPtr(ope, sharedVal));
                return true;
            } else {
                lk.setMutex(&mutex);
                return false;
            }
        }
        // Lock order will not be preserved.
        // If trylock failed, we need to go to retrospective mode.
        OpEntryL& ope = vec_.emplace_back();
        Lock& lk = ope.lock;
        ope.isShared = false;
        ope.info.set(allocateLocalVal(), sharedVal);
        if (likely(lk.write_trylock(mutex))) {
            copyValue(dst, getValidLocalValPtr(ope, sharedVal));
            return true;
        } else {
            lk.setMutex(&mutex);
            return false;
        }
    }
    INLINE bool blindWriteLockAll() {
        for (OpEntryL& ope : vec_) {
            Lock& lk = ope.lock;
            if (lk.mode() != Mode::Invalid) continue;
            assert(!ope.isShared);
            Mutex& mu = *lk.mutex();
            if (unlikely(!lk.tryLock(mu, ope.isShared ? Mode::S : Mode::X))) {
                lk.setMutex(&mu);
                return false;
            }
        }
        return true;
    }
    INLINE void recover() {
#if 0
        // for debug.
        for (size_t i = 0; i < vec_.size(); i++) {
            for (size_t j = 0; j < vec_.size(); j++) {
                if (i == j) continue;
                if (vec_[i].lock.getMutexId() == vec_[j].lock.getMutexId()) {
                    printVec("");
                    assert(false);
                }
            }
        }
#endif
        /*
         * Sort the vector and get the starting position to relock,
         * all mutexes after which is no more than mutex_.
         */
        std::sort(
            vec_.begin(), vec_.end(),
            [](const OpEntryL& a, const OpEntryL& b) {
                return a.lock.getMutexId() < b.lock.getMutexId();
            });

        // Search the first invalid item and
        // invalidate local values until it.
        typename Vec::iterator it = vec_.begin();
        while (it != vec_.end() && it->lock.mode() != Mode::Invalid) {
            if (!it->isShared) it->isValid = false;
            ++it;
        }
        assert(it != vec_.end());
        typename Vec::iterator target = it;
        // Unlock and invalidate local values.
        while (it != vec_.end()) {
            if (!it->isShared) it->isValid = false;
            Lock& lk = it->lock;
            Mutex* mu = (Mutex*)lk.getMutexId();
            lk.unlock();
            lk.setMutex(mu);
            ++it;
        }
        // Re-lock in order.
        it = target;
        while (it != vec_.end()) {
            Lock& lk = it->lock;
            Mutex& mu = *reinterpret_cast<Mutex*>(lk.getMutexId());
            lk.lock(mu, it->isShared ? Mode::S : Mode::X);
            ++it;
        }

        maxMutex_ = vec_.back().lock.getMutexId();
        nrSorted_ = vec_.size();
    }
    /**
     * Writeback local writeset and unlock.
     */
    INLINE void updateAndUnlock() {
        // Here is serialization point.

        for (OpEntryL& ope : vec_) {
            Lock& lk = ope.lock;
            assert(lk.mode() != Mode::Invalid);
            if (!lk.isShared()) {
                // Update the shared value.
                assert(ope.info.sharedVal != nullptr);
                copyValue(ope.info.sharedVal, getLocalValPtr(ope.info));
            }
            lk.unlock(); // This is S2PL protocol.
        }
        vec_.clear();
        maxMutex_ = 0; // smaller than any valid pointers.
        nrSorted_ = 0;
        local_.clear();
    }
    INLINE void unlock() noexcept {
        vec_.clear();  // automatically release the locks.
        maxMutex_ = 0; // smaller than any valid pointers.
        nrSorted_ = 0;
        local_.clear();
    }
    INLINE bool empty() const { return vec_.empty(); }
    INLINE size_t size() const { return vec_.size(); }
private:
    INLINE typename Vec::iterator find(Mutex *mutex) {
        const uintptr_t key = uintptr_t(mutex);
#if 1
        // Binary search in sorted area.
        typename Vec::iterator sortedEnd, it;
        sortedEnd = vec_.begin();
        std::advance(sortedEnd, nrSorted_);
        it = lower_bound(mutex, sortedEnd);
        if (it != sortedEnd && it->lock.getMutexId() == key) {
            // found.
            return it;
        }
        // Sequential search in unsorted area.
        return std::find_if(
            sortedEnd, vec_.end(),
            [&](const OpEntryL& ope) {
                return ope.lock.getMutexId() == key;
            });
#else
        return std::find_if(
            vec_.begin(), vec_.end(),
            [&](const OpEntryL& ope) {
                return ope.lock.getMutexId() == key;
            });
#endif
    }
    INLINE size_t allocateLocalVal() {
        const size_t idx = local_.size();
#ifndef NO_PAYLOAD
        local_.resize(idx + 1);
#endif
        return idx;
    }
    /**
     * DO NOT call this if the lock is not held.
     */
    INLINE void* getValidLocalValPtr(OpEntryL& ope, const void* sharedVal) {
        void* localVal = getLocalValPtr(ope.info);
        if (!ope.isValid) {
            assert(ope.info.sharedVal == sharedVal);
            copyValue(localVal, sharedVal);
            ope.isValid = true;
        }
        return localVal;
    }
    INLINE void copyValue(void* dst, const void* src) {
#ifndef NO_PAYLOAD
        ::memcpy(dst, src, valueSize_);
#else
        unused(dst); unused(src);
#endif
    }
    INLINE void writeLocalVal(OpEntryL& ope, const void* src) {
        copyValue(getLocalValPtr(ope.info), src);
        ope.isValid = true;
    }
    INLINE void* getLocalValPtr(const LocalValInfo& info) {
#ifdef NO_PAYLOAD
        unused(info);
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
            ::printf("%p %s mutex:%p %s isShared:%d isValid:%d shared:%p localIdx:%zu\n"
                     , this, prefix, ope.lock.mutex(), ope.lock.mutex()->str().c_str()
                     , ope.isShared ? 1 : 0, ope.isValid ? 1 : 0
                     , ope.info.sharedVal, ope.info.localValIdx);
        }
        ::printf("%p %s END\n", this, prefix);
    }
    INLINE typename Vec::iterator lower_bound(Mutex *mutex, typename Vec::iterator end) {
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
