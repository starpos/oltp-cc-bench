#pragma once
/**
 * @file
 * @brief an optimistic concurrency control method.
 * @author Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 *
 * (C) 2016 Cybozu Labs, Inc.
 */
#include <stdexcept>
#include <unordered_map>
#include "lock.hpp"
#include "arch.hpp"
#include "vector_payload.hpp"
#include "allocator.hpp"
#include "inline.hpp"


#if 0
#define USE_OCC_MCS
#else
#undef USE_OCC_MCS
#endif


namespace cybozu {
namespace occ {


struct OccMutexData
{
    union {
        uint32_t obj;
        struct {
            // layout for little endian architecture.
            uint32_t version:31;
            uint32_t locked:1;
        };
    };

    INLINE OccMutexData() = default;
    INLINE OccMutexData(uint32_t obj0): obj(obj0) {}
    INLINE operator uint32_t() const { return obj; }
};


struct OccMutex
{
    alignas(sizeof(uintptr_t))
    OccMutexData md;
#ifdef USE_OCC_MCS
    cybozu::lock::McsSpinlock::Mutex mcsMutex;
#endif

#ifdef USE_OCC_MCS
    INLINE OccMutex() : md(0), mcsMutex() {}
#else
    INLINE OccMutex() : md(0) {}
#endif

    INLINE OccMutexData load() const { return ::load(md); }
    INLINE OccMutexData load_acquire() const { return ::load_acquire(md); }
    INLINE void store(OccMutexData md0) { ::store(md, md0); }
    INLINE void store_release(OccMutexData md0) { ::store_release(md, md0); }

    // This is used in the write-lock phase.
    INLINE bool cas_acq(OccMutexData& md0, OccMutexData md1) {
        return ::compare_exchange_acquire(md, md0, md1);
    }
};


class OccLock
{
public:
    using MutexData = OccMutexData;
    using Mutex = OccMutex;
private:
    Mutex *mutex_;
    MutexData md_;
public:
    INLINE OccLock() : mutex_(), md_() {}
    INLINE explicit OccLock(Mutex *mutex) : OccLock() {
        lock(mutex);
    }
    INLINE ~OccLock() noexcept {
        unlock();
    }
    OccLock(const OccLock&) = delete;
    OccLock& operator=(const OccLock&) = delete;
    INLINE OccLock(OccLock&& rhs) noexcept : OccLock() { swap(rhs); }
    INLINE OccLock& operator=(OccLock&& rhs) noexcept { swap(rhs); return *this; }
    INLINE bool operator<(const OccLock& rhs) const {
        return uintptr_t(mutex_) < uintptr_t(rhs.mutex_);
    }
    INLINE void lock(Mutex *mutex) {
        assert(mutex != nullptr);
        mutex_ = mutex;
        MutexData md0 = mutex_->load();
        for (;;) {
            if (unlikely(md0.locked)) md0 = waitFor();
            MutexData md1 = md0;
            md1.locked = 1;
            if (likely(mutex_->cas_acq(md0, md1))) {
                md_ = md1;
                return;
            }
        }
    }
    INLINE bool tryLock(Mutex *mutex) {
        assert(mutex != nullptr);
        MutexData md0 = mutex->load();
        for (;;) {
            if (unlikely(md0.locked)) return false;
            MutexData md1 = md0;
            md1.locked = 1;
            if (likely(mutex->cas_acq(md0, md1))) {
                mutex_ = mutex;
                md_ = md1;
                return true;
            }
        }
    }
    INLINE void unlock(bool updated = false) {
        if (unlikely(!mutex_)) return;
        MutexData md0 = md_;
        assert(md0.locked);
        if (likely(updated)) md0.version++;
        md0.locked = 0;
        mutex_->store_release(md0);
        mutex_ = nullptr;
    }
    INLINE uintptr_t getMutexId() const { return uintptr_t(mutex_); }
private:
    INLINE void swap(OccLock& rhs) noexcept {
        std::swap(mutex_, rhs.mutex_);
        std::swap(md_, rhs.md_);
    }
    INLINE MutexData waitFor() {
        assert(mutex_ != nullptr);
#ifdef USE_OCC_MCS
        // In order so many threads not to spin on md value.
        cybozu::lock::McsSpinlock lk(mutex_->mcsMutex);
#endif
        MutexData md0 = mutex_->load();
        while (md0.locked) {
            _mm_pause();
            md0 = mutex_->load();
        }
        return md0;
    }
};


class OccReader
{
public:
    using MutexData = OccLock::MutexData;
    using Mutex = OccLock::Mutex;
private:
    const Mutex *mutex_;
    MutexData md_;
public:
    const void *sharedVal;
    size_t localValIdx;  // local data index.

    /**
     * uninitialized.
     * Call set() first.
     */
    INLINE OccReader() = default;

    OccReader(const OccReader&) = delete;
    OccReader& operator=(const OccReader&) = delete;
    INLINE OccReader(OccReader&& rhs) noexcept : OccReader() { swap(rhs); }
    INLINE OccReader& operator=(OccReader&& rhs) noexcept { swap(rhs); return *this; }

    INLINE void set(const Mutex *mutex, const void *sharedVal0, size_t localValIdx0) {
        mutex_ = mutex;
        // md_ is still uninitialized.
        sharedVal = sharedVal0;
        localValIdx = localValIdx0;
    }

    /**
     * Call this before read the resource.
     */
    INLINE void prepare() {
        assert(mutex_);
        MutexData md0 = mutex_->load_acquire();
        while (md0.locked) {
            _mm_pause();
            md0 = mutex_->load_acquire();
        }
        md_ = md0;
    }
    INLINE bool tryPrepare() {
        assert(mutex_);
        md_ = mutex_->load_acquire();
        return !md_.locked;
    }
    /**
     * Call this just after read the resource.
     */
    INLINE void readFence() const { acquire_fence(); }
    /**
     * Call this to verify read data is valid or not.
     */
    INLINE bool verifyAll() const {
        assert(mutex_);
        const MutexData md0 = mutex_->load();
        return !md0.locked && md_.version == md0.version;
    }
    INLINE bool verifyVersion() const {
        assert(mutex_);
        const MutexData md0 = mutex_->load();
        return md_.version == md0.version;
    }
    INLINE uintptr_t getMutexId() const { return uintptr_t(mutex_); }

    INLINE void swap(OccReader& rhs) noexcept {
        std::swap(mutex_, rhs.mutex_);
        std::swap(md_, rhs.md_);
        std::swap(sharedVal, rhs.sharedVal);
        std::swap(localValIdx, rhs.localValIdx);
    }
};


struct WriteEntry
{
    using Mutex = OccLock::Mutex;
    Mutex *mutex;
    void *sharedVal;
    size_t localValIdx;  // index in the local data area.

    /**
     * Call set() to fill values.
     */
    INLINE WriteEntry() = default;

    WriteEntry(const WriteEntry&) = delete;
    WriteEntry& operator=(const WriteEntry&) = delete;
    INLINE WriteEntry(WriteEntry&& rhs) noexcept : WriteEntry() { swap(rhs); }
    INLINE WriteEntry& operator=(WriteEntry&& rhs) noexcept { swap(rhs); return *this; }

    INLINE bool operator<(const WriteEntry& rhs) const {
        return getMutexId() < rhs.getMutexId();
    }
    INLINE void set(Mutex *mutex0, void *sharedVal0, size_t localValIdx0) {
        mutex = mutex0;
        sharedVal = sharedVal0;
        localValIdx = localValIdx0;
    }
    INLINE uintptr_t getMutexId() const { return uintptr_t(mutex); }
private:
    INLINE void swap(WriteEntry& rhs) noexcept {
        std::swap(mutex, rhs.mutex);
        std::swap(sharedVal, rhs.sharedVal);
        std::swap(localValIdx, rhs.localValIdx);
    }
};


class LockSet
{
public:
    using Mutex = OccMutex;

private:
    using LockV = std::vector<OccLock>;
    using ReadV = std::vector<OccReader>;
    using WriteV = std::vector<WriteEntry>;

    // key is mutex pointer, value is index in vector.
#if 1
    using IndexM = SingleThreadUnorderedMap<uintptr_t, size_t>;
#else
    using IndexM = std::unordered_map<uintptr_t, size_t>;
#endif

    WriteV writeV_; // write set.
    IndexM writeM_; // write set index.
    ReadV readV_; // read set.
    IndexM readM_; // read set index.
    LockV lockV_;

    MemoryVector local_; // stores local values of read/write set.
    size_t valueSize_;

public:
    INLINE void init(size_t valueSize, size_t nrReserve) {
        valueSize_ = valueSize;  // 0 can be allowed.

        // MemoryVector does not allow zero-size element.
        if (valueSize == 0) valueSize++;
        local_.setSizes(valueSize);

        // For long transactions.
        writeV_.reserve(nrReserve);
        readV_.reserve(nrReserve);
        lockV_.reserve(nrReserve);
        local_.reserve(nrReserve);
    }
    INLINE void read(Mutex& mutex, void *sharedVal, void *localVal) {
        unused(sharedVal); unused(localVal);
        size_t localValIdx;
        ReadV::iterator itR = findInReadSet(uintptr_t(&mutex));
        if (unlikely(itR != readV_.end())) {
            localValIdx = itR->localValIdx;
        } else {
            // For blind-write, you must check write set also.
            WriteV::iterator itW = findInWriteSet(uintptr_t(&mutex));
            if (unlikely(itW != writeV_.end())) {
                // This is blind write, so we just read from local write set.
                localValIdx = itW->localValIdx;
            } else {
                // allocate new local value area.
                localValIdx = local_.size();
#ifndef NO_PAYLOAD
                local_.resize(localValIdx + 1);
#endif
                OccReader& r = readV_.emplace_back();
                r.set(&mutex, sharedVal, localValIdx);
                readToLocal(r);
            }
        }
        // read local data.
#ifndef NO_PAYLOAD
        ::memcpy(localVal, &local_[localValIdx], valueSize_);
#endif
    }
    INLINE void readToLocal(OccReader& r) {
        for (;;) {
            r.prepare();
            // read shared data.
#ifndef NO_PAYLOAD
            ::memcpy(&local_[r.localValIdx], r.sharedVal, valueSize_);
#endif
            r.readFence();
            if (r.verifyAll()) break;
        }
    }
    INLINE bool tryReadToLocal(OccReader& r, bool inWriteSet) {
        if (unlikely(!r.tryPrepare())) return false;
#ifndef NO_PAYLOAD
        ::memcpy(&local_[r.localValIdx], r.sharedVal, valueSize_);
#endif
        r.readFence();
        return inWriteSet ? r.verifyVersion() : r.verifyAll();
    }
    INLINE void write(Mutex& mutex, void *sharedVal, void *localVal) {
        unused(sharedVal); unused(localVal);
        size_t localValIdx;
        WriteV::iterator itW = findInWriteSet(uintptr_t(&mutex));
        if (unlikely(itW != writeV_.end())) {
            localValIdx = itW->localValIdx;
        } else {
            ReadV::iterator itR = findInReadSet(uintptr_t(&mutex));
            if (likely(itR == readV_.end())) {
                // allocate new local value area.
                localValIdx = local_.size();
#ifndef NO_PAYLOAD
                local_.resize(localValIdx + 1);
#endif
            } else {
                localValIdx = itR->localValIdx;
            }
            WriteEntry& w = writeV_.emplace_back();
            w.set(&mutex, sharedVal, localValIdx);
        }
        // write local data.
#ifndef NO_PAYLOAD
        ::memcpy(&local_[localValIdx], localVal, valueSize_);
#endif
    }
    INLINE void lock() {
        std::sort(writeV_.begin(), writeV_.end());
        for (WriteEntry& w : writeV_) {
            lockV_.emplace_back(w.mutex);
        }
        // Serialization point.
        SERIALIZATION_POINT_BARRIER();
    }
    INLINE bool tryLock() {
        std::sort(writeV_.begin(), writeV_.end());
        for (WriteEntry& w : writeV_) {
            OccLock& lk = lockV_.emplace_back();
            if (unlikely(!lk.tryLock(w.mutex))) return false;
        }
        // Serialization point.
        SERIALIZATION_POINT_BARRIER();
        return true;
    }
    INLINE bool verify() {
        const bool useIndex = shouldUseIndex(writeV_); // QQQQQ
        if (likely(!useIndex)) {
            std::sort(writeV_.begin(), writeV_.end());
        }
        for (OccReader& r : readV_) {
            bool inWriteSet;
            if (unlikely(useIndex)) {
                inWriteSet = findInWriteSet(r.getMutexId()) != writeV_.end();
            } else {
                WriteEntry w;
                w.set((Mutex *)r.getMutexId(), nullptr, 0);
                inWriteSet = std::binary_search(writeV_.begin(), writeV_.end(), w);
            }
            const bool valid = inWriteSet ? r.verifyVersion() : r.verifyAll();
            if (unlikely(!valid)) return false;
        }
        return true;
    }
    /**
     * This is very limited healing.
     */
    INLINE bool verifyWithHealing() {
        const bool useIndex = shouldUseIndex(writeV_);
        if (likely(!useIndex)) {
            std::sort(writeV_.begin(), writeV_.end());
        }
        bool isHealed = true;
        while (isHealed) {
            isHealed = false;
            for (OccReader& r : readV_) {
                bool inWriteSet;
                if (unlikely(useIndex)) {
                    inWriteSet = findInWriteSet(r.getMutexId()) != writeV_.end();
                } else {
                    WriteEntry w;
                    w.set((Mutex *)r.getMutexId(), nullptr, 0);
                    inWriteSet = std::binary_search(writeV_.begin(), writeV_.end(), w);
                }
                const bool valid = inWriteSet ? r.verifyVersion() : r.verifyAll();
                if (!valid) {
                    // do healing
                    if (unlikely(!tryReadToLocal(r, inWriteSet))) {
                        // try read failed. (We can not wait for lock to avoid deadlock.)
                        return false;
                    }
                    isHealed = true;
                }
            }
        }
        return true;
    }
    INLINE void updateAndUnlock() {
        assert(lockV_.size() == writeV_.size());
        auto itLk = lockV_.begin();
        auto itW = writeV_.begin();
        while (itLk != lockV_.end()) {
            assert(itW != writeV_.end());
#ifndef NO_PAYLOAD
            // writeback
            ::memcpy(itW->sharedVal, &local_[itW->localValIdx], valueSize_);
#endif
            itLk->unlock(true);
            ++itLk;
            ++itW;
        }
        clear();
    }
    INLINE void clear() {
        lockV_.clear();
        readV_.clear();
        readM_.clear();
        writeV_.clear();
        writeM_.clear();
        local_.clear();
    }
    INLINE bool empty() const {
        return lockV_.empty() &&
            readV_.empty() &&
            readM_.empty() &&
            writeV_.empty() &&
            writeM_.empty() &&
            local_.empty();
    }
private:
    INLINE ReadV::iterator findInReadSet(uintptr_t key) {
        return findInSet(
            key, readV_, readM_,
            [](const OccReader& r) { return r.getMutexId(); });
    }
    INLINE WriteV::iterator findInWriteSet(uintptr_t key) {
        return findInSet(
            key, writeV_, writeM_,
            [](const WriteEntry& w) { return w.getMutexId(); });
    }
    /**
     * func: uintptr_t(const Vector::value_type&)
     */
    template <typename Vector, typename Map, typename Func>
    INLINE typename Vector::iterator findInSet(uintptr_t key, Vector& vec, Map& map, Func&& func) {
        if (unlikely(shouldUseIndex(vec))) {
            // create indexes.
            for (size_t i = map.size(); i < vec.size(); i++) {
                map[func(vec[i])] = i;
            }
            // use indexes.
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
    bool shouldUseIndex(const Vector& vec) const {
        constexpr size_t threshold = 4096 / sizeof(typename Vector::value_type);
        return vec.size() > threshold;
    }
};


}} // namespace cybozu::occ
