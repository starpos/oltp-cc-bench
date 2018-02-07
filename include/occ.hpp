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
#include "cache_line_size.hpp"
#include "allocator.hpp"


#define USE_OCC_MCS
//#undef USE_OCC_MCS


namespace cybozu {
namespace occ {

struct OccLockData
{
    /*
     * 0-30(31bits) record version
     * 31(1bit) X lock flag.
     */
    uint32_t obj;
    static constexpr uint32_t mask = (0x1 << 31);

    OccLockData() : obj(0) {
    }
    OccLockData(uint32_t obj): obj(obj) {
    }
    operator uint32_t() const { return obj; }

    uint32_t getVersion() const {
        return obj & ~mask;
    }
    void setVersion(uint32_t version) {
        assert(version < mask);
        obj &= mask;
        obj |= version;
    }
    void incVersion() {
        uint32_t v = getVersion();
        if (v < mask - 1) {
            v++;
        } else {
            v = 0;
        }
        setVersion(v);
    }
    bool isLocked() const {
        return (obj & mask) != 0;
    }
    void setLock() {
        obj |= mask;
    }
    void clearLock() {
        obj &= ~mask;
    }
};


struct OccMutex
{
#if 0
#ifdef MUTEX_ON_CACHELINE
    alignas(CACHE_LINE_SIZE)
#endif
#else
    alignas(sizeof(uintptr_t))
#endif
    uint32_t obj;
#ifdef USE_OCC_MCS
    cybozu::lock::McsSpinlock::Mutex mcsMutex;
#endif

    OccMutex() : obj(0) {
    }

    OccLockData load() const {
        return __atomic_load_n(&obj, __ATOMIC_RELAXED);
    }
    OccLockData loadAcquire() const {
        return __atomic_load_n(&obj, __ATOMIC_ACQUIRE);
    }
#if 0
    void set(const OccLockData& after) {
        obj = after.obj;
    }
#endif
    void store(const OccLockData& after) {
        __atomic_store_n(&obj, after.obj, __ATOMIC_RELAXED);
    }
    void storeRelease(const OccLockData& after) {
        __atomic_store_n(&obj, after.obj, __ATOMIC_RELEASE);
    }
    // This is used in the write-lock phase.
    // Full fence is set at the last point of the phase.
    // So we need not fence with CAS.
    bool compareAndSwap(OccLockData& before, const OccLockData& after) {
        return __atomic_compare_exchange_n(
            &obj, &before.obj, after.obj,
            false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
    }
};

class OccLock
{
public:
    using LockData = OccLockData;
    using Mutex = OccMutex;
private:
    Mutex *mutex_;
    LockData lockD_;
    bool updated_;
public:
    OccLock() : mutex_(), lockD_(), updated_(false) {}
    explicit OccLock(Mutex *mutex) : OccLock() {
        lock(mutex);
    }
    ~OccLock() noexcept {
        unlock();
    }
    OccLock(const OccLock&) = delete;
    OccLock(OccLock&& rhs) : OccLock() { swap(rhs); }
    OccLock& operator=(const OccLock&) = delete;
    OccLock& operator=(OccLock&& rhs) { swap(rhs); return *this; }
    bool operator<(const OccLock& rhs) const {
        return uintptr_t(mutex_) < uintptr_t(rhs.mutex_);
    }
    void lock(Mutex *mutex) {
        if (mutex_) throw std::runtime_error("OccLock::lock: already locked");
        mutex_ = mutex;

        lockD_ = mutex_->load();
        for (;;) {
            if (lockD_.isLocked()) waitFor();
            LockData lockD = lockD_;
            lockD.setLock();
            if (mutex_->compareAndSwap(lockD_, lockD)) {
                lockD_ = lockD;
                updated_ = false;
                break;
            }
        }
    }
    void unlock() {
        if (!mutex_) return;

        LockData lockD = lockD_;
        assert(lockD.isLocked());
        if (updated_) lockD.incVersion();
        lockD.clearLock();
        mutex_->storeRelease(lockD);
        mutex_ = nullptr;
    }
    void update() {
        updated_ = true;
    }
    uintptr_t getMutexId() const {
        return uintptr_t(mutex_);
    }
private:
    void swap(OccLock& rhs) {
        std::swap(mutex_, rhs.mutex_);
        std::swap(lockD_, rhs.lockD_);
    }
    void waitFor() {
        assert(mutex_);
#ifdef USE_OCC_MCS
        // In order so many threads not to spin on lockD value.
        cybozu::lock::McsSpinlock lk(&mutex_->mcsMutex);
#endif
        LockData lockD;
        for (;;) {
            lockD = mutex_->load();
            if (!lockD.isLocked()) {
                lockD_ = lockD;
                return;
            }
            _mm_pause();
        }
    }
};

class OccReader
{
public:
    using LockData = OccLock::LockData;
    using Mutex = OccLock::Mutex;
private:
    const Mutex *mutex_;
    LockData lockD_;
public:
    size_t localValIdx;  // local data index.

    OccReader() : mutex_(), lockD_() {}
    OccReader(const OccReader&) = delete;
    OccReader(OccReader&& rhs) : OccReader() { swap(rhs); }
    OccReader& operator=(const OccReader&) = delete;
    OccReader& operator=(OccReader&& rhs) { swap(rhs); return *this; }

    void set(const Mutex *mutex, size_t localValIdx0) {
        mutex_ = mutex;
        localValIdx = localValIdx0;
    }

    /**
     * Call this before read the resource.
     */
    void prepare() {
        assert(mutex_);
        for (;;) {
            lockD_ = mutex_->loadAcquire();
            if (!lockD_.isLocked()) break;
            _mm_pause();
        }
    }
    /**
     * Call this just after read the resource.
     */
    void readFence() const {
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
    }
    /**
     * Call this to verify read data is valid or not.
     */
    bool verifyAll() const {
        assert(mutex_);
        const LockData lockD = mutex_->load();
        return !lockD.isLocked() && lockD_.getVersion() == lockD.getVersion();
    }
    bool verifyVersion() const {
        assert(mutex_);
        const LockData lockD = mutex_->load();
        return lockD_.getVersion() == lockD.getVersion();
    }
    uintptr_t getMutexId() const {
        return uintptr_t(mutex_);
    }
private:
    void swap(OccReader& rhs) {
        std::swap(mutex_, rhs.mutex_);
        std::swap(lockD_, rhs.lockD_);
        std::swap(localValIdx, rhs.localValIdx);
    }
};


struct WriteEntry
{
    using Mutex = OccLock::Mutex;
    Mutex *mutex;
    void *sharedVal;
    size_t localValIdx;  // index in the local data area.

    WriteEntry() : mutex(), sharedVal(), localValIdx() {
    }
    WriteEntry(const WriteEntry&) = delete;
    WriteEntry(WriteEntry&& rhs) : WriteEntry() { swap(rhs); }
    WriteEntry& operator=(const WriteEntry&) = delete;
    WriteEntry& operator=(WriteEntry&& rhs) { swap(rhs); return *this; }

    bool operator<(const WriteEntry& rhs) const {
        return getMutexId() < rhs.getMutexId();
    }
    void set(Mutex *mutex0, void *sharedVal0, size_t localValIdx0) {
        mutex = mutex0;
        sharedVal = sharedVal0;
        localValIdx = localValIdx0;
    }
    uintptr_t getMutexId() const {
        return uintptr_t(mutex);
    }
private:
    void swap(WriteEntry& rhs) {
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
    void init(size_t valueSize) {
        valueSize_ = valueSize;  // 0 can be allowed.

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
        ReadV::iterator itR = findInReadSet(uintptr_t(&mutex));
        if (itR != readV_.end()) {
            localValIdx = itR->localValIdx;
        } else {
            // For blind-write, you must check write set also.
            WriteV::iterator itW = findInWriteSet(uintptr_t(&mutex));
            if (itW == writeV_.end()) {
                // allocate new local value area.
                localValIdx = local_.size();
#ifndef NO_PAYLOAD
                local_.resize(localValIdx + 1);
#endif
            } else {
                localValIdx = itW->localValIdx;
            }
            readV_.emplace_back();
            OccReader& r = readV_.back();
            r.set(&mutex, localValIdx);
            for (;;) {
                r.prepare();
                // read shared data.
#ifndef NO_PAYLOAD
                ::memcpy(&local_[localValIdx], sharedVal, valueSize_);
#endif
                r.readFence();
                if (r.verifyAll()) break;
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
        WriteV::iterator itW = findInWriteSet(uintptr_t(&mutex));
        if (itW != writeV_.end()) {
            localValIdx = itW->localValIdx;
        } else {
            ReadV::iterator itR = findInReadSet(uintptr_t(&mutex));
            if (itR == readV_.end()) {
                // allocate new local value area.
                localValIdx = local_.size();
#ifndef NO_PAYLOAD
                local_.resize(localValIdx + 1);
#endif
            } else {
                localValIdx = itR->localValIdx;
            }
            writeV_.emplace_back();
            WriteEntry& w = writeV_.back();
            w.set(&mutex, sharedVal, localValIdx);
        }
        // write local data.
#ifndef NO_PAYLOAD
        ::memcpy(&local_[localValIdx], localVal, valueSize_);
#endif
    }
    void lock() {
        std::sort(writeV_.begin(), writeV_.end());
        for (WriteEntry& w : writeV_) {
            lockV_.emplace_back(w.mutex);
        }
        // Serialization point.
        __atomic_thread_fence(__ATOMIC_ACQ_REL);
    }
    bool verify() {
        const bool useIndex = shouldUseIndex(writeV_);
        if (!useIndex) {
            std::sort(writeV_.begin(), writeV_.end());
        }
        for (OccReader& r : readV_) {
            bool inWriteSet;
            if (useIndex) {
                inWriteSet = findInWriteSet(r.getMutexId()) != writeV_.end();
            } else {
                WriteEntry w;
                w.set((Mutex *)r.getMutexId(), nullptr, 0);
                inWriteSet = std::binary_search(writeV_.begin(), writeV_.end(), w);
            }
            const bool valid = inWriteSet ? r.verifyVersion() : r.verifyAll();
            if (!valid) return false;
        }
        return true;
    }
    void updateAndUnlock() {
        assert(lockV_.size() == writeV_.size());
        auto itLk = lockV_.begin();
        auto itW = writeV_.begin();
        while (itLk != lockV_.end()) {
            assert(itW != writeV_.end());
            itLk->update();
#ifndef NO_PAYLOAD
            // writeback
            ::memcpy(itW->sharedVal, &local_[itW->localValIdx], valueSize_);
#endif
            itLk->unlock();
            ++itLk;
            ++itW;
        }
        clear();
    }
    void clear() {
        lockV_.clear();
        readV_.clear();
        readM_.clear();
        writeV_.clear();
        writeM_.clear();
        local_.clear();
    }
    bool empty() const {
        return lockV_.empty() &&
            readV_.empty() &&
            readM_.empty() &&
            writeV_.empty() &&
            writeM_.empty() &&
            local_.empty();
    }
private:
    ReadV::iterator findInReadSet(uintptr_t key) {
        return findInSet(
            key, readV_, readM_,
            [](const OccReader& r) { return r.getMutexId(); });
    }
    WriteV::iterator findInWriteSet(uintptr_t key) {
        return findInSet(
            key, writeV_, writeM_,
            [](const WriteEntry& w) { return w.getMutexId(); });
    }
    /**
     * func: uintptr_t(const Vector::value_type&)
     */
    template <typename Vector, typename Map, typename Func>
    typename Vector::iterator findInSet(uintptr_t key, Vector& vec, Map& map, Func&& func) {
        if (shouldUseIndex(vec)) {
            // create indexes.
            for (size_t i = map.size(); i < vec.size(); i++) {
                map[func(vec[i])] = i;
            }
            // use indexes.
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


}} // namespace cybozu::occ
