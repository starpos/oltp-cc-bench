#pragma once
/**
 * @file
 * @brief an optimistic concurrency control method.
 * @author Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 *
 * (C) 2016 Cybozu Labs, Inc.
 */
#include <stdexcept>
#include <immintrin.h>
#include <unordered_map>
#include "lock.hpp"

#define USE_OCC_MCS
//#undef USE_OCC_MCS


namespace cybozu {
namespace occ {

constexpr size_t CACHE_LINE_SIZE = 64;

struct OccLockData
{
    /*
     * 0-30(31bits) record version
     * 31(1bit) X lock flag.
     */
    uint32_t obj;
    static constexpr uint32_t mask = (0x1 << 31);

    OccLockData() : obj(0) {}
    OccLockData load() const {
        uint32_t x = __atomic_load_n(&obj, __ATOMIC_RELAXED);
        OccLockData *lockD = reinterpret_cast<OccLockData*>(&x);
        return *lockD;
    }
    bool compareAndSwap(const OccLockData& before, const OccLockData& after) {
        return __atomic_compare_exchange(&obj, (uint32_t *)&before, (uint32_t *)&after, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
    }
    void set(const OccLockData& after) {
        obj = after.obj;
    }
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
#ifdef MUTEX_ON_CACHELINE
    alignas(CACHE_LINE_SIZE)
#endif
    OccLockData lockD;
#ifdef USE_OCC_MCS
    cybozu::lock::McsSpinlock::Mutex mcsMutex;
#endif
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

        lockD_ = mutex_->lockD.load();
        for (;;) {
            if (lockD_.isLocked()) waitFor();
            LockData lockD = lockD_;
            lockD.setLock();
            if (mutex_->lockD.compareAndSwap(lockD_, lockD)) {
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
#if 0
        if (!mutex_->lockD.compareAndSwap(lockD_, lockD)) {
            throw std::runtime_error("OccLock::unlock: CAS failed. Something is wrong.");
        }
#else
        writeFence();
        mutex_->lockD.set(lockD);
#endif
        mutex_ = nullptr;
    }
    void update() {
        updated_ = true;
    }
    /**
     * Call this just after update the resource.
     */
    void writeFence() const {
        __atomic_thread_fence(__ATOMIC_RELEASE);
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
        for (;;) {
            lockD_ = mutex_->lockD.load();
            if (!lockD_.isLocked()) return;
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
    OccReader() : mutex_(), lockD_() {}
    OccReader(const OccReader&) = delete;
    OccReader(OccReader&& rhs) : OccReader() { swap(rhs); }
    OccReader& operator=(const OccReader&) = delete;
    OccReader& operator=(OccReader&& rhs) { swap(rhs); return *this; }
    /**
     * Call this before read the resource.
     */
    void prepare(const Mutex *mutex) {
        mutex_ = mutex;
        for (;;) {
            lockD_ = mutex_->lockD.load();
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
        if (!mutex_) throw std::runtime_error("OccReader::verify: mutex_ is null");
        const LockData lockD = mutex_->lockD.load();
        return !lockD.isLocked() && lockD_.getVersion() == lockD.getVersion();
    }
    bool verifyVersion() const {
        if (!mutex_) throw std::runtime_error("OccReader::verify: mutex_ is null");
        const LockData lockD = mutex_->lockD.load();
        return lockD_.getVersion() == lockD.getVersion();
    }
    uintptr_t getMutexId() const {
        return uintptr_t(mutex_);
    }
private:
    void swap(OccReader& rhs) {
        std::swap(mutex_, rhs.mutex_);
        std::swap(lockD_, rhs.lockD_);
    }
};


class LockSet
{
public:
    using Mutex = OccMutex;

private:
    using LockV = std::vector<OccLock>;
    using ReadV = std::vector<OccReader>;
    using WriteV = std::vector<uintptr_t>; // mutex pointers.
    using IndexM = std::unordered_map<uintptr_t, size_t>;

    WriteV writeV_; // write set.
    IndexM writeM_; // write set index.
    ReadV readV_; // read set.
    IndexM readM_; // read set index.
    LockV lockV_;

public:
    void read(Mutex& mutex) {
        ReadV::iterator it = findInReadSet(uintptr_t(&mutex));
        if (it != readV_.end()) {
            // read local data.
            return;
        }
        readV_.emplace_back();
        OccReader& r = readV_.back();
        for (;;) {
            r.prepare(&mutex);
            // read shared data.
            r.readFence();
            if (r.verifyAll()) break;
        }
    }
    void write(Mutex& mutex) {
        WriteV::iterator it = findInWriteSet(uintptr_t(&mutex));
        if (it != writeV_.end()) {
            // write local data.
            return;
        }
        writeV_.push_back(uintptr_t(&mutex));
        // write local data.
    }
    void lock() {
        std::sort(writeV_.begin(), writeV_.end());
        for (uintptr_t mutex : writeV_) {
            lockV_.emplace_back((Mutex *)mutex);
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
                inWriteSet = std::binary_search(
                    writeV_.begin(), writeV_.end(), r.getMutexId());
            }
            const bool valid = inWriteSet ? r.verifyVersion() : r.verifyAll();
            if (!valid) return false;
        }
        return true;
    }
    void updateAndUnlock() {
        for (OccLock& lk : lockV_) {
            lk.update();
            lk.unlock();
        }
        clear();
    }
    void clear() {
        lockV_.clear();
        readV_.clear();
        readM_.clear();
        writeV_.clear();
        writeM_.clear();
    }
    bool empty() const {
        return lockV_.empty() &&
            readV_.empty() &&
            readM_.empty() &&
            writeV_.empty() &&
            writeM_.empty();
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
            [](uintptr_t v) { return v; });
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
