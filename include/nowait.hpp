#pragma once
#include "lock.hpp"
#include "write_set.hpp"
#include "vector_payload.hpp"
#include "allocator.hpp"
#include "inline.hpp"


namespace cybozu {
namespace lock {


class NoWaitLockSet
{
    using Mutex = cybozu::lock::XSMutex;
    using Lock = cybozu::lock::XSLock;
    using Mode = Mutex::Mode;
    using OpEntryL = OpEntry<Lock>;

    using Vec = std::vector<OpEntryL>;
#if 1
    using Index = SingleThreadUnorderedMap<uintptr_t, size_t>;
#else
    using Index = std::unordered_map<uintptr_t, size_t>;
#endif

    Vec vec_;
    Index index_;  // key: mutex pointer, value: index in vec_.

    MemoryVector local_;
    size_t valueSize_;

    struct BlindWriteInfo {
        Mutex *mutex;
        size_t idx; // index of blind-write entry in vec_.

        BlindWriteInfo() {
        }
        BlindWriteInfo(Mutex* mutex0, size_t idx0) : mutex(mutex0), idx(idx0) {
        }
    };
    std::vector<BlindWriteInfo> bwV_;

public:
    void init(size_t valueSize, size_t nrReserve) {
        valueSize_ = valueSize;
        if (valueSize == 0) valueSize++;
        local_.setSizes(valueSize);

        // for long transactions.
        vec_.reserve(nrReserve);
        local_.reserve(nrReserve);
        bwV_.reserve(nrReserve);
    }

    INLINE bool read(Mutex& mutex, void* sharedVal, void* dst) {
        Vec::iterator it = find(uintptr_t(&mutex));
        if (unlikely(it != vec_.end())) {
            Lock& lk = it->lock;
            if (lk.mode() == Mode::S) {
                copyValue(dst, sharedVal); // read shared data.
                return true;
            }
            assert(lk.mode() == Mode::X || lk.mode() == Mode::Invalid);
            copyValue(dst, getLocalValPtr(it->info)); // read local data.
            return true;
        }
        // Try to read lock.
        OpEntryL& ope = vec_.emplace_back();
        Lock& lk = ope.lock;
        if (unlikely(!lk.read_trylock(mutex))) {
            return false; // should die.
        }
        copyValue(dst, sharedVal); // read shared data.
        return true;
    }
    INLINE bool write(Mutex& mutex, void* sharedVal, void* src) {
        Vec::iterator it = find(uintptr_t(&mutex));
        if (unlikely(it != vec_.end())) {
            Lock& lk = it->lock;
            if (lk.mode() == Mode::S) {
                if (unlikely(!lk.tryUpgrade())) return false;
                it->info.set(allocateLocalVal(), sharedVal);
            }
            assert(lk.mode() == Mode::X || lk.mode() == Mode::Invalid);
            copyValue(getLocalValPtr(it->info), src); // write local data.
            return true;
        }
        // This is blind write.
        OpEntryL& ope = vec_.emplace_back();
        // Lock will be tried later. See blindWriteLockAll().
        ope.lock.setMutex(&mutex); // for search.
        bwV_.emplace_back(&mutex, vec_.size() - 1);
        ope.info.set(allocateLocalVal(), sharedVal);
        copyValue(getLocalValPtr(ope.info), src); // write local data.
        return true;
    }
    INLINE bool readForUpdate(Mutex& mutex, void* sharedVal, void* dst) {
        Vec::iterator it = find(uintptr_t(&mutex));
        if (unlikely(it != vec_.end())) {
            Lock& lk = it->lock;
            LocalValInfo& info = it->info;
            if (lk.mode() == Mode::X) {
                copyValue(dst, getLocalValPtr(info)); // read local data.
                return true;
            }
            if (lk.mode() == Mode::S) {
                if (!lk.tryUpgrade()) return false;
                info.set(allocateLocalVal(), sharedVal);
                void *localVal = getLocalValPtr(info);
                copyValue(localVal, sharedVal); // for next read.
                copyValue(dst, localVal); // read local data.
                return true;
            }
            assert(lk.mode() == Mode::Invalid);
            copyValue(dst, getLocalValPtr(info)); // read local data.
            return true;
        }
        // Try to write lock.
        OpEntryL& ope = vec_.emplace_back();
        Lock& lk = ope.lock;
        LocalValInfo& info = ope.info;
        if (unlikely(!lk.write_trylock(mutex))) {
            return false; // should die.
        }
        info.set(allocateLocalVal(), sharedVal);
        void* localVal = getLocalValPtr(info);
        copyValue(localVal, sharedVal); // for next read.
        copyValue(dst, localVal); // read local data.
        return true;
    }

    INLINE bool blindWriteLockAll() {
        for (BlindWriteInfo& bwInfo : bwV_) {
            OpEntryL& ope = vec_[bwInfo.idx];
            assert(ope.lock.mode() == Mode::Invalid);
            if (unlikely(!ope.lock.write_trylock(*bwInfo.mutex))) {
                return false; // should die
            }
        }
        return true;
    }
    INLINE void updateAndUnlock() {
        // serialization point.

        for (OpEntryL& ope : vec_) {
            Lock& lk = ope.lock;
            if (lk.mode() == Mode::X) {
                // update.
                LocalValInfo& info = ope.info;
                copyValue(info.sharedVal, getLocalValPtr(info));
                ope.lock.write_unlock();
            } else {
                assert(lk.mode() == Mode::S);
                ope.lock.read_unlock();
            }
        }
        vec_.clear();
        index_.clear();
        local_.clear();
        bwV_.clear();
    }
    INLINE void unlock() {
        vec_.clear(); // unlock.
        index_.clear();
        local_.clear();
        bwV_.clear();
    }
    bool empty() const {
        return vec_.empty() && index_.empty();
    }
private:
    INLINE Vec::iterator find(uintptr_t key) {
        // at most 4KiB scan.
        const size_t threshold = 4096 / sizeof(OpEntryL);
        if (unlikely(vec_.size() > threshold)) {
            for (size_t i = index_.size(); i < vec_.size(); i++) {
                index_[vec_[i].lock.getMutexId()] = i;
            }
            Index::iterator it = index_.find(key);
            if (it == index_.end()) {
                return vec_.end();
            } else {
                size_t idx = it->second;
                return vec_.begin() + idx;
            }
        }
        return std::find_if(
            vec_.begin(), vec_.end(),
            [&](const OpEntryL& ope) {
                return ope.lock.getMutexId() == key;
            });
    }
    void* getLocalValPtr(const LocalValInfo& info) {
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
    void copyValue(void* dst, const void* src) {
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

}} //namespace cybozu::lock
