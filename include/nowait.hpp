#pragma once
#include "lock.hpp"
#include "write_set.hpp"
#include "vector_payload.hpp"
#include "cache_line_size.hpp"
#include "allocator.hpp"


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
    void init(size_t valueSize) {
        valueSize_ = valueSize;
        if (valueSize == 0) valueSize++;
#ifdef MUTEX_ON_CACHELINE
        local_.setSizes(valueSize, CACHE_LINE_SIZE);
#else
        local_.setSizes(valueSize);
#endif
    }

    bool read(Mutex& mutex, void* sharedVal, void* dst) {
        Vec::iterator it = find(uintptr_t(&mutex));
        if (it != vec_.end()) {
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
        vec_.emplace_back();
        Lock& lk = vec_.back().lock;
        if (!lk.tryLock(&mutex, Mode::S)) {
            return false; // should die.
        }
        copyValue(dst, sharedVal); // read shared data.
        return true;
    }
    bool write(Mutex& mutex, void* sharedVal, void* src) {
        Vec::iterator it = find(uintptr_t(&mutex));
        if (it != vec_.end()) {
            Lock& lk = it->lock;
            if (lk.mode() == Mode::S) {
                if (!lk.tryUpgrade()) return false;
                it->info.set(allocateLocalVal(), sharedVal);
            }
            assert(lk.mode() == Mode::X || lk.mode() == Mode::Invalid);
            copyValue(getLocalValPtr(it->info), src); // write local data.
            return true;
        }
        // This is blind write.
        vec_.emplace_back();
        OpEntryL& ope = vec_.back();
        // Lock will be tried later. See blindWriteLockAll().
        ope.lock.setMutex(&mutex); // for search.
        bwV_.emplace_back(&mutex, vec_.size() - 1);
        ope.info.set(allocateLocalVal(), sharedVal);
        copyValue(getLocalValPtr(ope.info), src); // write local data.
        return true;
    }
    bool readForUpdate(Mutex& mutex, void* sharedVal, void* dst) {
        Vec::iterator it = find(uintptr_t(&mutex));
        if (it != vec_.end()) {
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
        vec_.emplace_back();
        OpEntryL& ope = vec_.back();
        Lock& lk = ope.lock;
        LocalValInfo& info = ope.info;
        if (!lk.tryLock(&mutex, Mode::X)) {
            return false; // should die.
        }
        info.set(allocateLocalVal(), sharedVal);
        void* localVal = getLocalValPtr(info);
        copyValue(localVal, sharedVal); // for next read.
        copyValue(dst, localVal); // read local data.
        return true;
    }

    bool blindWriteLockAll() {
        for (BlindWriteInfo& bwInfo : bwV_) {
            OpEntryL& ope = vec_[bwInfo.idx];
            assert(ope.lock.mode() == Mode::Invalid);
            if (!ope.lock.tryLock(bwInfo.mutex, Mode::X)) {
                return false; // should die
            }
        }
        return true;
    }
    void updateAndUnlock() {
        // serialization point.

        for (OpEntryL& ope : vec_) {
            Lock& lk = ope.lock;
            if (lk.mode() == Mode::X) {
                // update.
                LocalValInfo& info = ope.info;
                copyValue(info.sharedVal, getLocalValPtr(info));
            } else {
                assert(lk.mode() == Mode::S);
            }
#if 1  // unlock one by one.
            ope.lock.unlock();
#endif
        }
        vec_.clear();
        index_.clear();
        local_.clear();
        bwV_.clear();
    }
    void unlock() {
        vec_.clear(); // unlock.
        index_.clear();
        local_.clear();
        bwV_.clear();
    }
    bool empty() const {
        return vec_.empty() && index_.empty();
    }
private:
    Vec::iterator find(uintptr_t key) {
        // at most 4KiB scan.
        const size_t threshold = 4096 / sizeof(OpEntryL);
        if (vec_.size() > threshold) {
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
#endif
    }
    size_t allocateLocalVal() {
        const size_t idx = local_.size();
#ifndef NO_PAYLOAD
        local_.resize(idx + 1);
#endif
        return idx;
    }
};

}} //namespace cybozu::lock
