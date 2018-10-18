#pragma once
#include <cstddef>
#include <cinttypes>
#include "cache_line_size.hpp"
#include "inline.hpp"


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
    LocalValInfo(LocalValInfo&& rhs) noexcept : LocalValInfo() {
        swap(rhs);
    }
    LocalValInfo& operator=(LocalValInfo&& rhs) noexcept {
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

    void swap(LocalValInfo& rhs) noexcept {
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
    explicit OpEntry(Lock&& lock0) : lock(std::move(lock0)), info() {
    }
    OpEntry(const OpEntry&) = delete;
    OpEntry& operator=(const OpEntry&) = delete;
    OpEntry(OpEntry&& rhs) noexcept : OpEntry() {
        swap(rhs);
    }
    OpEntry& operator=(OpEntry&& rhs) noexcept {
        swap(rhs);
        return *this;
    }
    void swap(OpEntry& rhs) noexcept {
        std::swap(lock, rhs.lock);
        std::swap(info, rhs.info);
    }
};
