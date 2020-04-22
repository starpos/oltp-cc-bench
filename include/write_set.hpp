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

    INLINE LocalValInfo() : localValIdx(UINT64_MAX), sharedVal(nullptr) {
    }
    INLINE LocalValInfo(size_t localValIdx0, void *sharedVal0)
        : localValIdx(localValIdx0), sharedVal(sharedVal0) {
    }
    INLINE LocalValInfo(const LocalValInfo&) = default;
    INLINE LocalValInfo& operator=(const LocalValInfo&) = default;
    INLINE LocalValInfo(LocalValInfo&& rhs) noexcept : LocalValInfo() {
        swap(rhs);
    }
    INLINE LocalValInfo& operator=(LocalValInfo&& rhs) noexcept {
        swap(rhs);
        return *this;
    }

    INLINE void set(size_t localValIdx0, void *sharedVal0) {
        localValIdx = localValIdx0;
        sharedVal = sharedVal0;
    }
    INLINE void reset() {
        localValIdx = UINT64_MAX;
        sharedVal = nullptr;
    }

    INLINE void swap(LocalValInfo& rhs) noexcept {
        std::swap(localValIdx, rhs.localValIdx);
        std::swap(sharedVal, rhs.sharedVal);
    }
};


/**
 * Lock object and localValInfo object.
 * Lock object must be default constructible and movable.
 */
template <typename Lock>
struct OpEntry
{
    Lock lock;
    LocalValInfo info;

    INLINE OpEntry() : lock(), info() {
    }
    INLINE explicit OpEntry(Lock&& lock0) : lock(std::move(lock0)), info() {
    }
    OpEntry(const OpEntry&) = delete;
    OpEntry& operator=(const OpEntry&) = delete;
    INLINE OpEntry(OpEntry&& rhs) noexcept : OpEntry() {
        swap(rhs);
    }
    INLINE OpEntry& operator=(OpEntry&& rhs) noexcept {
        swap(rhs);
        return *this;
    }
    INLINE void swap(OpEntry& rhs) noexcept {
        std::swap(lock, rhs.lock);
        std::swap(info, rhs.info);
    }
};
