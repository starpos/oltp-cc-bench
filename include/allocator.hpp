/**
 * Allocator for temporal memory for each transaction.
 * CAUTION: This is not thread-safe for speed.
 */
#pragma once
#include "cybozu/array.hpp"
#include <cassert>
#include <cstdio>
#include <unordered_map>
#include <map>
#include <list>
#include <deque>
#include "inline.hpp"


#define ALIGNED_SIZE 4096

#if 0
#define DEBUG_PRINT_ALLOCATOR
#endif


template <size_t BulkSize = ALIGNED_SIZE, size_t CacheSize = ALIGNED_SIZE * 4>
class LowOverheadMemoryAllocator
{
    using Buffer = cybozu::AlignedArray<char, BulkSize, false>;

    struct Fragment {
        Buffer buf; // memory buffer. nr > 0.
        uint32_t offset; // The position of non-allocated area.
        uint32_t nr; // Number of allocated.

        explicit Fragment(size_t size = BulkSize)
            : buf(size), offset(0), nr(0) {}

        bool operator<(const Fragment& rhs) const {
            return key() < rhs.key();
        }
        uintptr_t key() const {
            return uintptr_t(buf.data());
        }
        void *alloc(size_t size) {
#if 0
            // If you want to get aligned memory, you should give an appropriate size argument.
            if (size % 8 != 0) { size += (8 - size % 8); }
#endif
            if (buf.size() - offset >= size) {
                void *p = (void *)(buf.data() + offset);
                offset += size;
                nr++;
                return p;
            } else {
                return nullptr;
            }
        }
        bool free() {
            bool ret = --nr == 0;
            if (ret) offset = 0;
            return ret;
        }
    };
    using Map = std::unordered_map<uintptr_t, Fragment>;
    Map map_;
    typename Map::iterator cur_;
    std::list<Fragment> freeQ_;

public:
    LowOverheadMemoryAllocator()
        : map_(), freeQ_() {
#ifdef DEBUG_PRINT_ALLOCATOR
        ::printf("LowOverheadMemoryAllocator: cstr %p\n", this);
#endif
        addNewFragment();
    }
#if 0
    ~LowOverheadMemoryAllocator() noexcept {
    }
#endif
    INLINE void *allocate(size_t size) {
        if (size == 0) return nullptr;
        if (size > BulkSize) {
            void *p = ::malloc(size);
            if (p == nullptr) throw std::bad_alloc();
            return p;
        }
        void *p = cur_->second.alloc(size);
        if (p != nullptr) {
#ifdef DEBUG_PRINT_ALLOCATOR
            ::printf("addr %0lx %p\n", getKey(p), cur_->second.buf.data());
#endif
            assert(getKey(p) == uintptr_t(cur_->second.buf.data()));
            return p;
        }
        addNewFragment();
        p = cur_->second.alloc(size);
        assert(p != nullptr);
        return p;
    }
    INLINE void deallocate(void *p, size_t size) {
        if (p == nullptr) return;
        if (size > BulkSize) {
            ::free(p);
            return;
        }
        typename Map::iterator it = map_.find(getKey(p));
        assert(it != map_.end());
        if (!it->second.free()) return;
        if (cur_ == it) return;
#ifdef DEBUG_PRINT_ALLOCATOR
        ::printf("addTofreeQ: %0lx\n", it->first);
#endif
        freeQ_.push_front(std::move(it->second));
        map_.erase(it);
        gc();
    }
    void addNewFragment() {
        bool ret;
        if (freeQ_.empty()) {
            Fragment frag;
            std::tie(cur_, ret) =
                map_.emplace(std::make_pair(frag.key(), std::move(frag)));
        } else {
            Fragment &frag = freeQ_.front();
            std::tie(cur_, ret) =
                map_.emplace(std::make_pair(frag.key(), std::move(frag)));
            freeQ_.pop_front();
        }
#ifdef DEBUG_PRINT_ALLOCATOR
        ::printf("addNewFragment: %0lx %d\n", cur_->first, ret);
        //print();
#endif
        assert(ret);
    }
    static uintptr_t getKey(void *p) {
        uintptr_t ret = uintptr_t(p) & ~(BulkSize - 1);
#ifdef DEBUG_PRINT_ALLOCATOR
        //::printf("key: %0lx %0lx  %p\n", ret, ret + BulkSize, p);
#endif
        return ret;
    }
    void gc() {
        const size_t maxSize = (CacheSize - 1) / BulkSize + 1;
        if (freeQ_.size() > maxSize) {
#ifdef DEBUG_PRINT_ALLOCATOR
            ::printf("shrink %zu -> %zu\n", freeQ_.size(), maxSize);
#endif
            freeQ_.resize(maxSize);
        }
    }
    // for debug.
    void print() const {
        for (const typename Map::value_type& pair : map_) {
            ::printf("%0lx %p nr:%u off:%u\n", pair.first, pair.second.buf.data()
                     , pair.second.nr, pair.second.offset);
        }
    }
};


/**
 * CAUTION: an allocation and the corresponding deallocation must be done by the same thread.
 */
LowOverheadMemoryAllocator<>& memAlloc() {
    thread_local LowOverheadMemoryAllocator<> memAlloc_;
    return memAlloc_;
}

template <typename T>
class LowOverheadAllocatorT
{
public:
    using value_type = T;

#if 0
    template <typename U>
    struct rebind {
        using other = LowOverheadAllocatorT<U>;
    };
#endif

    LowOverheadAllocatorT() {}
    LowOverheadAllocatorT(const LowOverheadAllocatorT&) {}
#if 1
    template <typename U>
    LowOverheadAllocatorT(const LowOverheadAllocatorT<U>&) {}
#endif

    T* allocate(size_t n) {
        T* p = (T *)memAlloc().allocate(n * sizeof(T));
        //::printf("allocate   sizeof(T):%zu nr:%zu %p\n", sizeof(T), n, p); // QQQ
        return p;
    }
    void deallocate(T* p, size_t n) {
        //::printf("deallocate sizeof(T):%zu nr:%zu %p\n", sizeof(T), n, p); // QQQ
        memAlloc().deallocate(p, n * sizeof(T));
    }
};


template <typename Key, typename T>
using SingleThreadUnorderedMap =
    std::unordered_map<Key,
                       T,
                       std::hash<Key>,
                       std::equal_to<Key>,
                       LowOverheadAllocatorT<std::pair<const Key, T> > >;


template <typename Key, typename T>
using SingleThreadMap =
    std::map<Key,
             T,
             std::less<Key>,
             LowOverheadAllocatorT<std::pair<const Key, T> > >;


template <typename T>
using SingleThreadDeque = std::deque<T, LowOverheadAllocatorT<T> >;
