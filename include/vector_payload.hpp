#pragma once
#include <cstdlib>
#include <cstring>
#include <cinttypes>
#include <cassert>
#include <utility>
#include <vector>
#include <iterator>
#include <cstddef>
#include "allocator.hpp"


struct MemoryElement
{
    uint8_t payload[0];
};

static_assert(sizeof(MemoryElement) == 0, "sizeof(MemoryElement) must be 0.");


/**
 * Vector-type container where each element is fixed-size memory.
 * There is no constructor or destructor.
 */
struct MemoryVector
{
protected:
    size_t elemSize_; // [bytes]. must be non-zero. do not change this when nrElem_ > 0.
    size_t alignmentSize_; // [bytes]
    size_t nrElem_; // [nr]
    size_t nrReserved_; // [nr]
    uint8_t *data_;

public:
    explicit MemoryVector(size_t elemSize = 1)
        : elemSize_(elemSize), alignmentSize_(sizeof(uintptr_t))
        , nrElem_(0), nrReserved_(0), data_(nullptr) {
    }
    virtual ~MemoryVector() noexcept {
        if (data_) ::free(data_);
    }
    /*
     * Set element size.
     * DO NOT call this if the container is not empty.
     */
    void setSizes(size_t elemSize, size_t alignmentSize = sizeof(uintptr_t)) {
        if (elemSize == 0) {
            throw std::runtime_error("MemoryVector::setSizes: elemSize must not be 0.");
        }
        if (nrElem_ > 0) {
            throw std::runtime_error("MemoryVector::setSizes: nrElem_ must be 0.");
        }
        if (alignmentSize < sizeof(uintptr_t) || alignmentSize % sizeof(uintptr_t) != 0) {
            throw std::runtime_error("MemoryVector::setSizes: invalid alignmenSize.");
        }
        alignmentSize_ = alignmentSize;

        // calculate aligned payload size.
        size_t alignedElemSize = (((elemSize - 1) / alignmentSize) + 1) * alignmentSize;

        // recalculate reservedSize_.
        const size_t bytes = nrReserved_ * elemSize_; // old element size.
        nrReserved_ = bytes / alignedElemSize;
        elemSize_ = alignedElemSize;
    }

    /*
     * The below functions are the same as std::vector.
     */

    void resize(size_t nrElem) {
        if (nrElem < nrElem_) {
            // shrink.
            nrElem_ = nrElem;
        } else if (nrElem > nrElem_) {
            reserve(nrElem);
            assert(nrElem <= nrReserved_);
            nrElem_ = nrElem;
        } else {
            // do nothing.
        }
    }
    void reserve(size_t nrReserved) {
        if (data_ == nullptr) {
            data_ = allocateNewArray(nrReserved);
            nrReserved_ = nrReserved;
        } else if (nrReserved > nrReserved_) {
            uint8_t *data = allocateNewArray(nrReserved);
            ::memcpy(data, data_, elemSize_ * nrElem_);
            std::swap(data_, data);
            ::free(data);
            nrReserved_ = nrReserved;
        } else {
            // do nothing.
        }
    }
    size_t capacity() const noexcept { return nrReserved_; }

    /**
     * CAUSION: operator[] returns address.
     */
    MemoryElement& operator[](size_t i) {
        return *(MemoryElement *)getAddress(i);
    }
    const MemoryElement& operator[](size_t i) const {
        return *(const MemoryElement *)getAddress(i);
    }
    /**
     * CAUSION: back() returns address.
     */
    MemoryElement& back() {
        return operator[](nrElem_ - 1);
    }
    const MemoryElement& back() const {
        return operator[](nrElem_ - 1);
    }

    size_t size() const { return nrElem_; }
    bool empty() const { return nrElem_ == 0; }

    void pop_back() {
        assert(nrElem_ > 0);
        nrElem_--;
    }
    /**
     * This function does not deallocate memory at all.
     */
    void clear() noexcept { nrElem_ = 0; }


    class iterator {
        friend MemoryVector;

        MemoryVector *c_;
        uintptr_t p_;

    public:
        using value_type = MemoryElement;
        using difference_type = ssize_t;
        using pointer = MemoryElement*;
        using reference = MemoryElement&;
        using iterator_category = std::random_access_iterator_tag;

    private:
        iterator() : c_(nullptr), p_(0) {
        }

    public:
        /*
         * Iterator
         */
        MemoryElement& operator*() {
            return *(MemoryElement*)(p_);
        }
        const MemoryElement& operator*() const {
            return *(const MemoryElement*)(p_);
        }
        iterator& operator++() {
            assert(c_ != nullptr);
            p_ += c_->elemSize_;
            return *this;
        }

        /*
         * InputIterator
         */
        bool operator==(const iterator& rhs) const { return p_ == rhs.p_; }
        bool operator!=(const iterator& rhs) const { return p_ != rhs.p_; }
        MemoryElement* operator->() { return &operator*(); }
        const MemoryElement* operator->() const { return &operator*(); }

        /*
         * ForwardIterator
         */
        iterator operator++(int) {
            iterator it = *this;
            operator++();
            return it;
        }

        /*
         * BidirectionalIterator
         */
        iterator& operator--() {
            assert(c_ != nullptr);
            p_ -= c_->elemSize_;
            return *this;
        }
        iterator operator--(int) {
            iterator it = *this;
            operator--();
            return it;
        }

        /*
         * RandomAccessIterator
         */
        iterator& operator+=(ssize_t i) {
            assert(c_ != nullptr);
            p_ += c_->elemSize_ * i;
            return *this;
        }
        iterator operator+(ssize_t i) {
            iterator it = *this;
            it.p_ += c_->elemSize_ * i;
            return it;
        }
        iterator& operator-=(ssize_t i) {
            assert(c_ != nullptr);
            p_ -= c_->elemSize_ * i;
            return *this;
        }
        iterator operator-(ssize_t i) {
            iterator it = *this;
            it.p_ -= c_->elemSize_ * i;
            return it;
        }
        ssize_t operator-(const iterator& rhs) const {
            assert(c_ != nullptr);
            assert(c_->elemSize_ != 0);
            return (ptrdiff_t(p_) - ptrdiff_t(rhs.p_)) / c_->elemSize_;
        }
        MemoryElement& operator[](ssize_t i) {
            return *(MemoryElement*)(p_ + (c_->elemSize_ * i));
        }
        const MemoryElement& operator[](ssize_t i) const {
            return *(const MemoryElement*)(p_ + (c_->elemSize_ * i));
        }
        bool operator<(const iterator& rhs) const { return p_ < rhs.p_; }
        bool operator>(const iterator& rhs) const { return p_ > rhs.p_; }
        bool operator<=(const iterator& rhs) const { return p_ <= rhs.p_; }
        bool operator>=(const iterator& rhs) const { return p_ >= rhs.p_; }
    };

    friend iterator;

    iterator begin() {
        iterator it;
        it.c_ = this;
        it.p_ = getAddress(0);
        return it;
    }
    iterator end() {
        iterator it;
        it.c_ = this;
        it.p_ = getAddress(nrElem_);
        return it;
    }

protected:
    uint8_t* allocateNewArray(size_t nrElem) const {
        void *p;
        // posix_memalign() may return non-null with size 0.
        if (nrElem == 0) return nullptr;
        if (::posix_memalign(&p, alignmentSize_, elemSize_ * nrElem) != 0) {
            throw std::bad_alloc();
        }
        return (uint8_t *)p;
    }
    uintptr_t getAddress(size_t i, uintptr_t base = 0) const {
        if (base == 0) base = uintptr_t(data_);
        return base + (elemSize_ * i);
    }
    bool isFull() const {
        return nrElem_ == nrReserved_;
    }
};


template <typename T>
struct DataWithPayload
{
    T value;
    uint8_t payload[0];
};


template <typename T>
class VectorWithPayload2 : private MemoryVector
{
public:
    VectorWithPayload2() : MemoryVector(sizeof(T)) {
    }
    ~VectorWithPayload2() {
        callDstrRange(0, nrElem_);
    }
    void setPayloadSize(size_t payloadSize, size_t alignmentSize = sizeof(uintptr_t)) {
        MemoryVector::setSizes(sizeof(T) + payloadSize, alignmentSize);
    }

    /*
     * The below functions are the same as std::vector.
     */

    void resize(size_t nrElem) {
        if (nrElem < nrElem_) {
            // shrink.
            callDstrRange(nrElem, nrElem_);
            nrElem_ = nrElem_;
        } else if (nrElem > nrElem_) {
            // grow.
            reserve(nrElem);
            assert(nrElem <= nrReserved_);
            callCstrRange(nrElem_, nrElem);
            nrElem_ = nrElem;
        } else {
            // do nothing.
        }
    }
    void reserve(size_t nrReserved) {
        if (data_ == nullptr) {
            data_ = allocateNewArray(nrReserved);
            nrReserved_ = nrReserved;
        } else if (nrReserved > nrReserved_) {
            uint8_t *data = allocateNewArray(nrReserved);
            moveToNewArray(data_, data, nrElem_);
            std::swap(data_, data);
            ::free(data);
            nrReserved_ = nrReserved;
        } else {
            // do nothing.
        }
    }
    template <typename... Args>
    void emplace_back(Args&&... args) {
        if (isFull()) reserve((nrElem_ + 1) * 2);
        void *addr = (void *)getAddress(nrElem_);
        nrElem_++;
        new(addr) T(std::forward<Args>(args)...);
    }
    void push_back(T&& t) {
        if (isFull()) reserve((nrElem_ + 1) * 2);
        callCstr(nrElem_);
        operator[](nrElem_).value = std::move(t);
        nrElem_++;
    }
    void push_back(const T& t) {
        if (isFull()) reserve((nrElem_ + 1) * 2);
        callCstr(nrElem_);
        operator[](nrElem_).value = t;
        nrElem_++;
    }
    size_t capacity() const noexcept { return nrReserved_; }

    DataWithPayload<T>& operator[](size_t i) {
        return *(DataWithPayload<T>*)getAddress(i);
    }
    const DataWithPayload<T>& operator[](size_t i) const {
        return *(const DataWithPayload<T>*)getAddress(i);
    }
    DataWithPayload<T>& back() {
        return operator[](nrElem_ - 1);
    }
    const DataWithPayload<T>& back() const {
        return operator[](nrElem_ - 1);
    }
    size_t size() const { return nrElem_; }
    bool empty() const { return nrElem_ == 0; }
    void pop_back() {
        assert(nrElem_ > 0);
        callDstr(nrElem_ - 1);
        nrElem_--;
    }
    void clear() noexcept {
        callDstrRange(0, nrElem_);
        nrElem_ = 0;
    }


    class iterator {
        friend VectorWithPayload2<T>;

        VectorWithPayload2<T> *c_;
        uintptr_t p_;

    public:
        using value_type = DataWithPayload<T>;
        using difference_type = ssize_t;
        using pointer = DataWithPayload<T>*;
        using reference = DataWithPayload<T>&;
        using iterator_category = std::random_access_iterator_tag;

    private:
        iterator() : c_(nullptr), p_(0) {
        }
    public:

        /*
         * Iterator
         */
        DataWithPayload<T>& operator*() {
            return *(DataWithPayload<T>*)(p_);
        }
        const DataWithPayload<T>& operator*() const {
            return *(const DataWithPayload<T>*)(p_);
        }
        iterator& operator++() {
            assert(c_ != nullptr);
            p_ += c_->elemSize_;
            return *this;
        }

        /*
         * InputIterator
         */
        bool operator==(const iterator& rhs) const { return p_ == rhs.p_; }
        bool operator!=(const iterator& rhs) const { return p_ != rhs.p_; }
        DataWithPayload<T>* operator->() { return &operator*(); }
        const DataWithPayload<T>* operator->() const { return &operator*(); }

        /*
         * ForwardIterator
         */
        iterator operator++(int) {
            iterator it = *this;
            operator++();
            return it;
        }

        /*
         * BidirectionalIterator
         */
        iterator& operator--() {
            assert(c_ != nullptr);
            p_ -= c_->elemSize_;
            return *this;
        }
        iterator operator--(int) {
            iterator it = *this;
            operator--();
            return it;
        }

        /*
         * RandomAccessIterator
         */
        iterator& operator+=(ssize_t i) {
            assert(c_ != nullptr);
            p_ += c_->elemSize_ * i;
            return *this;
        }
        iterator operator+(ssize_t i) {
            iterator it = *this;
            it.p_ += c_->elemSize_ * i;
            return it;
        }
        iterator& operator-=(ssize_t i) {
            assert(c_ != nullptr);
            p_ -= c_->elemSize_ * i;
            return *this;
        }
        iterator operator-(ssize_t i) {
            iterator it = *this;
            it.p_ -= c_->elemSize_ * i;
            return it;
        }
        ssize_t operator-(const iterator& rhs) const {
            return (ptrdiff_t(p_) - ptrdiff_t(rhs.p_)) / c_->elemSize_;
        }
        DataWithPayload<T>& operator[](ssize_t i) {
            return *(DataWithPayload<T>*)(p_ + (c_->elemSize_ * i));
        }
        const DataWithPayload<T>& operator[](ssize_t i) const {
            return *(const DataWithPayload<T>*)(p_ + (c_->elemSize_ * i));
        }
        bool operator<(const iterator& rhs) const { return p_ < rhs.p_; }
        bool operator>(const iterator& rhs) const { return p_ > rhs.p_; }
        bool operator<=(const iterator& rhs) const { return p_ <= rhs.p_; }
        bool operator>=(const iterator& rhs) const { return p_ >= rhs.p_; }
    };

    friend iterator;

    iterator begin() {
        iterator it;
        it.c_ = this;
        it.p_ = getAddress(0);
        return it;
    }
    iterator end() {
        iterator it;
        it.c_ = this;
        it.p_ = getAddress(nrElem_);
        return it;
    }

private:
    size_t payloadSize() const {
        assert(elemSize_ >= sizeof(T));
        return elemSize_ - sizeof(T);
    }
    void moveToNewArray(void *src, void *dst, size_t nrElem) const {
        uintptr_t srcU = uintptr_t(src);
        uintptr_t dstU = uintptr_t(dst);
        for (size_t i = 0; i < nrElem; i++) {
            auto *srcP = (DataWithPayload<T>*)(srcU);
            auto *dstP = (DataWithPayload<T>*)(dstU);
            new(&dstP->value) T(std::move(srcP->value)); // move constructor.
            srcP->value.~T();
            ::memcpy(dstP->payload, srcP->payload, payloadSize());
            srcU += elemSize_;
            dstU += elemSize_;
        }
    }
    void callCstrRange(size_t i, size_t j) {
        uintptr_t endAddr = getAddress(j);
        for (uintptr_t addr = getAddress(i); addr < endAddr; addr += elemSize_) {
            new(&((DataWithPayload<T>*)addr)->value) T;
        }
    }
    void callDstrRange(size_t i, size_t j) {
        uintptr_t endAddr = getAddress(j);
        for (uintptr_t addr = getAddress(i); addr < endAddr; addr += elemSize_) {
            ((DataWithPayload<T>*)addr)->value.~T();
        }
    }
    void callCstr(size_t i) {
        uintptr_t addr = getAddress(i);
        new(&((DataWithPayload<T>*)addr)->value) T;
    }
    void callDstr(size_t i) {
        uintptr_t addr = getAddress(i);
        ((DataWithPayload<T>*)addr)->value.~T();
    }
};


/**
 * Vector-type container in which each element has fixed-size payload.
 */
template <typename T>
class VectorWithPayload
{
public:
    using value_type = DataWithPayload<T>;

private:
    size_t payloadSize_; // [bytes]. do not change payloadSize when size_ > 0.
    uint8_t *data_;
    size_t size_;  // number of current items [nr].
    size_t reservedSize_; // allocated memory size [nr].
    size_t alignmentSize_; // alignment [bytes].

public:
    VectorWithPayload()
        : payloadSize_(0), data_(nullptr), size_(0)
        , reservedSize_(0), alignmentSize_(sizeof(uintptr_t)) {
    }
    ~VectorWithPayload() noexcept {
        callDstrRange(0, size_);
        if (data_) ::free(data_);
    }

    void setPayloadSize(size_t payloadSize, size_t alignmentSize = sizeof(uintptr_t)) {
        if (size_ > 0) {
            throw std::runtime_error("setPayloadSize: size_ must be 0.");
        }
        if (alignmentSize < sizeof(uintptr_t) || alignmentSize % sizeof(uintptr_t) != 0) {
            throw std::runtime_error("setPayloadSize: invalid alignmenSize.");
        }
        alignmentSize_ = alignmentSize;

        // calculate aligned payload size.
        size_t elementSize = sizeof(T) + payloadSize;
        size_t alignedElementSize = (((elementSize - 1) / alignmentSize) + 1) * alignmentSize;
        size_t alignedPayloadSize = alignedElementSize - sizeof(T);

        // recalculate reservedSize_.
        const size_t bytes = reservedSize_ * (sizeof(T) + payloadSize_);
        reservedSize_ = bytes / alignedElementSize;
        payloadSize_ = alignedPayloadSize;
    }

    /*
     * The below functions are the same as std::vector.
     */

    void resize(size_t size) {
        if (size < size_) {
            // shrink.
            callDstrRange(size, size_);
            size_ = size;
        } else if (size > size_) {
            reserve(size);
            assert(size <= reservedSize_);
            callCstrRange(size_, size);
            size_ = size;
        } else {
            // do nothing.
        }
    }

    void reserve(size_t size) {
        if (data_ == nullptr) {
            data_ = allocateNewArray(size);
#if 0
            // zero-clear
            ::memset(data_, 0, elemSize() * size);
#endif
            reservedSize_ = size;
        } else if (size > reservedSize_) {
            uint8_t *data = allocateNewArray(size);
            moveToNewArray(data_, data, size_);
            std::swap(data_, data);
            ::free(data);
#if 0
            // zero-clear
            size_t start = reservedSize_ * elemSize();
            size_t end = size * elemSize();
            ::memset(data_ + start, 0, end - start);
#endif
            reservedSize_ = size;
        } else {
            // do nothing.
        }
    }

    template <typename... Args>
    void emplace_back(Args&&... args) {
        if (size_ == reservedSize_) {
            reserve((size_ + 1) * 2);
        }
        void *addr = (void *)getAddress(size_);
        size_++;
        new(addr) T(std::forward<Args>(args)...);
    }
    void push_back(T&& t) {
        if (size_ == reservedSize_) reserve((size_ + 1) * 2);
        callCstr(size_);
        operator[](size_).value = std::move(t);
        size_++;
    }
    void push_back(const T& t) {
        if (size_ == reservedSize_) reserve((size_ + 1) * 2);
        callCstr(size_);
        operator[](size_).value = t;
        size_++;
    }
    size_t capacity() const noexcept {
        return reservedSize_;
    }

    DataWithPayload<T>& operator[](size_t i) {
        return *(DataWithPayload<T>*)getAddress(i);
    }
    const DataWithPayload<T>& operator[](size_t i) const {
        return *(const DataWithPayload<T>*)getAddress(i);
    }
    DataWithPayload<T>& back() {
        return operator[](size_ - 1);
    }
    const DataWithPayload<T>& back() const {
        return operator[](size_ - 1);
    }

    size_t size() const {
        return size_;
    }
    bool empty() const {
        return size_ == 0;
    }

    void pop_back() {
        assert(size_ > 0);
        callDstr(size_ - 1);
        size_--;
    }

    void clear() noexcept {
        callDstrRange(0, size_);
        size_ = 0;
    }

    class iterator {
        friend VectorWithPayload<T>;

        VectorWithPayload<T> *c_;
        uint8_t *p_;

    public:
        using value_type = DataWithPayload<T>;
        using difference_type = ssize_t;
        using pointer = DataWithPayload<T>*;
        using reference = DataWithPayload<T>&;
        using iterator_category = std::random_access_iterator_tag;

    private:
        iterator() : c_(nullptr), p_(nullptr) {
        }
    public:

        /*
         * Iterator
         */
        DataWithPayload<T>& operator*() {
            return *(DataWithPayload<T>*)(p_);
        }
        const DataWithPayload<T>& operator*() const {
            return *(const DataWithPayload<T>*)(p_);
        }
        iterator& operator++() {
            assert(c_ != nullptr);
            p_ += c_->elemSize();
            return *this;
        }

        /*
         * InputIterator
         */
        bool operator==(const iterator& rhs) const { return p_ == rhs.p_; }
        bool operator!=(const iterator& rhs) const { return p_ != rhs.p_; }
        DataWithPayload<T>* operator->() { return &operator*(); }
        const DataWithPayload<T>* operator->() const { return &operator*(); }

        /*
         * ForwardIterator
         */
        iterator operator++(int) {
            iterator it = *this;
            operator++();
            return it;
        }

        /*
         * BidirectionalIterator
         */
        iterator& operator--() {
            assert(c_ != nullptr);
            p_ -= c_->elemSize();
            return *this;
        }
        iterator operator--(int) {
            iterator it = *this;
            operator--();
            return it;
        }

        /*
         * RandomAccessIterator
         */
        iterator& operator+=(ssize_t i) {
            assert(c_ != nullptr);
            p_ += c_->elemSize() * i;
            return *this;
        }
        iterator operator+(ssize_t i) {
            iterator it = *this;
            it.p_ += c_->elemSize() * i;
            return it;
        }
        iterator& operator-=(ssize_t i) {
            assert(c_ != nullptr);
            p_ -= c_->elemSize() * i;
            return *this;
        }
        iterator operator-(ssize_t i) {
            iterator it = *this;
            it.p_ -= c_->elemSize() * i;
            return it;
        }
        ssize_t operator-(const iterator& rhs) const {
            return (ptrdiff_t(p_) - ptrdiff_t(rhs.p_)) / c_->elemSize();
        }
        DataWithPayload<T>& operator[](ssize_t i) {
            return *(DataWithPayload<T>*)(p_ + (c_->elemSize() * i));
        }
        const DataWithPayload<T>& operator[](ssize_t i) const {
            return *(const DataWithPayload<T>*)(p_ + (c_->elemSize() * i));
        }
        bool operator<(const iterator& rhs) const {
            return uintptr_t(p_) < uintptr_t(rhs.p_);
        }
        bool operator>(const iterator& rhs) const {
            return uintptr_t(p_) > uintptr_t(rhs.p_);
        }
        bool operator<=(const iterator& rhs) const {
            return uintptr_t(p_) <= uintptr_t(rhs.p_);
        }
        bool operator>=(const iterator& rhs) const {
            return uintptr_t(p_) >= uintptr_t(rhs.p_);
        }
    };

    class const_iterator {
        friend VectorWithPayload<T>;

        VectorWithPayload<T> *c_;
        uint8_t *p_;

    public:
        using value_type = DataWithPayload<T>;
        using difference_type = ssize_t;
        using pointer = const DataWithPayload<T>*;
        using reference = const DataWithPayload<T>&;
        using iterator_category = std::random_access_iterator_tag;

    private:
        const_iterator() : c_(nullptr), p_(nullptr) {
        }
    public:

        /*
         * Iterator
         */
        const DataWithPayload<T>& operator*() const {
            return *(const DataWithPayload<T>*)(p_);
        }
        const_iterator& operator++() {
            assert(c_ != nullptr);
            p_ += c_->elemSize();
            return *this;
        }

        /*
         * InputIterator
         */
        bool operator==(const const_iterator& rhs) const { return p_ == rhs.p_; }
        bool operator!=(const const_iterator& rhs) const { return p_ != rhs.p_; }
        const DataWithPayload<T>* operator->() const { return &operator*(); }

        /*
         * ForwardIterator
         */
        const_iterator operator++(int) {
            const_iterator it = *this;
            operator++();
            return it;
        }

        /*
         * BidirectionalIterator
         */
        const_iterator& operator--() {
            assert(c_ != nullptr);
            p_ -= c_->elemSize();
            return *this;
        }
        const_iterator operator--(int) {
            const_iterator it = *this;
            operator--();
            return it;
        }

        /*
         * RandomAccessIterator
         */
        const_iterator& operator+=(ssize_t i) {
            assert(c_ != nullptr);
            p_ += c_->elemSize() * i;
            return *this;
        }
        const_iterator operator+(ssize_t i) {
            const_iterator it = *this;
            it.p_ += c_->elemSize() * i;
            return it;
        }
        const_iterator& operator-=(ssize_t i) {
            assert(c_ != nullptr);
            p_ -= c_->elemSize() * i;
            return *this;
        }
        const_iterator operator-(ssize_t i) {
            const_iterator it = *this;
            it.p_ -= c_->elemSize() * i;
            return it;
        }
        ssize_t operator-(const const_iterator& rhs) const {
            return (ptrdiff_t(p_) - ptrdiff_t(rhs.p_)) / c_->elemSize();
        }
        const DataWithPayload<T>& operator[](ssize_t i) const {
            return *(const DataWithPayload<T>*)(p_ + (c_->elemSize() * i));
        }
        bool operator<(const const_iterator& rhs) const {
            return uintptr_t(p_) < uintptr_t(rhs.p_);
        }
        bool operator>(const const_iterator& rhs) const {
            return uintptr_t(p_) > uintptr_t(rhs.p_);
        }
        bool operator<=(const const_iterator& rhs) const {
            return uintptr_t(p_) <= uintptr_t(rhs.p_);
        }
        bool operator>=(const const_iterator& rhs) const {
            return uintptr_t(p_) >= uintptr_t(rhs.p_);
        }
    };


    friend iterator;
    friend const_iterator;

    iterator begin() {
        iterator it;
        it.c_ = this;
        it.p_ = (uint8_t *)getAddress(0);
        return it;
    }
    iterator end() {
        iterator it;
        it.c_ = this;
        it.p_ = (uint8_t *)getAddress(size_);
        return it;
    }
    const_iterator begin() const {
        return cbegin();
    }
    const_iterator end() const {
        return cend();
    }
    const_iterator cbegin() const {
        const_iterator it;
        it.c_ = this;
        it.p_ = (uint8_t *)getAddress(0);
    }
    const_iterator cend() const {
        const_iterator it;
        it.c_ = this;
        it.p_ = (uint8_t *)getAddress(size_);
    }

    /**
     * For debug.
     */
    void dump() const {
        if (data_ == nullptr) return;
        for (size_t i = 0; i < size_; i++) {
            size_t off0 = elemSize() * i;
            ::printf("%04zu", i);
            for (size_t j = 0; j < elemSize(); j++) {
                size_t off1 = off0 + j;
                if (off1 % 4 == 0) ::printf(" ");
                ::printf("%02x", data_[off1]);
            }
            ::printf("\n");
        }
    }

    size_t elemSize() const {
        return sizeof(T) + payloadSize_;
    }
private:
    uintptr_t getAddress(size_t i, uintptr_t base = 0) const {
        if (base == 0) base = uintptr_t(data_);
        return base + (elemSize() * i);
    }
    uint8_t* allocateNewArray(size_t size) const {
        void *p;
        if (size == 0) return nullptr;
        if (::posix_memalign(&p, alignmentSize_, elemSize() * size) != 0) {
            throw std::bad_alloc();
        }
        return (uint8_t *)p;
    }
    void moveToNewArray(void *src, void *dst, size_t nr) const {
#if 0
        for (size_t off = 0; off < nr * elemSize(); off += elemSize()) {
            auto *src0 = (DataWithPayload<T>*)(uintptr_t(src) + off);
            auto *dst0 = (DataWithPayload<T>*)(uintptr_t(dst) + off);
            new(&dst0->value) T(std::move(src0->value));  // move constructor.
            src0->value.~T();
            ::memcpy(dst0->payload, src0->payload, payloadSize_);
        }
#else
        size_t srcU = uintptr_t(src);
        size_t dstU = uintptr_t(dst);
        for (size_t i = 0; i < nr; i++) {
            auto *srcP = (DataWithPayload<T>*)srcU;
            auto *dstP = (DataWithPayload<T>*)dstU;
            new(&dstP->value) T(std::move(srcP->value)); // move constructor.
            srcP->value.~T();
            ::memcpy(dstP->payload, srcP->payload, payloadSize_);
            srcU += elemSize();
            dstU += elemSize();
        }
#endif
    }
    void callCstrRange(size_t i, size_t j) {
        size_t endAddr = getAddress(j);
        for (uintptr_t addr = getAddress(i); addr < endAddr; addr += elemSize()) {
            new(&((DataWithPayload<T>*)addr)->value) T;
        }
    }
    void callDstrRange(size_t i, size_t j) {
        size_t endAddr = getAddress(j);
        for (uintptr_t addr = getAddress(i); addr < endAddr; addr += elemSize()) {
            ((DataWithPayload<T>*)addr)->value.~T();
        }
    }
    void callCstr(size_t i) {
        uintptr_t addr = getAddress(i);
        new(&((DataWithPayload<T>*)addr)->value) T;
    }
    void callDstr(size_t i) {
        uintptr_t addr = getAddress(i);
        ((DataWithPayload<T>*)addr)->value.~T();
    }
};






/*
 *  MemoryBuffer is not tested well... (2018-01-18)
 */

template <typename Allocator = std::allocator<uint8_t> >
class MemoryBufferT
{
public:
    size_t size;
    uint8_t *data;

    MemoryBufferT() : size(0), data(nullptr) {
    }
    explicit MemoryBufferT(size_t n) : MemoryBufferT() {
        allocate(n);
    }
    ~MemoryBufferT() noexcept {
        deallocate();
    }
    MemoryBufferT(const MemoryBufferT& rhs) : MemoryBufferT(rhs.size) {
        ::memcpy(data, rhs.data, size);
    }
    MemoryBufferT(MemoryBufferT&& rhs) : MemoryBufferT() {
        std::swap(rhs.data, data);
        std::swap(rhs.size, size);
    }
    MemoryBufferT& operator=(const MemoryBufferT& rhs) {
        if (rhs.size != rhs.size) {
            // Is it waste? If you feel so, you should use std::vector<char>.
            deallocate();
            allocate(rhs.size);
        }
        ::memcpy(data, rhs.data, size);
        return *this;
    }
    MemoryBufferT& operator=(MemoryBufferT&& rhs) {
        std::swap(rhs.data, data);
        std::swap(rhs.size, size);
        return *this;
    }
    void allocate(size_t n) {
        deallocate();
        data = getAllocator().allocate(n);
        size = n;
    }
    void deallocate() {
        getAllocator().deallocate(data, size);
        data = nullptr;
        size = 0;
    }
private:
    static Allocator& getAllocator() {
        static Allocator allocator_;
        return allocator_;
    }
};


using MemoryBuffer = MemoryBufferT<LowOverheadAllocatorT<uint8_t> >;


template <typename T>
using VectorWithMemory = std::vector<std::pair<T, MemoryBuffer> >;
