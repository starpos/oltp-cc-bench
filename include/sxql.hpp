#pragma once
/**
 * Shared eXclusive Queuing Lock.
 */
#include <immintrin.h>
#include <cstring>
#include <cinttypes>
#include "util.hpp"
#include "allocator.hpp"

//#define DEBUG_PRINT
#undef DEBUG_PRINT


namespace cybozu {
namespace lock {

namespace sxql_local {

constexpr size_t CACHE_LINE_SIZE = 64;

} // namepsace sxql_local


/**
 * Constraints.
 *
 * (1) The least 3 bits of the Node pointers will be used for special purpose.
 *     So the nodes must be aligned by 8 bytes.
 * (2) We also assume the pointer's most 16bits will be unused.
 */


namespace sxq {


using uint128_t = __uint128_t;

struct Node;


struct NodePtrAndIsWriter
{
    uintptr_t ptrAndBit;

    operator uintptr_t() const {
        return ptrAndBit;
    }
    NodePtrAndIsWriter() = default;
    NodePtrAndIsWriter(uintptr_t v) : ptrAndBit(v) {
    }

    void set(const Node* node, bool isWriter) {
        ptrAndBit = uintptr_t(node) | isWriter;
    }
    const Node* getNode() const {
        return (const Node *)(ptrAndBit & ~0x1);
    }
    Node* getNode() {
        return (Node *)(ptrAndBit & ~0x1);
    }
    bool isWriter() const {
        return (ptrAndBit & 0x1) != 0;
    }
    void init() {
        ptrAndBit = 0;
    }
};


struct Node
{
#ifdef MUTEX_ON_CACHELINE
    alignas(sxql_local::CACHE_LINE_SIZE)
#else
    alignas(8)
#endif
    uintptr_t nextAndIsWriter;
    bool wait;

    void init() {
        nextAndIsWriter = 0;
        wait = false;
    }

    void setNextAndIsWriter(const Node* node, bool isWriter) {
        NodePtrAndIsWriter v;
        v.set(node, isWriter);
        __atomic_store_n(&nextAndIsWriter, v, __ATOMIC_RELEASE);
    }
    NodePtrAndIsWriter loadNextAndIsWriter() const {
        return __atomic_load_n(&nextAndIsWriter, __ATOMIC_ACQUIRE);
    }
};


constexpr uint MAX_READERS = (1U << 8) - 1;


struct LockData
{
    uint isTailWriter:1;
    uint unused1:2;
    uint64_t tail:45;
    uint waitingPhase:8;
    uint lockingPhase:8;
    uint isNextWriter:1;
    uint unused2:2;
    uint64_t next:45;
    uint nrReaders:8;
    uint unused3:8;

    operator uint128_t() {
        uint128_t ret;
        ::memcpy(&ret, this, sizeof(ret));
        return ret;
    }
    LockData() = default;
    LockData(uint128_t obj) {
        ::memcpy(this, &obj, sizeof(*this));
    }

    void init() {
        ::memset(this, 0, sizeof(*this));
    }

    void setTail(const Node *node) {
        tail = (uint64_t(node) >> 3);
    }
    void setNext(const Node *node) {
        next = (uint64_t(node) >> 3);
    }

    const Node *getTail() const {
        return (const Node *)(tail << 3);
    }
    Node *getTail() {
        return (Node *)(tail << 3);
    }
    const Node *getNext() const {
        return (const Node *)(next << 3);
    }
    Node *getNext() {
        return (Node *)(next << 3);
    }

    bool isSamePhase() const {
        return waitingPhase == lockingPhase;
    }

    std::string str() const {
        return cybozu::util::formatString(
            "LockData isTailWriter:%u tail:%p "
            "waiting/locking:%u/%u isNextWriter:%u next:%p nrReaders:%u"
            , isTailWriter, getTail(), waitingPhase, lockingPhase
            , isNextWriter, getNext(), nrReaders);
    }
    void print() const {
        ::printf("%s\n", str().c_str());
    }
};

static_assert(sizeof(LockData) == 16, "LockData is not 128bits");


#if 0
thread_local std::vector<LockData> ld_;
#endif


struct Mutex
{
    enum class Mode : uint8_t { Invalid = 0, X, S, };

#ifdef MUTEX_ON_CACHELINE
    alignas(sxql_local::CACHE_LINE_SIZE)
#else
    alignas(16)
#endif
    uint128_t obj;

    Mutex() : obj(0) {}

    LockData atomicLoad() const {
        return __atomic_load_n(&obj, __ATOMIC_ACQUIRE);
    }
    bool compareAndSwap(LockData& before, const LockData &after) {
#if 1
        return __atomic_compare_exchange(
            &obj, (uint128_t *)&before, (uint128_t *)&after,
            false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
#else
        LockData after2 = after; // debug
        after2.unused3++;

        bool ret = __atomic_compare_exchange(
            &obj, (uint128_t *)&before, (uint128_t *)&after2,
            false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
        if (ret) {
            ld_.push_back(before);
            ld_.push_back(after2);
        }
        return ret;
#endif
    }

    std::string str() const {
        return cybozu::util::formatString(
            "sxq::Mutex %p %s"
            , this, ((LockData *)&obj)->str().c_str());
    }
};



} // namespace sxq


class SXQLock
{
public:
    using Mutex = sxq::Mutex;
    using Node = sxq::Node;
    using Mode = sxq::Mutex::Mode;

private:
    using LockData = sxq::LockData;

    Mutex *mutex_;
    Mode mode_;

    LowOverheadAllocatorT<Node> allocator_;

    // This is pointer in order to support SXQLock move.
    // Do not rellocate the memory during locked.
    Node* nodeP_;

public:
    SXQLock()
        : mutex_(nullptr)
        , mode_(Mode::Invalid)
        , allocator_()
        , nodeP_(allocator_.allocate(1)) {
    }
    SXQLock(Mutex* mutex, Mode mode) : SXQLock() {
        lock(mutex, mode);
    }
    ~SXQLock() noexcept {
        unlock();
        allocator_.deallocate(nodeP_, 1);
    }

    SXQLock(const SXQLock&) = delete;
    SXQLock(SXQLock&& rhs) : SXQLock() { swap(rhs); }
    SXQLock& operator=(const SXQLock&) = delete;
    SXQLock& operator=(SXQLock&& rhs) { swap(rhs); return *this; }

    std::string str() const {
        return cybozu::util::formatString("SXQLock mutex:%p mode:%hhu", mutex_, mode_);
    }

    void lock(Mutex* mutex, Mode mode) {
        switch (mode) {
        case Mode::X:
            lockX(mutex);
            break;
        case Mode::S:
            lockS(mutex);
            break;
        default:
            assert(false);
        }
    }
    bool tryLock(Mutex* mutex, Mode mode) {
        switch (mode) {
        case Mode::X:
            return tryLockX(mutex);
        case Mode::S:
            return tryLockS(mutex);
        default:
            assert(false);
        }
        return false;
    }
    void lockX(Mutex* mutex) {
        assert(mutex);
        mutex_ = mutex;
        mode_ = Mode::X;

        nodeP_->init();
        bool locked;
        LockData ld0 = mutex_->atomicLoad();
        LockData ld1;
        for (;;) {
            ld1 = ld0;
            nodeP_->wait = false;

            if (ld0.getTail() == nullptr) {
                if (ld0.nrReaders == 0) {
                    // No one holds the lock.
                    locked = true;
                } else {
                    // (locked R,R,...,R),W <- It's me.
                    ld1.setNext(nodeP_);
                    ld1.isNextWriter = true;
                    locked = false;
                    // A reader may notify me just after CAS.
                    nodeP_->wait = true;
                }
            } else {
                // The previous node will notify me.
                locked = false;
            }
            ld1.isTailWriter = true;
            ld1.setTail(nodeP_);
            if (mutex_->compareAndSwap(ld0, ld1)) {
#ifdef DEBUG_PRINT
                ::printf("lockX-before    %p %s\n", mutex_, ld0.str().c_str());
                ::printf("lockX-after     %p %s\n", mutex_, ld1.str().c_str());
#endif
                break;
            }
        }
        if (locked) {
#ifdef DEBUG_PRINT
            ::printf("lockX locked0 %p\n", mutex_);
#endif
#if 0
            LockData x; x.init(); x.unused1 = 1; x.tail = 1;
            sxq::ld_.push_back(x);
#endif
            return; // lock has been acquired now.
        }
        Node *prev = ld0.getTail();
        if (prev != nullptr) {
            nodeP_->wait = true;
            prev->setNextAndIsWriter(nodeP_, true);
        }
        while (nodeP_->wait) _mm_pause(); // local spin wait
#ifdef DEBUG_PRINT
        ::printf("lockX locked1 %p\n", mutex_);
#endif
#if 0
        LockData x; x.init(); x.unused1 = 1; x.tail = 2;
        sxq::ld_.push_back(x);
#endif
    }
    void lockS(Mutex* mutex) {
        assert(mutex);
        mutex_ = mutex;
        mode_ = Mode::S;

        nodeP_->init();
        bool locked;
        LockData ld0 = mutex_->atomicLoad();
        LockData ld1;
        for (;;) {
            ld1 = ld0;
            if (ld0.isTailWriter) {
                assert(ld0.getTail() != nullptr);
                ld1.waitingPhase++;
                locked = false;
            } else {
                if (ld0.getTail() == nullptr && ld0.isSamePhase()) {
                    if (ld0.nrReaders < sxq::MAX_READERS) {
                        locked = true;
                        ld1.setTail(nullptr);
                        ld1.nrReaders++;
                    } else {
                        // (locked R, R, ..., R), R <- It's me.
                        ld1.setNext(nodeP_);
                        ld1.isNextWriter = false;
                        locked = false;
                        // A reader may notify me just after CAS.
                        nodeP_->wait = true;
                    }
                } else {
                    locked = false;
                }
            }
            ld1.isTailWriter = false;
            if (!locked) {
                ld1.setTail(nodeP_);
            }
            if (mutex_->compareAndSwap(ld0, ld1)) {
#ifdef DEBUG_PRINT
                ::printf("lockS-before %p %s\n", mutex_, ld0.str().c_str());
                ::printf("lockS-after  %p %s\n", mutex_, ld1.str().c_str());
#endif
                break;
            }
        }
        if (locked) {
            // do nothing
#ifdef DEBUG_PRINT
            ::printf("lockS locked0 %p\n", mutex_);
#endif
#if 0
            LockData x; x.init(); x.unused1 = 1; x.tail = 3;
            sxq::ld_.push_back(x);
#endif
            return;
        }
        Node *prev = ld0.getTail();
        if (prev != nullptr) {
            nodeP_->wait = true;
            prev->setNextAndIsWriter(nodeP_, false);
        }
        while (nodeP_->wait) _mm_pause(); // local spin wait
        // lock acquired.
        const bool shouldUpdateLockingPhase = ld0.isTailWriter;
        LockData ld2 = mutex_->atomicLoad();
        LockData ld3;
        for (;;) {
            ld3 = ld2;
            ld3.nrReaders++;
            if (shouldUpdateLockingPhase) {
                ld3.lockingPhase++;
            }
            if (ld2.getTail() == nodeP_) {
                ld3.setTail(nullptr);
            }
            if (mutex_->compareAndSwap(ld2, ld3)) {
                break;
            }
        }
#ifdef DEBUG_PRINT
        ::printf("lockS locked1 %p\n", mutex_);
#endif
#if 0
        LockData x; x.init(); x.unused1 = 1; x.tail = 4;
        sxq::ld_.push_back(x);
#endif
        if (ld2.getTail() != nodeP_) {
            // Next node must exist (or will appear soon).
            notifyNextReaderOrSetNext(ld3);
        }
    }
    bool tryLockX(Mutex* mutex) {
        assert(mutex);
        mutex_ = mutex;
        mode_ = Mode::X;

        nodeP_->init();
        LockData ld0 = mutex_->atomicLoad();
        for (;;) {
            LockData ld1 = ld0;
            if (ld0.getTail() != nullptr || ld0.nrReaders > 0) {
                init();
                return false;
            }
            ld1.isTailWriter = true;
            ld1.setTail(nodeP_);
            if (mutex_->compareAndSwap(ld0, ld1)) {
#ifdef DEBUG_PRINT
                ::printf("tryLockX-before %s\n", ld0.str().c_str());
                ::printf("tryLockX-after  %s\n", ld1.str().c_str());
#endif
                break;
            }
        }
        return true;
    }
    bool tryLockS(Mutex* mutex) {
        assert(mutex);
        mutex_ = mutex;
        mode_ = Mode::S;

        nodeP_->init();
        bool locked;
        LockData ld0 = mutex_->atomicLoad();
        LockData ld1;
        for (;;) {
            ld1 = ld0;
            if (ld0.isTailWriter) {
                assert(ld0.getTail() != nullptr);
                init();
                return false;
            }
            if (!ld0.isSamePhase()) {
                init();
                return false;
            }
            if (ld0.getTail() == nullptr) {
                if (ld0.nrReaders >= sxq::MAX_READERS) {
                    init();
                    return false;
                }
                locked = true;
                ld1.setTail(nullptr);
                ld1.nrReaders++;
            } else {
                // Lock will get soon.
                locked = false;
            }
            ld1.isTailWriter = false;
            if (!locked) {
                ld1.setTail(nodeP_);
            }
            if (mutex_->compareAndSwap(ld0, ld1)) {
#ifdef DEBUG_PRINT
                ::printf("tryLockS-before %s\n", ld0.str().c_str());
                ::printf("tryLockS-after  %s\n", ld1.str().c_str());
#endif
                break;
            }
        }
        if (locked) {
            // do nothing anymore.
            return true;
        }
        Node *prev = ld0.getTail();
        assert(prev != nullptr);
        nodeP_->wait = true;
        prev->setNextAndIsWriter(nodeP_, false);
        while (nodeP_->wait) _mm_pause(); // local spin wait
        // lock acquired.
        assert(!ld0.isTailWriter);
        LockData ld2 = mutex_->atomicLoad();
        LockData ld3;
        for (;;) {
            ld3 = ld2;
            ld3.nrReaders++;
            if (ld2.getTail() == nodeP_) {
                ld3.setTail(nullptr);
            }
            if (mutex_->compareAndSwap(ld2, ld3)) {
                break;
            }
        }
        if (ld2.getTail() != nodeP_) {
            // Next node must exist (or will appear soon).
            notifyNextReaderOrSetNext(ld3);
        }
        return true;
    }
    bool tryUpgrade() {
        assert(mutex_);
        assert(mode_ == Mode::S);

        nodeP_->init(); // I'm reader so nodeP_ can be reused.
        LockData ld0 = mutex_->atomicLoad();
        for (;;) {
            LockData ld1 = ld0;
            assert(ld0.nrReaders > 0);
            if (ld0.getTail() != nullptr || ld0.nrReaders != 1) {
                return false;
            }
            ld1.nrReaders--;
            ld1.isTailWriter = true;
            ld1.setTail(nodeP_);
            if (mutex_->compareAndSwap(ld0, ld1)) {
#ifdef DEBUG_PRINT
                ::printf("tryUpgrade-before %s\n", ld0.str().c_str());
                ::printf("tryUpgrade-after  %s\n", ld1.str().c_str());
#endif
                break;
            }
        }
        mode_ = Mode::X;
        return true;
    }
    bool upgrade() {
        assert(mutex_);
        assert(mode_ == Mode::S);

        nodeP_->init(); // I'm reader so nodeP_ can be reused.
        bool locked;
        LockData ld0 = mutex_->atomicLoad();
        for (;;) {
            LockData ld1 = ld0;
            nodeP_->wait = false;

            if (ld0.getTail() == nullptr) {
                if (ld0.nrReaders == 1) {
                    // No one except me holds the lock.
                    locked = true;
                } else {
                    assert(ld0.nrReaders > 0);
                    // (locked R,R,...,R),W <- It's me.
                    ld1.setNext(nodeP_);
                    ld1.isNextWriter = true;
                    locked = false;
                    // A reader may notify me just after CAS.
                    nodeP_->wait = true;
                }
            } else {
                if (ld0.isTailWriter || !ld0.isSamePhase()) {
                    // Another writer exists.
                    return false;
                }
                // The previous node will notify me.
                locked = false;
            }
            ld1.nrReaders--;
            ld1.isTailWriter = true;
            ld1.setTail(nodeP_);
            if (mutex_->compareAndSwap(ld0, ld1)) {
#ifdef DEBUG_PRINT
                ::printf("upgrade-before %s\n", ld0.str().c_str());
                ::printf("upgrade-after  %s\n", ld1.str().c_str());
#endif
                break;
            }
        }
        // The upgrade will success.
        mode_ = Mode::X;
        if (locked) {
            return true;
        }
        Node *prev = ld0.getTail();
        if (prev != nullptr) {
            nodeP_->wait = true;
            prev->setNextAndIsWriter(nodeP_, true);
        }
        while (nodeP_->wait) _mm_pause(); // local spin wait
        return true;
    }
    void unlock() noexcept {
        switch (mode_) {
        case Mode::X:
            unlockX();
            break;
        case Mode::S:
            unlockS();
            break;
        case Mode::Invalid:
            ; // do nothing
        }
        init();
    }
    void unlockX() noexcept {
        assert(mutex_);
        LockData ld0 = mutex_->atomicLoad();
        while (ld0.getTail() == nodeP_) {
             LockData ld1 = ld0;
             ld1.setTail(nullptr);
             ld1.isTailWriter = false;
             if (mutex_->compareAndSwap(ld0, ld1)) {
#ifdef DEBUG_PRINT
                 ::printf("unlockX0-before %p %s\n", mutex_, ld0.str().c_str());
                 ::printf("unlockX0-after  %p %s\n", mutex_, ld1.str().c_str());
#endif
#if 0
                 LockData x; x.init(); x.unused1 = 1; x.tail = 5;
                 sxq::ld_.push_back(x);
#endif
                 return;
             }
        }
        sxq::NodePtrAndIsWriter npaiw = waitForNextPtrIsSet();
#ifdef DEBUG_PRINT
        ::printf("unlockX1 %p\n", mutex_);
#endif
        npaiw.getNode()->wait = false; // notify
#if 0
        LockData x; x.init(); x.unused1 = 1; x.tail = 6;
        sxq::ld_.push_back(x);
#endif
    }
    void unlockS() noexcept {
        assert(mutex_);
        LockData ld0 = mutex_->atomicLoad();
        Node *next = nullptr;
        for (;;) {
            LockData ld1 = ld0;
            ld1.nrReaders--;
            const bool shouldNotifyNextReader = !ld0.isNextWriter && ld1.nrReaders < sxq::MAX_READERS;
            const bool shouldNotifyNextWriter = ld0.isNextWriter && ld1.nrReaders == 0;
            if (shouldNotifyNextReader || shouldNotifyNextWriter) {
                next = ld0.getNext();
                if (next != nullptr) {
                    ld1.setNext(nullptr);
                    ld1.isNextWriter = false;
                }
            }
            if (mutex_->compareAndSwap(ld0, ld1)) {
#ifdef DEBUG_PRINT
                ::printf("unlockS-before %p %s\n", mutex_, ld0.str().c_str());
                ::printf("unlockS-after  %p %s\n", mutex_, ld1.str().c_str());
#endif
                break;
            }
        }
#if 0
        LockData x; x.init(); x.unused1 = 1; x.tail = 7;
        sxq::ld_.push_back(x);
#endif
        if (next != nullptr) {
            next->wait = false; // notify
        }
    }

    bool isShared() const { return mode_ == Mode::S; }
    const Mutex* mutex() const { return mutex_; }
    Mutex* mutex() { return mutex_; }
    uintptr_t getMutexId() const { return uintptr_t(mutex_); }
    Mode mode() const { return mode_; }

    // This is used for dummy object to comparison.
    void setMutex(Mutex *mutex) {
        mutex_ = mutex;
        mode_ = Mode::Invalid;
    }

private:
    void init() {
        mutex_ = nullptr;
        mode_ = Mode::Invalid;
    }
    void swap(SXQLock& rhs) {
        std::swap(mutex_, rhs.mutex_);
        std::swap(mode_, rhs.mode_);
        std::swap(nodeP_, rhs.nodeP_);
    }
    void notifyNextReaderOrSetNext(LockData ld0) {
        sxq::NodePtrAndIsWriter npaiw = waitForNextPtrIsSet();
        while (npaiw.isWriter() || ld0.nrReaders >= sxq::MAX_READERS) {
            LockData ld1 = ld0;
            ld1.setNext(npaiw.getNode());
            ld1.isNextWriter = npaiw.isWriter();
            if (mutex_->compareAndSwap(ld0, ld1)) {
                return;
            }
        }
        npaiw.getNode()->wait = false; // notify.
    }
    sxq::NodePtrAndIsWriter waitForNextPtrIsSet() {
        sxq::NodePtrAndIsWriter npaiw = nodeP_->loadNextAndIsWriter();
        while (npaiw.getNode() == nullptr) {
            _mm_pause();
            npaiw = nodeP_->loadNextAndIsWriter();
        }
        return npaiw;
    }
};

}} // namespace cybozu::lock
