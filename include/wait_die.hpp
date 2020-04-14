#pragma once
/**
 * 2PL wait and die for deadlock prevension.
 */
#include <vector>
#include "lock_data.hpp"
#include "arch.hpp"
#include "vector_payload.hpp"
#include "write_set.hpp"
#include "atomic_wrapper.hpp"
#include "inline.hpp"


namespace cybozu {
namespace wait_die {

using TxId = uint32_t;
const TxId MAX_TXID = TxId(-1);


/**
 * Constants.
 */
constexpr size_t Cumulo_readers_bits = 7;
constexpr size_t Max_cumulo_readers = (1 << Cumulo_readers_bits) - 1;
constexpr size_t Readers_bits = 7;
constexpr uint32_t Max_readers = (1 << Readers_bits) - 1;


/**
 * Wait-die deadlock avoidance revision 2.
 *
 * This variant uses 64bit mutex object only.
 * A merit of the design is it does not require any queues.
 * A demerit of the design is additional possibility of abort(die).
 *
 * Smaller values of Threshold_cumulo_readers are expected to reduce
 * lock waiting time of transactions with higher priority.
 * We have not found the efficiency of Threshold_cumulo_readers yet.
 */
template <size_t Threshold_cumulo_readers>
struct WaitDieData2
{
    static_assert(Threshold_cumulo_readers <= Max_cumulo_readers);
    using Mode = cybozu::lock::LockStateXS::Mode;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
    alignas(sizeof(uint64_t))
    union {
        uint64_t obj;
        struct {
            // tx_id that holds X lock or mininum tx_id (that means prior tx) that holds S lock.
            uint32_t tx_id;
            union {
                uint32_t mutex_obj;
                struct {
                    uint32_t write_locked:1;
                    uint32_t readers:Readers_bits;
                    uint32_t reserved0:1;
                    uint32_t cumulo_readers:Cumulo_readers_bits;
                    uint32_t reserved1:16;
                };
            };
        };
    };
#pragma GCC diagnostic pop

    INLINE WaitDieData2() { init(); }
    INLINE void init() {
        tx_id = MAX_TXID;
        mutex_obj = 0;
    }
    INLINE WaitDieData2(uint64_t obj0) { obj = obj0; }
    INLINE operator uint64_t() const { return obj; }

    std::string str() const {
        return cybozu::util::formatString(
            "tx_id %u X %u S %u (cumulative %u)"
            , tx_id, write_locked, readers, cumulo_readers);
    }

    bool is_unlocked() const {
        return tx_id == MAX_TXID && write_locked == 0
            && readers == 0 && cumulo_readers == 0;
    }
};

static_assert(sizeof(WaitDieData2<1>) == sizeof(uint64_t));


template <size_t Threshold_cumulo_readers>
struct WaitDieLock2
{
    using Data = WaitDieData2<Threshold_cumulo_readers>;
    using Mode = typename Data::Mode;
    struct Mutex : Data {

        using WaitDieData2<Threshold_cumulo_readers>::WaitDieData2;

        INLINE Mutex load() const {
            return load_acquire(this->obj);
        }
        INLINE void store(Mutex mu0) {
            store_release(this->obj, mu0.obj);
        }
        INLINE bool cas(Mutex& mu0, Mutex mu1) {
            return compare_exchange(this->obj, mu0.obj, mu1.obj);
        }
        INLINE bool cas_acquire(Mutex& mu0, Mutex mu1) {
            return compare_exchange_acquire(this->obj, mu0.obj, mu1.obj);
        }
        INLINE bool cas_release(Mutex& mu0, Mutex mu1) {
            return compare_exchange_release(this->obj, mu0.obj, mu1.obj);
        }
    };

private:
    Mutex *mutexp_;
    Mode mode_;
    TxId tx_id_;

public:
    INLINE WaitDieLock2() noexcept : mutexp_(nullptr), mode_(Mode::INVALID), tx_id_(MAX_TXID) {}
    INLINE ~WaitDieLock2() noexcept { unlock(); }
    INLINE WaitDieLock2(const WaitDieLock2& ) = delete;
    INLINE WaitDieLock2(WaitDieLock2&& rhs) noexcept : WaitDieLock2() { swap(rhs); }
    INLINE WaitDieLock2& operator=(const WaitDieLock2& rhs) = delete;
    INLINE WaitDieLock2& operator=(WaitDieLock2&& rhs) noexcept { swap(rhs); return *this; }

    /**
     * This is for blind-write.
     */
    INLINE void setMutex(Mutex& mutex) noexcept { mutexp_ = &mutex; }

    /**
     * If true, locked.
     * If false, you must abort your running transaction.
     */
    INLINE bool readLock(Mutex& mutex, TxId tx_id) noexcept {
        assert(tx_id != MAX_TXID);
        Mutex mu0 = mutex.load();
        for (;;) {
            _mm_pause();
            if (mu0.write_locked > 0) {
                if (mu0.tx_id < tx_id) return false; // die
                mu0 = mutex.load();
                continue; // wait
            }
            if (mu0.tx_id < tx_id && mu0.cumulo_readers >= Threshold_cumulo_readers) {
                return false; // die
            }
            if (mu0.readers >= Max_readers) {
                mu0 = mutex.load();
                continue; // wait
            }
            // try to lock
            Mutex mu1 = mu0;
            mu1.readers++;
            mu1.cumulo_readers++;
            mu1.tx_id = std::min(mu1.tx_id, tx_id);
            if (mutex.cas_acquire(mu0, mu1)) {
                mutexp_ = &mutex;
                mode_ = Mode::S;
                tx_id_ = tx_id;
                return true;
            }
            // retry
        }
    }
    INLINE bool writeLock(Mutex& mutex, TxId tx_id) noexcept {
        assert(tx_id != MAX_TXID);
        Mutex mu0 = mutex.load();
        for (;;) {
            _mm_pause();
            if (mu0.write_locked || mu0.readers != 0) {
                if (mu0.tx_id < tx_id) return false; // die
                mu0 = mutex.load();
                continue; // wait
            }
            // try to lock
            Mutex mu1 = mu0;
            mu1.write_locked = 1;
            mu1.tx_id = tx_id;
            if (mutex.cas_acquire(mu0, mu1)) {
                mutexp_ = &mutex;
                mode_ = Mode::X;
                tx_id_ = tx_id;
                return true;
            }
            // retry
        }
    }
    INLINE void unlock() noexcept {
        // mutexp_ may not be nullptr due to setMutex().
        // so we do not use nullcheck of mutexp_ to switch case.
        switch (mode_) {
        case Mode::INVALID: return;
        case Mode::S: readUnlock(); return;
        case Mode::X: writeUnlock(); return;
        default: assert(false);
        }
    }
    INLINE void readUnlock() noexcept {
        assert_locked(Mode::S);
        Mutex& mutex = *mutexp_;
        Mutex mu0 = mutex.load();
        for (;;) {
            _mm_pause();
            Mutex mu1 = mu0;
            assert(mu1.readers > 0);
            mu1.readers--;
            if (mu1.readers == 0) {
                mu1.cumulo_readers = 0;
                mu1.tx_id = MAX_TXID;
            }
            if (mutex.cas_release(mu0, mu1)) {
                init();
                return;
            }
            // retry
        }
    }
    INLINE void writeUnlock() noexcept {
        assert_locked(Mode::X);
        Mutex& mutex = *mutexp_;
#ifndef NDEBUG
        Mutex mu0 = mutex.load();
        assert(mu0.write_locked == 1);
        assert(mu0.readers == 0);
        assert(mu0.cumulo_readers == 0);
        assert(mu0.tx_id == tx_id_);
#endif
        Mutex mu1;
        assert(mu1.is_unlocked());
        mutex.store(mu1);
        init();
    }
    INLINE bool upgrade() noexcept {
        assert_locked(Mode::S);
        Mutex& mutex = *mutexp_;
        Mutex mu0 = mutex.load();
        while (mu0.readers == 1) {
            assert(mu0.tx_id <= tx_id_);
            _mm_pause();
            Mutex mu1 = mu0;
            mu1.write_locked = 1;
            mu1.readers = 0;
            mu1.cumulo_readers = 0;
            mu1.tx_id = tx_id_;
            if (mutex.cas(mu0, mu1)) {
                mode_ = Mode::X;
                return true;
            }
            // retry
        }
        return false; // failed.
    }
    INLINE Mode mode() const noexcept {
        return mode_;
    }
    INLINE uintptr_t getMutexId() const noexcept {
        return uintptr_t(mutexp_);
    }
private:
    INLINE void init() noexcept {
        mutexp_ = nullptr;
        mode_ = Mode::INVALID;
        tx_id_ = MAX_TXID;
    }
    INLINE void swap(WaitDieLock2& rhs) noexcept {
        std::swap(mutexp_, rhs.mutexp_);
        std::swap(mode_, rhs.mode_);
        std::swap(tx_id_, rhs.tx_id_);
    }
    INLINE void assert_locked(Mode mode) const noexcept {
        unused(mode);
        assert(mode_ == mode);
        assert(mutexp_ != nullptr);
        assert(tx_id_ != MAX_TXID);
    }
};


/**
 * Simpler version with additional store for txids of readers.
 */
struct WaitDieData3
{
    using Mode = cybozu::lock::LockStateXS::Mode;

    /**
     * Max number of readers is limited to Txids_size.
     */
    static constexpr size_t Txids_size = CACHE_LINE_SIZE / sizeof(TxId);
    static_assert(Txids_size <= Max_readers);

    struct Header {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
        alignas(sizeof(uint64_t))
        union {
            uint64_t obj;
            struct {
                TxId tx_id;
                // latch is used to access locked_txids temporary.
                uint32_t latch:1;
                uint32_t readers:Readers_bits;
                uint32_t reserved0:(32 - Readers_bits - 1);
            };
        };
#pragma GCC diagnostic pop

        INLINE Header() noexcept {
            obj = 0;
            tx_id = MAX_TXID;
        }

        INLINE Header(uint64_t obj0) noexcept : obj(obj0) {
        }
        INLINE operator uint64_t() const noexcept { return obj; }

        /**
         * is_write_locked || is_read_locked --> is_locked
         * is_read_locked_full --> is_read_locked
         */
        INLINE bool is_locked() const noexcept { return tx_id != MAX_TXID; }
        INLINE bool is_write_locked() const noexcept { return is_locked() && readers == 0; }
        INLINE bool is_read_locked() const noexcept { return readers > 0; }
        INLINE bool is_read_locked_full() const noexcept { return readers >= Txids_size; }
    };
    static_assert(sizeof(Header) == sizeof(uint64_t));

    Header header; // this must be accessed atomically.

    /**
     * Transactions that have locks.
     * This is preallocated to eliminate benchmark overhead.
     * The size of the vector is Txids_size.
     * You need to have the latch to access this vector.
     * The element value MAX_TXID means empty.
     */
    std::vector<TxId> txids;

    INLINE WaitDieData3() : header(), txids(Txids_size, MAX_TXID) {
    }

    /** Wrappers of each atomic operation. */
    INLINE Header load() const noexcept { return load_acquire(header.obj); }
    INLINE void store(Header h0) noexcept { store_release(header.obj, h0.obj); }
    INLINE bool cas(Header& h0, Header h1) noexcept {
        return compare_exchange(header.obj, h0.obj, h1.obj);
    }

    /*
     * add_tx_id(), remove_tx_id(), and get_min_tx_id() access the txids,
     * so you need to get latch before calling them.
     */

    /**
     * return vector index.
     */
    INLINE size_t add_tx_id(TxId tx_id) noexcept {
        for (size_t i = 0; i < Txids_size; i++) {
            if (txids[i] == MAX_TXID) {
                txids[i] = tx_id;
                return i;
            }
        }
        // If the following code is executed, it is BUG.
        assert(false);
        return SIZE_MAX;
    }
    INLINE void remove_tx_id(size_t idx) noexcept {
        assert(idx < Txids_size);
        txids[idx] = MAX_TXID;
    }
    INLINE TxId get_min_tx_id() const noexcept {
        TxId min_tx_id = MAX_TXID;
        for (size_t i = 0; i < Txids_size; i++) {
            min_tx_id = std::min(min_tx_id, txids[i]);
        }
        return min_tx_id;
    }
};


struct WaitDieLock3
{
    using Mode = WaitDieData3::Mode;
    using Mutex = WaitDieData3;
    using Header = WaitDieData3::Header;

private:
    Mutex *mutexp_;
    Mode mode_;
    TxId tx_id_;
    size_t idx_in_txids_; // for readers.

public:
    INLINE WaitDieLock3() noexcept
        : mutexp_(nullptr), mode_(Mode::INVALID), tx_id_(MAX_TXID), idx_in_txids_(SIZE_MAX) {
    }
    INLINE ~WaitDieLock3() noexcept { unlock(); }
    INLINE WaitDieLock3(const WaitDieLock3& ) = delete;
    INLINE WaitDieLock3(WaitDieLock3&& rhs) noexcept : WaitDieLock3() { swap(rhs); }
    INLINE WaitDieLock3& operator=(const WaitDieLock3& rhs) = delete;
    INLINE WaitDieLock3& operator=(WaitDieLock3&& rhs) noexcept { swap(rhs); return *this; }

    /**
     * This is for blind-write.
     */
    INLINE void setMutex(Mutex& mutex) noexcept { mutexp_ = &mutex; }

    /**
     * If true, locked.
     * If false, you must abort your running transaction.
     */
    INLINE bool readLock(Mutex& mutex, TxId tx_id) noexcept {
        assert(tx_id != MAX_TXID);
        Header h0 = mutex.load();
        for (;;) {
            _mm_pause();
            /*
             * State --> What to do.
             * (1) unlocked and unlatched.
             *     --> no need to wait.
             * (2) unlocked and latched.
             *     --> can wait unconditionally.
             * (3) write locked (latched).
             *     --> can wait if prior (3a) or die (3b).
             * (4) read locked and latched.
             *     --> can wait unconditionally.
             * (5) read locked and unlatched.
             *     --> no need to wait if not full (5a), or
             *         can wait if prior (5b), or die (5c).
             */
            const bool is_prior = (tx_id < h0.tx_id);
            if ((h0.is_write_locked() || (!h0.latch && h0.is_read_locked_full())) && !is_prior) {
                return false; // (3b)(5c)
            }
            if (h0.latch || h0.is_read_locked_full()) {
                // (2)(3)(4)(5b)(5c) - (3b)(5c) = (2)(3a)(4)(5b)
                h0 = mutex.load();
                continue;
            }
            // (1)(5a)
            Header h1 = h0; h1.latch = 1;
            if (!mutex.cas(h0, h1)) continue;
            const size_t idx = mutex.add_tx_id(tx_id);
            h1.readers++;
            h1.tx_id = std::min(h1.tx_id, tx_id);
            h1.latch = 0;
            mutex.store(h1);
            // lock has done.
            mutexp_ = &mutex;
            mode_ = Mode::S;
            tx_id_ = tx_id;
            idx_in_txids_ = idx;
            return true;
        }
        assert(false);
    }
    INLINE bool writeLock(Mutex& mutex, TxId tx_id) noexcept {
        assert(tx_id != MAX_TXID);
        Header h0 = mutex.load();
        for (;;) {
            _mm_pause();
            /*
             * State --> What to do.
             * (1) unlocked and unlatched.
             *     --> no need to wait.
             * (2) unlocked and latched.
             *     --> can wait unconditionally.
             * (3) write locked (latched).
             *     --> can wait if prior (3a) or die (3b).
             * (4) read locked and latched.
             *     --> can wait unconditionally.
             * (5) read locked and unlatched.
             *     --> can wait if prior (5a), or die (5b).
             */
            const bool is_prior = (tx_id < h0.tx_id);
            if ((h0.is_write_locked() || (!h0.latch && h0.is_read_locked())) && !is_prior) {
                return false; // (3b)(5b)
            }
            if (h0.latch || h0.is_locked()) {
                // !(1) - (3b)(5b) = (2)(3a)(4)(5a)
                h0 = mutex.load();
                continue;
            }
            // (1)
            Header h1 = h0;
            h1.latch = 1;
            h1.tx_id = tx_id;
            if (!mutex.cas(h0, h1)) continue;
            // lock has done.
            mutexp_ = &mutex;
            mode_ = Mode::X;
            tx_id_ = tx_id;
            return true;
        }
        assert(false);
    }
    INLINE void unlock() noexcept {
        // mutexp_ may not be nullptr due to setMutex().
        // so we do not use nullcheck of mutexp_ to switch case.
        switch (mode_) {
        case Mode::INVALID: return;
        case Mode::S: readUnlock(); return;
        case Mode::X: writeUnlock(); return;
        default: assert(false);
        }
    }
    INLINE void readUnlock() noexcept {
        assert_locked(Mode::S);
        Mutex& mutex = *mutexp_;
        Header h0 = mutex.load();
        for (;;) {
            _mm_pause();
            if (h0.latch) {
                h0 = mutex.load();
                continue;
            }
            Header h1 = h0;
            h1.latch = 1;
            if (!mutex.cas(h0, h1)) continue;
            mutex.remove_tx_id(idx_in_txids_);
            h1.readers--;
            h1.latch = 0;
            if (h1.readers == 0) {
                h1.tx_id = MAX_TXID;
            } else if (h1.tx_id == tx_id_) {
                h1.tx_id = mutex.get_min_tx_id();
            }
            mutex.store(h1);
            init();
            return;
        }
        assert(false);
    }
    INLINE void writeUnlock() noexcept {
        assert_locked(Mode::X);
        Mutex& mutex = *mutexp_;
#ifndef NDEBUG
        Header h0 = mutex.load();
        assert(h0.latch == 1);
        assert(h0.readers == 0);
        assert(h0.tx_id == tx_id_);
#endif
        Header h1;
        assert(!h1.is_locked());
        mutex.store(h1);
        init();
    }
    INLINE bool upgrade() noexcept {
        assert_locked(Mode::S);
        Mutex& mutex = *mutexp_;
        Header h0 = mutex.load();
        while (h0.readers == 1) {
            assert(h0.tx_id == tx_id_);
            _mm_pause();
            if (h0.latch) {
                h0 = mutex.load();
                continue;
            }
            Header h1 = h0;
            h1.latch = 1;
            if (!mutex.cas(h0, h1)) continue;
            mutex.remove_tx_id(idx_in_txids_);
            h1.readers = 0;
            // h1.tx_id = tx_id_;
            mutex.store(h1); // upgrade has done.
            mode_ = Mode::X;
            idx_in_txids_ = SIZE_MAX;
            return true;
        }
        return false; // failed.
    }
    INLINE Mode mode() const noexcept {
        return mode_;
    }
    INLINE uintptr_t getMutexId() const noexcept {
        return uintptr_t(mutexp_);
    }
private:
    INLINE void init() noexcept {
        mutexp_ = nullptr;
        mode_ = Mode::INVALID;
        tx_id_ = MAX_TXID;
        idx_in_txids_ = SIZE_MAX;
    }
    INLINE void swap(WaitDieLock3& rhs) noexcept {
        std::swap(mutexp_, rhs.mutexp_);
        std::swap(mode_, rhs.mode_);
        std::swap(tx_id_, rhs.tx_id_);
        std::swap(idx_in_txids_, rhs.idx_in_txids_);
    }
    INLINE void assert_locked(Mode mode) const noexcept {
        unused(mode);
#ifndef NDEBUG
        assert(mode_ == mode);
        assert(mutexp_ != nullptr);
        assert(tx_id_ != MAX_TXID);
#endif
    }
};


class LockSet
{
public:
#if 1
    using Lock = WaitDieLock2<Max_cumulo_readers>;
#else
    using Lock = WaitDieLock3;
#endif
    using Mode = Lock::Mode;
    using Mutex = Lock::Mutex;
private:
    using OpEntryL = OpEntry<Lock>;
    using Vec = std::vector<OpEntryL>;

    // key: mutex addr, value: index in the vector.
#if 1
    using Index = SingleThreadUnorderedMap<uintptr_t, size_t>;
#else
    using Index = std::unordered_map<uintptr_t, size_t>;
#endif

    Vec vec_;
    Index index_;

    TxId txId_;

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
    // Call this at first once.
    void init(size_t valueSize, size_t nrReserve) {
        valueSize_ = valueSize;
        if (valueSize == 0) valueSize++;
        local_.setSizes(valueSize);

        vec_.reserve(nrReserve);
        local_.reserve(nrReserve);
        bwV_.reserve(nrReserve); // This may be too conservative and memory eater.
    }
    /* call this before read/write just after a transaction trial starts. */
    void setTxId(TxId txId) { txId_ = txId; }

    INLINE bool read(Mutex& mutex, void *sharedVal, void *dst) {
        Vec::iterator it = find(uintptr_t(&mutex));
        if (it != vec_.end()) {
            Lock& lk = it->lock;
            if (lk.mode() == Mode::S) {
                copyValue(dst, sharedVal); // read shared data.
                return true;
            }
            assert(lk.mode() == Mode::X || lk.mode() == Mode::INVALID);
            copyValue(dst, getLocalValPtr(it->info)); // read local data.
            return true;
        }
        // Try to read lock.
        vec_.emplace_back();
        OpEntryL &ope = vec_.back();
        Lock& lk = ope.lock;
        if (!lk.readLock(mutex, txId_)) {
            // should die.
            return false;
        }
        copyValue(dst, sharedVal); // read shared data.
        return true;
    }
    INLINE bool write(Mutex& mutex, void *sharedVal, void *src) {
        Vec::iterator it = find(uintptr_t(&mutex));
        if (it != vec_.end()) {
            Lock& lk = it->lock;
            if (lk.mode() == Mode::S) {
                if (!lk.upgrade()) return false;
                it->info.set(allocateLocalVal(), sharedVal);
            }
            assert(lk.mode() == Mode::X || lk.mode() == Mode::INVALID);
            copyValue(getLocalValPtr(it->info), src); // write local data.
            return true;
        }
        // This is blind write.
        vec_.emplace_back();
        OpEntryL& ope = vec_.back();
        // Lock will be tried later. See blindWriteLockAll().
        ope.lock.setMutex(mutex); // for search.
        bwV_.emplace_back(&mutex, vec_.size() - 1);
        ope.info.set(allocateLocalVal(), sharedVal);
        copyValue(getLocalValPtr(ope.info), src); // write local data.
        return true;
    }
    INLINE bool readForUpdate(Mutex& mutex, void *sharedVal, void *dst) {
        Vec::iterator it = find(uintptr_t(&mutex));
        if (it != vec_.end()) {
            // Found.
            Lock& lk = it->lock;
            LocalValInfo& info = it->info;
            if (lk.mode() == Mode::X) {
                copyValue(dst, getLocalValPtr(info)); // read local data.
                return true;
            }
            if (lk.mode() == Mode::S) {
                if (!lk.upgrade()) return false;
                info.set(allocateLocalVal(), sharedVal);
                void* localVal = getLocalValPtr(info);
                copyValue(localVal, sharedVal); // for next read.
                copyValue(dst, localVal); // read local data.
                return true;
            }
            assert(lk.mode() == Mode::INVALID);
            copyValue(dst, getLocalValPtr(info)); // read local data.
            return true;
        }
        // Not found. Try to write lock.
        vec_.emplace_back();
        OpEntryL &ope = vec_.back();
        Lock& lk = ope.lock;
        LocalValInfo& info = ope.info;
        if (!lk.writeLock(mutex, txId_)) {
            // should die.
            return false;
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
            assert(ope.lock.mode() == Mode::INVALID);
            if (!ope.lock.writeLock(*bwInfo.mutex, txId_)) {
                // should die.
                return false;
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
            } else {
                assert(lk.mode() == Mode::S);
            }
#if 1 // unlock one by one.
            ope.lock.unlock();
#endif
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


}} // namespace cybozu::wait_die
