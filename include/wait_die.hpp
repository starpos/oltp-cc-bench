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
#include "list_util.hpp"
#include "mcslikelock.hpp"

/*
 * Currently three variants of wait-die are avaialble.
 *
 * WaitDieLock2:
 *   One-word (64bit) mutex object only.
 *   This is based on the naive reader-writer locking protocol,
 *   which does not provide fairness.
 *
 * WaitDieLock3:
 *   One word mutex object and txid list (on an cache line).
 *   This does not also provide fairness like WaitDieLock2.
 *
 * WaitDieLock4:
 *   Fair locking version with MCS-like lock template.
 *   All operations must stand in the request queue with atomic exchange.
 *   Mutex object is a bit large (32-48bytes) to have several pointers.
 *   Additional heap allocation is not required at all (requests can use stack).
 */

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
 *
 * This is not fair locking protocol too.
 */
template <size_t Threshold_cumulo_readers>
struct WaitDieData2
{
    static_assert(Threshold_cumulo_readers <= Max_cumulo_readers);

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

    INLINE WaitDieData2() {
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

    INLINE bool is_unlocked() const {
        return tx_id == MAX_TXID && write_locked == 0
            && readers == 0 && cumulo_readers == 0;
    }

    INLINE WaitDieData2 load() const {
        return load_acquire(this->obj);
    }
    INLINE void store(WaitDieData2 d0) {
        store_release(obj, d0.obj);
    }
    INLINE bool cas(WaitDieData2& d0, WaitDieData2 d1) {
        return compare_exchange(obj, d0.obj, d1.obj);
    }
    INLINE bool cas_acq(WaitDieData2& d0, WaitDieData2 d1) {
        return compare_exchange_acquire(obj, d0.obj, d1.obj);
    }
    INLINE bool cas_rel(WaitDieData2& d0, WaitDieData2 d1) {
        return compare_exchange_release(obj, d0.obj, d1.obj);
    }
};


static_assert(sizeof(WaitDieData2<1>) == sizeof(uint64_t));


template <size_t Threshold_cumulo_readers>
struct WaitDieLock2T
{
    using Mode = cybozu::lock::LockStateXS::Mode;
    using Mutex = WaitDieData2<Threshold_cumulo_readers>;

private:
    Mutex *mutexp_;
    Mode mode_;
    TxId tx_id_;

public:
    INLINE WaitDieLock2T() noexcept : mutexp_(nullptr), mode_(Mode::INVALID), tx_id_(MAX_TXID) {}
    INLINE ~WaitDieLock2T() noexcept { unlock(); }
    WaitDieLock2T(const WaitDieLock2T& ) = delete;
    WaitDieLock2T& operator=(const WaitDieLock2T& rhs) = delete;
    INLINE WaitDieLock2T(WaitDieLock2T&& rhs) noexcept : WaitDieLock2T() { swap(rhs); }
    INLINE WaitDieLock2T& operator=(WaitDieLock2T&& rhs) noexcept { swap(rhs); return *this; }

    /**
     * This is for blind-write.
     */
    INLINE void setMutex(Mutex& mutex) noexcept { mutexp_ = &mutex; }

    INLINE void set(Mutex& mutex, Mode mode, TxId tx_id) {
        mutexp_ = &mutex;
        mode_ = mode;
        tx_id_ = tx_id;
    }

    /**
     * If true, locked.
     * If false, you must abort your running transaction.
     */
    INLINE bool readLock(Mutex& mutex, TxId tx_id) noexcept {
        assert(tx_id != MAX_TXID);
        Mutex mu0 = mutex.load();
        for (;;) {
            _mm_pause();
            if (unlikely(mu0.write_locked > 0)) {
                if (unlikely(mu0.tx_id < tx_id)) return false; // die
                mu0 = mutex.load();
                continue; // wait
            }
            if (unlikely(mu0.tx_id < tx_id && mu0.cumulo_readers >= Threshold_cumulo_readers)) {
                return false; // die
            }
            if (unlikely(mu0.readers >= Max_readers)) {
                mu0 = mutex.load();
                continue; // wait
            }
            // try to lock
            Mutex mu1 = mu0;
            mu1.readers++;
            mu1.cumulo_readers++;
            mu1.tx_id = std::min(mu1.tx_id, tx_id);
            if (likely(mutex.cas_acq(mu0, mu1))) {
                set(mutex, Mode::S, tx_id);
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
            if (unlikely(mu0.write_locked || mu0.readers != 0)) {
                if (unlikely(mu0.tx_id < tx_id)) return false; // die
                mu0 = mutex.load();
                continue; // wait
            }
            // try to lock
            Mutex mu1 = mu0;
            mu1.write_locked = 1;
            mu1.tx_id = tx_id;
            if (likely(mutex.cas_acq(mu0, mu1))) {
                set(mutex, Mode::X, tx_id);
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
            if (unlikely(mu1.readers == 0)) {
                mu1.cumulo_readers = 0;
                mu1.tx_id = MAX_TXID;
            }
            if (likely(mutex.cas_rel(mu0, mu1))) {
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
            if (likely(mutex.cas(mu0, mu1))) {
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
    INLINE void swap(WaitDieLock2T& rhs) noexcept {
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


using WaitDieLock2 = WaitDieLock2T<Max_cumulo_readers>;


/**
 * Simpler version with additional store for txids of readers.
 *
 * Comparing with WaitDieData2/WaitDieLock2, this takes more change to wait lock instead abort.
 * Its disadvantage is overhead.
 *
 * This is not fair locking protocol too.
 */
struct WaitDieData3
{
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
        INLINE bool is_read_locked_full() const noexcept { return readers >= Max_txids; }
    };
    static_assert(sizeof(Header) == sizeof(uint64_t));

    Header header; // this must be accessed atomically.

    /**
     * Transactions that have locks.
     * This is preallocated to eliminate benchmark overhead.
     * You need to have the latch to access this vector.
     * The element value MAX_TXID means empty.
     */
    static constexpr size_t Reserved_size = 16; // for small payload.
    static constexpr size_t Max_txids = (CACHE_LINE_SIZE - sizeof(Header) - Reserved_size) / sizeof(TxId);
    static_assert(Max_txids <= Max_readers);
    TxId txids2[Max_txids];


    INLINE WaitDieData3() : header(), txids2() {
        for (size_t i = 0; i < Max_txids; i++) txids2[i] = MAX_TXID;
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
        for (size_t i = 0; i < Max_txids; i++) {
            if (txids2[i] == MAX_TXID) {
                txids2[i] = tx_id;
                return i;
            }
        }
        // If the following code is executed, it is BUG.
        assert(false);
        return SIZE_MAX;
    }
    INLINE void remove_tx_id(size_t idx) noexcept {
        assert(idx < Max_txids);
        txids2[idx] = MAX_TXID;
    }
    INLINE TxId get_min_tx_id() const noexcept {
        TxId min_tx_id = MAX_TXID;
        for (size_t i = 0; i < Max_txids; i++) {
            min_tx_id = std::min(min_tx_id, txids2[i]);
        }
        return min_tx_id;
    }
};


struct WaitDieLock3
{
    using Mode = cybozu::lock::LockStateXS::Mode;
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

    INLINE void set(Mutex* mutexp, Mode mode, TxId tx_id, size_t idx_in_txids = SIZE_MAX) {
        mutexp_ = mutexp;
        mode_ = mode;
        tx_id_ = tx_id;
        idx_in_txids_ = idx_in_txids;
    }

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
            if (unlikely((h0.is_write_locked() || (!h0.latch && h0.is_read_locked_full())) && !is_prior)) {
                return false; // (3b)(5c)
            }
            if (unlikely(h0.latch || h0.is_read_locked_full())) {
                // (2)(3)(4)(5b)(5c) - (3b)(5c) = (2)(3a)(4)(5b)
                h0 = mutex.load();
                continue;
            }
            // (1)(5a)
            Header h1 = h0; h1.latch = 1;
            if (unlikely(!mutex.cas(h0, h1))) continue;
            const size_t idx = mutex.add_tx_id(tx_id);
            h1.readers++;
            h1.tx_id = std::min(h1.tx_id, tx_id);
            h1.latch = 0;
            mutex.store(h1);
            // lock has done.
            set(&mutex, Mode::S, tx_id, idx);
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
            if (unlikely((h0.is_write_locked() || (!h0.latch && h0.is_read_locked())) && !is_prior)) {
                return false; // (3b)(5b)
            }
            if (unlikely(h0.latch || h0.is_locked())) {
                // !(1) - (3b)(5b) = (2)(3a)(4)(5a)
                h0 = mutex.load();
                continue;
            }
            // (1)
            Header h1 = h0;
            h1.latch = 1;
            h1.tx_id = tx_id;
            if (unlikely(!mutex.cas(h0, h1))) continue;
            // lock has done.
            set(&mutex, Mode::X, tx_id);
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
            if (unlikely(h0.latch)) {
                h0 = mutex.load();
                continue;
            }
            Header h1 = h0;
            h1.latch = 1;
            if (unlikely(!mutex.cas(h0, h1))) continue;
            mutex.remove_tx_id(idx_in_txids_);
            h1.readers--;
            h1.latch = 0;
            if (unlikely(h1.readers == 0)) {
                h1.tx_id = MAX_TXID;
            } else if (unlikely(h1.tx_id == tx_id_)) {
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
            if (unlikely(h0.latch)) {
                h0 = mutex.load();
                continue;
            }
            Header h1 = h0;
            h1.latch = 1;
            if (unlikely(!mutex.cas(h0, h1))) continue;
            mutex.remove_tx_id(idx_in_txids_);
            h1.readers = 0;
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


struct RequestType
{
    uint8_t value;

    /**
     * Representation: 0bYXX
     *   XX is the main type.
     *     XX=00: invalid
     *     XX=01: lock
     *     XX=10: unlock
     *     XX=11: upgrade
     *   Y is the optional information of lock and unlock type only.
     *     Y=0: read
     *     Y=1: write
     */
    static constexpr uint8_t INVALID      = 0b000;
    static constexpr uint8_t READ_LOCK    = 0b001;
    static constexpr uint8_t READ_UNLOCK  = 0b010;
    static constexpr uint8_t WRITE_LOCK   = 0b101;
    static constexpr uint8_t WRITE_UNLOCK = 0b110;
    static constexpr uint8_t UPGRADE      = 0b011;

    RequestType(uint8_t value0 = INVALID) noexcept : value(value0) {}
    operator uint8_t() const noexcept { return value; }

    bool is_invalid() const noexcept      { return value == INVALID; }
    bool is_read_lock() const noexcept    { return value == READ_LOCK; }
    bool is_read_unlock() const noexcept  { return value == READ_UNLOCK; }
    bool is_write_lock() const noexcept   { return value == WRITE_LOCK; }
    bool is_write_unlock() const noexcept { return value == WRITE_UNLOCK; }
    bool is_upgrade() const noexcept      { return value == UPGRADE; }

    bool is_lock() const noexcept   { return (value & 0b011) == 0b001; }
    bool is_unlock() const noexcept { return (value & 0b011) == 0b010; }

    bool is_write() const noexcept {
        assert(is_lock() || is_unlock());
        return (value & 0b100) == 0b100;
    }
    bool is_read() const noexcept {
        assert(is_lock() || is_unlock());
        return (value & 0b100) == 0b000;
    }
};


/**
 * This is a queuing lock so is fair locking protocol.
 */
struct WaitDieData4
{
    struct Header {
        static constexpr size_t Readers_bits = 10;
        static constexpr size_t Write_requests_bits = 10;
        static_assert(Readers_bits + Write_requests_bits + 1 <= 32);

        static constexpr size_t Max_readers = (1UL << Readers_bits) - 1;
        static constexpr size_t Max_write_requests = (1UL << Write_requests_bits) - 1;

        union {
            uint64_t obj;
            struct {
                // minimum tx_id (that means prior tx) that holds or waits lock.
                uint32_t tx_id;

                uint32_t readers:Readers_bits;
                uint32_t write_locked:1;

                // If this is 0, readers with lower priority can stand in the request queue.
                uint32_t write_requests:Write_requests_bits;

                uint32_t reserved:(32 - Readers_bits - Write_requests_bits - 1);
            };
        };
        INLINE Header() {
            obj = 0;
            tx_id = MAX_TXID;
        }
        INLINE Header(uint64_t obj0) noexcept { obj = obj0; }
        INLINE operator uint64_t() const noexcept { return obj; }
        std::string str() const {
            return fmtstr("tx_id %u readers %u write_requests %u"
                          , tx_id, readers, write_requests);
        }
        INLINE bool is_read_locked() const noexcept {
            bool ret = readers != 0;
#ifndef NDEBUG
            if (ret) {
                assert(tx_id != MAX_TXID);
                assert(write_locked == 0);
            }
#endif
            return ret;
        }
        INLINE bool is_write_locked() const noexcept {
#ifndef NDEBUG
            if (write_locked) {
                assert(tx_id != MAX_TXID);
                assert(readers == 0);
            }
#endif
            return write_locked;
        }
        INLINE bool is_locked() const noexcept {
            return is_read_locked() || is_write_locked();
        }
        INLINE bool is_unlocked() const noexcept { return !is_locked(); }

        template <typename Ostream>
        void out(Ostream& os) const {
            os << fmtstr("Header txid %u readers %u write_locked %u write_requests %u"
                         , tx_id, readers, write_locked, write_requests);
        }
        template <typename Ostream>
        friend Ostream& operator<<(Ostream& os, const Header& h) {
            h.out(os);
            return os;
        }
    };
    static_assert(sizeof(Header) == sizeof(uint64_t));

    enum Message : uint8_t {
        WAITING   = 0, // Initial value.
        OWNER     = 1, // You became the owner.
        SUCCEEDED = 2, // Your request succeeded.
        FAILED    = 3, // Your request failed.
    };
    struct Request {
        using Message = Message;

        alignas(CACHE_LINE_SIZE)
        Request* next; // Another thread will set the variable.
        TxId tx_id; // read-only.

        // These fields are used by read requests only.
        TxId write_tx_id; // tx_id of the previous write request (if exists).
        TxId read_tx_id; // minimum tx_id of the contiguous read requests with it.

        uint16_t reserved0;
        RequestType req_type; // read only.
        Message receiver; // The owner will set the variable.

        INLINE Request() { reset(); }
        INLINE Request(TxId tx_id0, RequestType req_type0) {
            assert(!req_type0.is_invalid());
            reset(tx_id0, req_type0);
        }
        INLINE void reset(TxId tx_id0 = MAX_TXID,
                          RequestType req_type0 = RequestType::INVALID) {
            next = nullptr;
            tx_id = tx_id0;
            write_tx_id = MAX_TXID;
            read_tx_id = MAX_TXID;
            reserved0 = 0;
            req_type = req_type0;
            receiver = WAITING;
        }

        INLINE Message local_spin_wait() {
            Message msg;
            while ((msg = load_acquire(receiver)) == WAITING) _mm_pause();
            store(receiver, WAITING);
            return msg;
        }
        INLINE void delegate_ownership() { notify(OWNER); }
        INLINE void wait_for_ownership() {
            Message msg = local_spin_wait();
            assert(msg == OWNER); unused(msg);
        }

        INLINE void set_next(Request* next0) { store_release(next, next0); }

        INLINE Request* get_non_empty_next() const {
            Request* next0;
            while ((next0 = load(next)) == nullptr) _mm_pause();
            return next0;
        }
        INLINE void notify(Message msg) { store_release(receiver, msg); }

        /**
         * for debug.
         */
        template <typename Ostream>
        void out(Ostream& os) const {
            // TODO
            os << " ";
        }
        template <typename Ostream>
        friend Ostream& operator<<(Ostream& os, const Request& req) {
            req.out(os);
            return os;
        }
    };

private:
    Header header_; // This variable must be accessed by atomic load/store only.

    uintptr_t tail_; // normal pointer or UNOWNED or OWNED.
    Request* head_; // normal pointer or nullptr.

    using ReqList = NodeListT<Request>;

    ReqList wq_; // waiting queue. This is FIFO.

public:
    INLINE WaitDieData4() : header_(), tail_(mcslike::UNOWNED), head_(nullptr), wq_() {}

    INLINE bool do_request(Request& req) {
        assert(!req.req_type.is_invalid());
        const Message msg = mcslike::do_request_sync(
            req, tail_, head_, [&](Request& tail) { owner_task(req, tail); });
        assert(msg == SUCCEEDED || msg == FAILED);
        return msg == SUCCEEDED;
    }
    /**
     * Debug or test purpuse.
     */
    INLINE void do_request_async(Request& req) {
        assert(!req.req_type.is_invalid());
        mcslike::do_request_async(
            req, tail_, head_, [&](Request& tail) { owner_task(req, tail); });
        // The caller must call req.local_spin_wait() later.
    }

    INLINE Header load_header() const { return load_acquire(header_.obj); }

    /**
     * for debug.
     */
    template <typename Ostream>
    void out(Ostream& os) const {
        os << "WaitDieData4 [" << header_ << "] ";
        os << fmtstr("tail %u(%p) head %p"
                     , tail_, (void*)tail_, head_);
        // TODO
    }
    template <typename Ostream>
    friend Ostream& operator<<(Ostream& os, const WaitDieData4& req) {
        req.out(os);
        return os;
    }
private:
    INLINE void store_header(Header h0) { store_release(header_.obj, h0.obj); }

    INLINE void owner_task(Request& head, Request& tail) {
        ReqList unlock_list, lock_list, upgrade_list;
        size_t nr_write_unlock = 0, nr_read_unlock = 0;
        size_t nr_upgrade = 0;
        const Header h0 = load_header();
        Header h1 = h0;

        // Dispatch new requests.
        Request* req = &head;
        while (req != nullptr) {
            // req->next will be destroyed so we obtain its value at first.
            Request* next = (req == &tail ? nullptr : req->get_non_empty_next());
            const RequestType req_type = load(req->req_type);
            if (likely(req_type.is_lock())) {
                if (unlikely(!try_add_lock_req_to_wait_queue(h1, req))) {
                    req->notify(FAILED);
                }
            } else if (unlikely(req_type.is_upgrade())) {
                if (!try_add_upgrade_req_to_wait_queue(h1, req)) {
                    req->notify(FAILED);
                } else {
                    nr_upgrade++;
                }
            } else {
                assert(req_type.is_unlock());
                if (req_type.is_write()) {
                    nr_write_unlock++;
                } else {
                    assert(req_type.is_read());
                    nr_read_unlock++;
                }
                unlock_list.push_back(req);
            }
            req = next;
        }
        assert(nr_upgrade <= 1);
        assert(nr_write_unlock <= 1);
        assert(nr_write_unlock == 0 || nr_read_unlock == 0);

        prepare_unlock_requests(h1, nr_write_unlock, nr_read_unlock);
        Request* upgrade_req = (nr_upgrade == 0 ? nullptr : prepare_upgrade_request(h1));
        prepare_lock_requests(h1, lock_list);
#ifndef NDEBUG
        if (h1.is_unlocked()) {
            assert(h1.write_requests == 0);
            assert(h1.tx_id == MAX_TXID);
        }
#endif
        store_header(h1);
        notify_success_to_all(unlock_list);
        if (unlikely(upgrade_req != nullptr)) upgrade_req->notify(SUCCEEDED);
        notify_success_to_all(lock_list);
    }
    INLINE bool try_add_upgrade_req_to_wait_queue(const Header& h0, Request* req) {
        assert(load(req->req_type).is_upgrade());
        if (h0.readers != 1 || !wq_.empty()) return false;
        assert(h0.is_read_locked());
        wq_.push_back(req);
        return true;
    }
    INLINE bool try_add_lock_req_to_wait_queue(Header& h0, Request* req) {
        const RequestType req_type = load(req->req_type);
        const TxId tx_id = load(req->tx_id);
        assert(req_type.is_lock());
        if (req_type.is_write()) {
            if (likely(wq_.empty())) {
                if (likely(h0.is_unlocked() || tx_id <= h0.tx_id)) {
                    // Use '<=' instead '<' because the worker may used the same tx_id before.
                    assert(h0.write_requests < Header::Max_write_requests); // TODO
                    h0.write_requests++;
                    wq_.push_back(req);
                    return true;
                }
                return false;
            }
            Request& back = *wq_.back();
            const RequestType back_req_type = load(back.req_type);
            const bool back_is_write = back_req_type.is_upgrade() || back_req_type.is_write_lock();
            const TxId check_tx_id = (back_is_write ? load(back.tx_id) : load(back.read_tx_id));
            if (likely(tx_id < check_tx_id)) {
                assert(h0.write_requests < Header::Max_write_requests); // TODO
                h0.write_requests++;
                wq_.push_back(req);
                return true;
            }
            return false;
        }
        assert(req_type.is_read());
        if (likely(wq_.empty())) {
            if (likely(h0.is_unlocked() || h0.is_read_locked())) {
                // TODO: if read locked, check Max_readers limitation.
                assert(load(req->write_tx_id) == MAX_TXID);
                store(req->read_tx_id, std::min(h0.tx_id, load(req->tx_id)));
                wq_.push_back(req);
                return true;
            }
            return false;
        }
        Request& back = *wq_.back();
        const RequestType back_req_type = load(back.req_type);
        const bool back_is_write = back_req_type.is_upgrade() || back_req_type.is_write_lock();
        if (back_is_write) {
            const TxId back_tx_id = load(back.tx_id);
            if (likely(tx_id < back_tx_id)) {
                store(req->write_tx_id, back_tx_id);
                store(req->read_tx_id, tx_id);
                wq_.push_back(req);
                return true;
            }
            return false;
        }
        assert(load(back.req_type).is_read());
        // TODO: check Max_readers limitation.
        const TxId back_write_tx_id = load(back.write_tx_id);
        if (likely(tx_id < back_write_tx_id)) {
            store(req->write_tx_id, back_write_tx_id);
            store(req->read_tx_id, std::min(tx_id, load(back.read_tx_id)));
            wq_.push_back(req);
            return true;
        }
        return false;
    }
    INLINE void prepare_unlock_requests(Header& h0, size_t nr_write, size_t nr_read) noexcept {
        assert(nr_write == 0 || nr_read == 0);
        if (nr_read != 0) {
            assert(h0.readers >= nr_read);
            h0.readers -= nr_read;
            if (unlikely(h0.readers == 0)) h0.tx_id = MAX_TXID;
            return;
        }
        if (nr_write != 0) {
            assert(h0.is_write_locked());
            h0.write_locked = 0;
            h0.tx_id = MAX_TXID;
            return;
        }
        // do nothing because there exist no unlock requests.
    }
    INLINE Request* prepare_upgrade_request(Header& h0) noexcept {
        assert(!wq_.empty());
        Request* req = wq_.front();
        assert(load(req->req_type).is_upgrade());
        assert(h0.readers == 1);
        assert(h0.write_locked == 0);
        assert(h0.tx_id != MAX_TXID);
        h0.tx_id = load(req->tx_id);
        h0.write_locked = 1;
        h0.readers = 0;
        wq_.pop_front();
        return req;
    }
    INLINE void prepare_lock_requests(Header& h0, ReqList& lock_list) noexcept {
        if (wq_.empty()) return;
        assert(lock_list.empty());

        Request* req = wq_.front();
        if (load(req->req_type).is_write()) {
            if (h0.is_locked()) return; // still waiting.
            move_write_request_to_lock_list(h0, lock_list);
        } else {
            if (h0.is_write_locked()) return; // still waiting.
            move_read_requests_to_lock_list(h0, lock_list);
        }
    }
    INLINE void move_write_request_to_lock_list(Header& h0, ReqList& lock_list) noexcept {
        Request* req = wq_.front();
        assert(req != nullptr);
        assert(load(req->req_type).is_write());
        h0.tx_id = load(req->tx_id);
        h0.write_locked = 1;
        assert(h0.write_requests > 0);
        h0.write_requests--;
        wq_.pop_front();
        lock_list.push_back(req);
    }
    INLINE void move_read_requests_to_lock_list(Header& h0, ReqList& lock_list) noexcept {
        Request* req = wq_.front();
        assert(req != nullptr);
        assert(load(req->req_type).is_read());
        while (req != nullptr) {
            if (unlikely(h0.readers >= Max_readers)) {
                // TODO: Check Max_readers at try_add_to_wait_queue().
                wq_.pop_front();
                req->notify(FAILED);
            } else {
                h0.readers++;
                h0.tx_id = std::min(h0.tx_id, load(req->tx_id));
                wq_.pop_front();
                lock_list.push_back(req);
            }

            // next request.
            if (wq_.empty()) {
                req = nullptr;
            } else {
                req = wq_.front();
                if (load(req->req_type).is_write()) req = nullptr;
            }
        }
    }
    INLINE void notify_success_to_all(ReqList& req_list) noexcept {
        while (!req_list.empty()) {
            Request& req = *req_list.front();
            req_list.pop_front();
            // pop_front must be called before notification.
            req.notify(SUCCEEDED);
        }
    }
};


struct WaitDieLock4
{
    using Mode = cybozu::lock::LockStateXS::Mode;
    using Mutex = WaitDieData4;
    using Header = Mutex::Header;
    using Message = Mutex::Message;
    using Request = Mutex::Request;

private:
    Mutex *mutexp_;
    Mode mode_;
    TxId tx_id_;

public:
    INLINE WaitDieLock4() noexcept : mutexp_(nullptr), mode_(Mode::INVALID), tx_id_(MAX_TXID) {}
    INLINE ~WaitDieLock4() noexcept { unlock(); }
    INLINE WaitDieLock4(const WaitDieLock4& ) = delete;
    INLINE WaitDieLock4(WaitDieLock4&& rhs) noexcept : WaitDieLock4() { swap(rhs); }
    INLINE WaitDieLock4& operator=(const WaitDieLock4& rhs) = delete;
    INLINE WaitDieLock4& operator=(WaitDieLock4&& rhs) noexcept { swap(rhs); return *this; }

    /**
     * This is for blind-write.
     */
    INLINE void setMutex(Mutex& mutex) noexcept { mutexp_ = &mutex; }

    INLINE void set(Mutex* mutexp, Mode mode, TxId tx_id) {
        mutexp_ = mutexp;
        mode_ = mode;
        tx_id_ = tx_id;
    }

    /**
     * If true, locked.
     * If false, you must abort your running transaction.
     */
    INLINE bool readLock(Mutex& mutex, TxId tx_id) noexcept {
        assert(tx_id != MAX_TXID);
        Header h0 = mutex.load_header();
        const bool writer_exists = (h0.is_write_locked() ||
                                    (h0.is_read_locked() && h0.write_requests > 0));
        if (unlikely(writer_exists && h0.tx_id < tx_id)) return false;

        Request req(tx_id, RequestType::READ_LOCK);
        if (unlikely(!mutex.do_request(req))) return false;
        set(&mutex, Mode::S, tx_id);
        return true;
    }
    INLINE bool writeLock(Mutex& mutex, TxId tx_id) noexcept {
        assert(tx_id != MAX_TXID);
        Header h0 = mutex.load_header();
        if (unlikely(h0.is_locked() && h0.tx_id < tx_id)) return false;

        Request req(tx_id, RequestType::WRITE_LOCK);
        if (unlikely(!mutex.do_request(req))) return false;
        set(&mutex, Mode::X, tx_id);
        return true;
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
        Request req(tx_id_, RequestType::READ_UNLOCK);
        bool ret = mutex.do_request(req);
        assert(ret); unused(ret);
        init();
    }
    INLINE void writeUnlock() noexcept {
        assert_locked(Mode::X);
        Mutex& mutex = *mutexp_;
#ifndef NDEBUG
        Header h0 = mutex.load_header();
        assert(h0.is_write_locked());
#endif
        Request req(tx_id_, RequestType::WRITE_UNLOCK);
        bool ret = mutex.do_request(req);
        assert(ret); unused(ret);
        init();
    }
    INLINE bool upgrade() noexcept {
        assert_locked(Mode::S);
        Mutex& mutex = *mutexp_;
        Header h0 = mutex.load_header();
        if (unlikely(h0.readers != 1 || h0.write_requests != 0)) return false;

        Request req(tx_id_, RequestType::UPGRADE);
        if (unlikely(!mutex.do_request(req))) return false;

        mode_ = Mode::X;
        return true;
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
    INLINE void swap(WaitDieLock4& rhs) noexcept {
        std::swap(mutexp_, rhs.mutexp_);
        std::swap(mode_, rhs.mode_);
        std::swap(tx_id_, rhs.tx_id_);
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
#if 0
    using Lock = WaitDieLock2;
#elif 0
    using Lock = WaitDieLock3;
#elif 1
    using Lock = WaitDieLock4;
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
        OpEntryL &ope = vec_.emplace_back();
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
        OpEntryL& ope = vec_.emplace_back();
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
        OpEntryL &ope = vec_.emplace_back();
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
