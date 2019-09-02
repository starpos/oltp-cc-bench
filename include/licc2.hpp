#pragma once
/**
 * Lock Interception Concurrency Control (LICC) version 2 for 1VCC.
 *
 * (1) simple version.
 *   64bit mutex object and use CAS to change it.
 *   i_ver and readers are removed and is_writer bit is added.
 *
 * (2) completely starvation-free version.
 *   It is like MCS-like Lock. Requests are sorted by ord_id.
 */
#include <cinttypes>
#include <cstring>
#include <utility>
#include <string>
#include <sstream>
#include <vector>
#include "util.hpp"
#include "inline.hpp"
#include "arch.hpp"
#include "atomic_wrapper.hpp"
#include "write_set.hpp"
#include "allocator.hpp"
#include "vector_payload.hpp"


namespace cybozu {
namespace lock {
namespace licc2 {


const uint32_t MAX_ORD_ID = UINT32_MAX;


/**
 * Mutex data.
 */
struct MutexData
{
    alignas(sizeof(uint64_t))
    union {
        uint32_t ord_id;
        struct {
            // lower bits on little endian architecture.
            uint32_t worker_id:10;
            uint32_t epoch_id:22;
        };
    };
    uint32_t version:30;
    uint32_t protected_:1;
    uint32_t is_writer:1;

    MutexData() = default; // CAUSION: not initialized
    MutexData(const MutexData&) = default;
    MutexData& operator=(const MutexData&) = default;

    MutexData(uint64_t x) : MutexData() {
        ::memcpy(this, &x, sizeof(*this));
    }
    operator uint64_t() const {
        uint64_t x;
        ::memcpy(&x, this, sizeof(x));
        return x;
    }

    // Reference of uint64_t.
    uint64_t& ref() { return *(uint64_t *)this; }
    const uint64_t& ref() const { return *(uint64_t *)this; }
    // Value of uint64_t explicitly.  (x.val() is shorter than uint64_t(x).)
    uint64_t val() const { return *this; }

    bool operator==(const MutexData& rhs) const {
        return uint64_t(*this) == uint64_t(rhs);
    }
    bool operator!=(const MutexData& rhs) const {
        return !(*this == rhs);
    }

    void init() {
        ord_id = MAX_ORD_ID;
        version = 0;
        protected_ = 0;
        is_writer = 0;
    }
    std::string str() const {
        return cybozu::util::formatString(
            "MutexData{ord:%x worker:%x epoch:%x ver:%u protected:%u is_writer:%u}"
            , ord_id, worker_id, epoch_id, version, protected_, is_writer);
    }

    bool is_valid(uint32_t version0, bool allow_protected = false) const {
        return (allow_protected || !protected_) && version == version0;
    }

    bool is_unreserved() const {
        return ord_id == MAX_ORD_ID;
    }
    bool is_shared() const {
        return ord_id != MAX_ORD_ID && is_writer == 0;
    }
    bool is_unreserved_or_shared() const {
        return ord_id == MAX_ORD_ID || is_writer == 0;
    }
    bool can_intercept(uint32_t ord_id0) const {
        // equality is required to be included for reserving-again.
        return ord_id0 <= ord_id;
    }
    bool can_read_reserve(uint32_t ord_id0) const {
        return is_unreserved_or_shared() || can_intercept(ord_id0);
    }
    bool can_write_reserve(uint32_t ord_id0) const {
        return is_unreserved() || can_intercept(ord_id0);
    }
    void prepare_read_reserve(uint32_t ord_id0) {
        is_writer = 0;
        ord_id = std::min(ord_id, ord_id0);
    }
    void prepare_write_reserve(uint32_t ord_id0) {
        is_writer = 1;
        ord_id = ord_id0;
    }
};


static_assert(sizeof(MutexData) == sizeof(uint64_t));


/**
 * Lock state transitions.
 *
 * INIT --> READ (by first read)
 * INIT --> PRE_BLIND_WRITE (by first write, not reserved yet)
 * READ --> READ_MODIFY_WRITE  (by upgrade)
 * PRE_BLIND_WRITE --> BLIND_WRITE  (reservation in pre-commit phase)
 * BLIND_WRITE or READ_MODIFY_WRITE --> PROTECTED (protection in pre-commit phase.)
 * READ or BLIND_WRITE or READ_MODIFY_WRITE --> INIT (by unreserve)
 * PRE_BLIND_WRITE --> INIT (abort)
 * PROTECTED --> INIT (by unprotect)
 */
enum class LockState : uint8_t
{
    INIT = 0,
    READ = 1,
    PRE_BLIND_WRITE = 2,
    BLIND_WRITE = 3,
    READ_MODIFY_WRITE = 4,
    PROTECTED = 5,
    MAX = 6,
};


const char* lock_state_str(LockState st)
{
    struct {
        LockState st;
        const char* name;
    } table [] = {
        {LockState::INIT, "INIT"},
        {LockState::READ, "READ"},
        {LockState::PRE_BLIND_WRITE, "PRE_BLIND_WRITE"},
        {LockState::BLIND_WRITE, "BLIND_WRITE"},
        {LockState::READ_MODIFY_WRITE, "READ_MODIFY_WRITE"},
        {LockState::PROTECTED, "PROTECTED"},
    };
    for (size_t i = 0; i < sizeof(table) / sizeof(table[0]); i++) {
        if (table[i].st == st) {
            return table[i].name;
        }
    }
    return "UNKNOWN";
}


template <typename Ostream>
Ostream& operator<<(Ostream& os, LockState st)
{
    os << lock_state_str(st);
    return os;
}


constexpr bool is_lock_state_in(LockState st, std::initializer_list<LockState> st_list)
{
    for (LockState st0 : st_list) {
        if (st == st0) return true;
    }
    return false;
}


constexpr bool is_protectable(LockState st)
{
    return is_lock_state_in(st, {LockState::BLIND_WRITE, LockState::READ_MODIFY_WRITE});
}


/**
 * Lock data.
 */
struct LockData
{
    LockState state;
    bool updated;
    uint32_t ord_id;
    uint32_t version;

#ifdef NDEBUG
    LockData() = default; // CAUSION: not initialied.
#else
    LockData() : state(LockState::INIT), updated(false), ord_id(MAX_ORD_ID), version(0) {
    }
#endif
    explicit LockData(uint32_t ord_id0) : LockData() { init(ord_id0); }

    void init(uint32_t ord_id0) {
        state = LockState::INIT;
        updated = false;
        ord_id = ord_id0;
        version = 0;
    }

    std::string str() const {
        return cybozu::util::formatString(
            "LockData{state:%s updated:%u ord:%u ver:%u}"
            , lock_state_str(state), updated, ord_id, version);
    }

    bool is_state(LockState st) const {
        return st == state;
    }
    bool is_state_in(std::initializer_list<LockState> st_list) const {
        return is_lock_state_in(state, st_list);
    }
};


enum MutexOpCapability: uint8_t
{
    POSSIBLE = 0,
    MUST_WAIT = 1,
    IMPOSSIBLE = 2,
};


/**
 * Create (LockData, MutexData) from initial data and operations.
 */
struct MutexOpCreator
{
    MutexOpCapability capability;
    LockData ld;
    MutexData md;

    MutexOpCreator() = default; // CAUSION. not initialized.

    // MUST_WAIT or IMPOSSIBLE.
    explicit MutexOpCreator(MutexOpCapability res) : capability(res), ld(), md() {
        assert(res != POSSIBLE);
    }
    // Valid initial value.
    MutexOpCreator(LockData ld0, MutexData md0) : capability(POSSIBLE), ld(ld0), md(md0) {
    }

    MutexOpCreator(const MutexOpCreator&) = default;
    MutexOpCreator& operator=(const MutexOpCreator&) = default;
    MutexOpCreator clone() const { return *this; }

    explicit operator bool() const { return possible(); }
    bool possible() const { return capability == POSSIBLE; }


    std::string str() const {
        std::stringstream ss;
        if (capability == POSSIBLE) ss << "POSSIBLE";
        else if (capability == MUST_WAIT) ss << "MUST_WAIT";
        else if (capability == IMPOSSIBLE) ss << "IMPOSSIBLE";
        else ss << "UNKONWON";
        ss << " " << ld.str() << " " << md.str();
        return ss.str();
    }


    /**
     * Operations can be chained.
     */

    /**
     * No need to change mutex object.
     */
    MutexOpCreator invisible_read() const {
        MutexOpCreator moc = clone();
        LockData& ld = moc.ld; MutexData& md = moc.md;
        assert(ld.state == LockState::INIT);
        ld.state = LockState::READ;
        ld.version = md.version;
        return moc;
    }
    /**
     * No need to change mutex object.
     */
    MutexOpCreator blind_write() const {
        MutexOpCreator moc = clone();
        LockData& ld = moc.ld;
        assert(ld.state == LockState::INIT);
        ld.state = LockState::PRE_BLIND_WRITE;
        return moc;
    }
    /**
     * At 1st read operation, call this function.
     */
    template <LockState lock_state = LockState::INIT>
    MutexOpCreator read_reserve_1st() const {
        static_assert(is_lock_state_in(lock_state, {LockState::INIT, LockState::READ}));
        MutexOpCreator moc = clone();
        if (!possible()) return moc;
        LockData& ld = moc.ld; MutexData& md = moc.md;
        if (!md.can_read_reserve(ld.ord_id) || md.protected_) {
            return MutexOpCreator(MUST_WAIT);
        }
        md.prepare_read_reserve(ld.ord_id);
        assert(ld.state == lock_state);
        ld.state = LockState::READ;
        ld.version = md.version;
        return moc;
    }
    /**
     * At 2nd or more later read operation,
     * mutex check and if the reservation is intercepted, you can recover the reservation.
     */
    template <bool allow_different_version = false>
    MutexOpCreator read_reserve_recover() const {
        MutexOpCreator moc = clone();
        if (!possible()) return moc;
        LockData& ld = moc.ld; MutexData& md = moc.md;
        if (allow_different_version)  {
            if (md.protected_) {
                return MutexOpCreator(MUST_WAIT);
            }
        } else {
            if (ld.version != md.version || md.protected_) {
                return MutexOpCreator(IMPOSSIBLE);
            }
        }
        if (!md.can_read_reserve(ld.ord_id)) {
            return MutexOpCreator(MUST_WAIT);
        }
        assert(ld.state == LockState::READ);
        md.prepare_read_reserve(ld.ord_id);
        return moc;
    }
    template <LockState lock_state>
    MutexOpCreator blind_write_reserve_detail() const {
        static_assert(lock_state == LockState::PRE_BLIND_WRITE || lock_state == LockState::BLIND_WRITE);
        MutexOpCreator moc = clone();
        if (!possible()) return moc;
        LockData& ld = moc.ld; MutexData& md = moc.md;
        if (!md.can_write_reserve(ld.ord_id) || md.protected_) {  // wait for unprotected here.
            return MutexOpCreator(MUST_WAIT);
        }
        md.prepare_write_reserve(ld.ord_id);
        assert(ld.state == lock_state);
        ld.state = LockState::BLIND_WRITE;
        return moc;
    }
    MutexOpCreator blind_write_reserve_1st() const {
        return blind_write_reserve_detail<LockState::PRE_BLIND_WRITE>();
    }
    MutexOpCreator blind_write_reserve_recover() const {
        return blind_write_reserve_detail<LockState::BLIND_WRITE>();
    }
    template <LockState lock_state>
    MutexOpCreator read_modify_write_reserve_detail() const {
        static_assert(lock_state == LockState::READ || lock_state == LockState::READ_MODIFY_WRITE);
        MutexOpCreator moc = clone();
        if (!possible()) return moc;
        LockData& ld = moc.ld; MutexData& md = moc.md;
        if (md.version != ld.version || md.protected_) {
            // If protected, we expect the corresponding transaction will success its pre-commit,
            // so here we prefer abort.
            return MutexOpCreator(IMPOSSIBLE);
        }
        if (md.ord_id < ld.ord_id) {
            return MutexOpCreator(md.is_writer ? IMPOSSIBLE : MUST_WAIT);
        }
        md.prepare_write_reserve(ld.ord_id);
        assert(ld.state == lock_state);
        ld.state = LockState::READ_MODIFY_WRITE;
        return moc;
    }
    MutexOpCreator upgrade_reservation() const {
        return read_modify_write_reserve_detail<LockState::READ>();
    }
    MutexOpCreator read_modify_write_reserve_recover() const {
        return read_modify_write_reserve_detail<LockState::READ_MODIFY_WRITE>();
    }
    template <LockState lock_state>
    MutexOpCreator keep_reservation() const {
        static_assert(is_lock_state_in(lock_state, {LockState::READ, LockState::READ_MODIFY_WRITE}));
        assert(ld.state == lock_state);
        if (lock_state == LockState::READ) {
            return read_reserve_recover<false>();
        } else {
            return read_modify_write_reserve_recover();
        }
    }
    template <LockState lock_state = LockState::INIT>
    MutexOpCreator read_modify_write_reserve_1st() const {
        static_assert(is_lock_state_in(lock_state, {LockState::INIT, LockState::READ_MODIFY_WRITE}));
        MutexOpCreator moc = clone();
        if (!possible()) return moc;
        LockData& ld = moc.ld; MutexData& md = moc.md;
        if (!md.can_write_reserve(ld.ord_id) || md.protected_) {
            // We must wait for unprotected to ensure it before calling protect_all().
            return MutexOpCreator(MUST_WAIT);
        }
        md.prepare_write_reserve(ld.ord_id);
        assert(ld.state == lock_state);
        ld.state = LockState::READ_MODIFY_WRITE;
        ld.version = md.version;
        return moc;
    }
    template <bool do_write_reserve, bool is_retry>
    MutexOpCreator reserve_for_read() const {
        if (do_write_reserve) {
            constexpr LockState lock_state = is_retry ? LockState::READ_MODIFY_WRITE : LockState::INIT;
            return read_modify_write_reserve_1st<lock_state>();
        } else {
            constexpr LockState lock_state = is_retry ? LockState::READ : LockState::INIT;
            return read_reserve_1st<lock_state>();
        }
    }
    template <LockState lock_state>
    MutexOpCreator write_reserve_recover() const {
        static_assert(is_protectable(lock_state));
        assert(ld.state == lock_state);
        if (lock_state == LockState::BLIND_WRITE) {
            return blind_write_reserve_recover();
        } else if (lock_state == LockState::READ_MODIFY_WRITE) {
            return read_modify_write_reserve_recover();
        } else {
            BUG();
        }
    }
    template <LockState lock_state>
    MutexOpCreator protect() const {
        static_assert(is_protectable(lock_state));
        MutexOpCreator moc = clone();
        if (!moc) return moc;
        LockData& ld = moc.ld; MutexData& md = moc.md;
        assert(ld.state == lock_state);
        if (lock_state == LockState::READ_MODIFY_WRITE && ld.version != md.version) {
            return MutexOpCreator(IMPOSSIBLE);
        }
        if (ld.ord_id != md.ord_id || md.protected_) {
            return MutexOpCreator(IMPOSSIBLE);
        }
        // If the lock object is read reserved, we consider the upgrade reservation will be processed at the same time.
        ld.state = LockState::PROTECTED;
        md.ord_id = MAX_ORD_ID;
        md.protected_ = 1;
        return moc;
    }
    MutexOpCreator unlock() const {
        MutexOpCreator moc = clone();
        if (!moc) return moc;
        LockData& ld = moc.ld; MutexData& md = moc.md;

        switch (ld.state) {
        case LockState::READ:
        case LockState::BLIND_WRITE:
        case LockState::READ_MODIFY_WRITE:
            if (ld.ord_id == md.ord_id) md.ord_id = MAX_ORD_ID;
            break;
        case LockState::PROTECTED:
            md.protected_ = 0;
            if (ld.updated) md.version++;
            break;
        case LockState::INIT:
        case LockState::PRE_BLIND_WRITE:
            // do nothing;
            break;
        default:
            assert(false);
        }
        ld.state = LockState::INIT;
        return moc;
    }
};


/**
 * Simple CAS-only locking.
 * Starfation-freeness depends on possibility of occurrence of starvation on CAS operations.
 */
namespace cas {


struct Mutex
{
    MutexData md;
    Mutex() : md() { md.init(); }

    MutexData load() const {
        return load_acquire(md.ref());
    }
    void store(MutexData md0) {
        store_release(md.ref(), md0.val());
    }
    bool cas(MutexData& before, MutexData after) {
        return compare_exchange(md.ref(), before.ref(), after.val());
    }
};


class Lock
{
private:
    Mutex* mutex_;
    LockData ld_;

public:
    INLINE Lock() : mutex_(nullptr), ld_() {
    }
    INLINE Lock(Mutex& mutex, uint32_t ord_id) : mutex_(&mutex), ld_(ord_id) {
    }
    INLINE ~Lock() {
        unlock();
    }

    void init(Mutex& mutex, uint32_t ord_id) {
        mutex_ = &mutex;
        ld_.init(ord_id);
    }

    // copy constructor/assign operator are removed.
    Lock(const Lock&) = delete;
    Lock& operator=(const Lock&) = delete;

    // move constructor/assign operator.
    Lock(Lock&& rhs) noexcept : Lock() { swap(rhs); }
    Lock& operator=(Lock&& rhs) noexcept { swap(rhs); return *this; }


    /**
     * INIT --> READ.
     */
    void invisible_read(const void *shared, void *local, size_t size) {
        unused(shared, local, size);
        MutexData md0 = mutex_->load();
        for (;;) {
            _mm_pause();
            if (md0.protected_) {
                md0 = mutex_->load();
                continue;
            }
            MutexOpCreator moc0(ld_, md0);
            MutexOpCreator moc1 = moc0.invisible_read();
            assert(moc1.possible());
            assert(md0 == moc1.md);
#ifndef NO_PAYLOAD
            ::memcpy(local, shared, size);
#endif
            acquire_memory_barrier();
            MutexData md1 = mutex_->load();
            if (!md1.is_valid(moc1.ld.version)) {
                md0 = md1;
                continue;
            }
            ld_ = moc1.ld;
            return;
        }
    }
    /**
     * WRITE_RESERVE = true
     *   INIT --> READ_MODIFY_WRITE
     * otherwise
     *   INIT --> READ
     */
    template <bool do_write_reserve = false>
    void read_and_reserve(const void *shared, void *local, size_t size) {
        unused(shared, local, size);
        MutexData md0 = mutex_->load();
        for (;;) {
            _mm_pause();
            MutexOpCreator moc0(ld_, md0);
            MutexOpCreator moc1 = moc0.reserve_for_read<do_write_reserve, false>();
            if (moc1.capability == MUST_WAIT) {
                md0 = mutex_->load();
                continue;
            }
            assert(moc1.capability == POSSIBLE);
            assert(!moc1.md.protected_);
#ifndef NO_PAYLOAD
            ::memcpy(local, shared, size);
#endif
            acquire_memory_barrier();
            if (!do_write_reserve && md0 == moc1.md) {
                // CAS is not required but verify is required.
                MutexData md1 = mutex_->load();
                if (md1.is_valid(md0.version)) {
                    ld_ = moc1.ld;
                    return;
                }
                md0 = md1;
                continue;
            }
            if (mutex_->cas(md0, moc1.md)) {
                ld_ = moc1.ld;
                return;
            }
            // CAS failed and continue.
        }
    }
    template <bool do_write_reserve, bool is_retry>
    MutexData reserve_for_read(MutexData md0) {
        for (;;) {
            _mm_pause();
            MutexOpCreator moc0(ld_, md0);
            MutexOpCreator moc1 = moc0.reserve_for_read<do_write_reserve, is_retry>();
            if (moc1.capability == MUST_WAIT) {
                md0 = mutex_->load();
                continue;
            }
            assert(moc1.capability == POSSIBLE);
            assert(!moc1.md.protected_);
            if ((!do_write_reserve && md0 == moc1.md) || mutex_->cas(md0, moc1.md)) {
                ld_ = moc1.ld;
                return moc1.md;
            }
            // CAS failed and continue.
        }
    }
    /**
     * A bit difrerent version of read_and_reserve().
     * reserve() --> contents read ---> verify
     */
    template <bool do_write_reserve = false>
    void read_and_reserve2(const void* shared, void* local, size_t size) {
        unused(shared, local, size);
        MutexData md0 = mutex_->load();
        md0 = reserve_for_read<do_write_reserve, false>(md0);
        for (;;) {
            _mm_pause();
#ifndef NO_PAYLOAD
            ::memcpy(local, shared, size);
#endif
            acquire_memory_barrier();
            MutexData md1 = mutex_->load();
            if (md1.is_valid(md0.version)) return;
            md0 = reserve_for_read<do_write_reserve, true>(md1);
        }
    }
    /**
     * Try to keep reservation for READ and READ_MODIFY_WRITE state.
     * This checks version unchanged.
     */
    template <LockState lock_state>
    bool try_keep_reservation() {
        static_assert(is_lock_state_in(lock_state, {LockState::READ, LockState::READ_MODIFY_WRITE}));
        assert(ld_.state == lock_state);
        MutexData md0 = mutex_->load();
        for (;;) {
            MutexOpCreator moc0(ld_, md0);
            MutexOpCreator moc1 = moc0.keep_reservation<lock_state>();
            if (moc1.capability == IMPOSSIBLE) return false;
            if (moc1.capability == MUST_WAIT) {
                md0 = mutex_->load();
                continue;
            }
            assert(moc1.capability == POSSIBLE);
            if (md0 == moc1.md || mutex_->cas(md0, moc1.md)) {
                ld_ = moc1.ld;
                return true;
            }
            // CAS failed and continue.
        }
    }
    void blind_write() {
        assert(ld_.state == LockState::INIT);
        ld_.state = LockState::PRE_BLIND_WRITE;
    }
    void blind_write_reserve() {
        MutexData md0 = mutex_->load();
        for (;;) {
            _mm_pause();
            MutexOpCreator moc0(ld_, md0);
            MutexOpCreator moc1 = moc0.blind_write_reserve_1st();
            if (moc1.capability == MUST_WAIT) {
                md0 = mutex_->load();
                continue;
            }
            assert(moc1.capability == POSSIBLE);
            assert(md0 != moc1.md);
            if (mutex_->cas(md0, moc1.md)) {
                ld_ = moc1.ld;
                return;
            }
            // CAS failed and continue.
        }
    }
    bool upgrade() {
        MutexData md0 = mutex_->load();
        for (;;) {
            _mm_pause();
            MutexOpCreator moc0(ld_, md0);
            MutexOpCreator moc1 = moc0.upgrade_reservation();
            if (moc1.capability == IMPOSSIBLE) return false;
            if (moc1.capability == MUST_WAIT) {
                md0 = mutex_->load();
                continue;
            }
            assert(moc1.capability == POSSIBLE);
            assert(md0 != moc1.md);
            if (mutex_->cas(md0, moc1.md)) {
                ld_ = moc1.ld;
                return true;
            }
            // CAS failed and continue.
        }
    }
    bool upgrade_if_necessary() {
        if (ld_.state != LockState::READ) {
            // no need to upgrade.
            return true;
        }
        return upgrade();
    }
    /**
     * Now this is not required.
     */
    template <LockState lock_state>
    bool wait_for_unprotected() {
        static_assert(is_protectable(lock_state));
        assert(ld_.state == lock_state);
        MutexData md0 = mutex_->load();
        for (;;) {
            _mm_pause();
            if (md0.ord_id != ld_.ord_id) {
                MutexOpCreator moc0(ld_, md0);
                MutexOpCreator moc1 = moc0.write_reserve_recover<lock_state>();
                if (!moc1.possible()) return false;
                if (md0 != moc1.md && !mutex_->cas(md0, moc1.md)) continue;
                ld_ = moc1.ld;
            }
            if (md0.protected_) {
                md0 = mutex_->load();
                continue;
            }
            return true;
        }
    }
    template <LockState lock_state>
    bool protect() {
        static_assert(is_protectable(lock_state));
        assert(ld_.state == lock_state);
        MutexData md0 = mutex_->load();
        for (;;) {
            _mm_pause();
            MutexOpCreator moc0(ld_, md0);
            MutexOpCreator moc1 = moc0.write_reserve_recover<lock_state>();
            MutexOpCreator moc2 = moc1.protect<lock_state>();
            if (!moc2.possible()) return false;
            if (mutex_->cas(md0, moc2.md)) {
                ld_ = moc2.ld;
                return true;
            }
            // CAS failed and continue.
        }
    }
    void unlock() {
        if (mutex_ == nullptr) return;
        MutexData md0 = mutex_->load();
        for (;;) {
            _mm_pause();
            MutexOpCreator moc0(ld_, md0);
            MutexOpCreator moc1 = moc0.unlock();
            assert(moc1.possible());
            if (md0 == moc1.md || mutex_->cas(md0, moc1.md)) {
                ld_ = moc1.ld;
                mutex_ = nullptr;
                return;
            }
            // CAS failed and continue.
        }
    }
    bool is_unchanged(bool allow_protected = false) const {
        assert(mutex_);
        return mutex_->load().is_valid(ld_.version, allow_protected);
    }
    void update() {
        assert(ld_.state == LockState::PROTECTED);
        ld_.updated = true;
    }

    uintptr_t get_mutex_id() const { return uintptr_t(mutex_); }

    bool is_state(LockState st) const { return ld_.is_state(st); }
    bool is_state_in(std::initializer_list<LockState> st_list) const { return ld_.is_state_in(st_list); }

private:
    void swap(Lock& rhs) noexcept {
        std::swap(mutex_, rhs.mutex_);
        std::swap(ld_, rhs.ld_);
    }
};


class LockSet
{
public:
    //using Lock = Lock;
    //using Mutex = Mutex;
    using OpEntryL = OpEntry<Lock>;
    using Vec = std::vector<OpEntryL>;
    using UMap = SingleThreadUnorderedMap<uintptr_t, size_t>;

private:
    Vec vec_;
    MemoryVector local_;

    // key: mutex pointer. value: index in vec_.
    UMap index_;

    uint32_t ord_id_;
    size_t value_size_;

public:
    // You must call this method at first.
    void init(size_t value_size, size_t nr_reserve) {
        value_size_ = value_size;
        if (value_size == 0) value_size++;
        local_.setSizes(value_size);

        vec_.reserve(nr_reserve);
        local_.reserve(nr_reserve);
    }
    void set_ord_id(uint32_t ord_id) {
        ord_id_ = ord_id;
    }

    enum ReadType : uint8_t {
        OPTIMISTIC = 0,
        READ_RESERVE = 1,
        WRITE_RESERVE = 2,
    };
    template <ReadType read_type>
    INLINE bool read_detail(Mutex& mutex, const void* shared_val, void* dst) {
        unused(shared_val, dst);
        const uintptr_t key = uintptr_t(&mutex);
        typename Vec::iterator it = find_entry(key);
        if (it == vec_.end()) {
            vec_.emplace_back(Lock(mutex, ord_id_));
            OpEntryL& ope = vec_.back();
            ope.info.set(allocate_local_val(), (void*)shared_val);
            Lock& lk = ope.lock;
            void* local_val = get_local_val_ptr(ope.info);
            if (read_type == OPTIMISTIC) {
                lk.invisible_read(shared_val, local_val, value_size_);
            } else {
                constexpr bool do_write_reserve = (read_type == WRITE_RESERVE);
#if 1
                lk.read_and_reserve<do_write_reserve>(shared_val, local_val, value_size_);
#else
                lk.read_and_reserve2<do_write_reserve>(shared_val, local_val, value_size_);
#endif
            }
            copy_value(dst, local_val);
            return true;
        }
#if 1  // early verify or keep reservation if required.
        Lock& lk = it->lock;
        if (lk.is_state(LockState::READ)) {
            if (read_type == OPTIMISTIC) {
                if (!lk.is_unchanged(true)) return false;
            } else {
                if (!lk.try_keep_reservation<LockState::READ>()) return false;
            }
        } else if (lk.is_state(LockState::READ_MODIFY_WRITE)) {
            if (!lk.try_keep_reservation<LockState::READ_MODIFY_WRITE>()) return false;
        } else {
            // do nothing.
        }
#endif
        copy_value(dst, get_local_val_ptr(it->info));
        return true;
    }
    INLINE bool optimistic_read(Mutex& mutex, const void* shared_val, void* dst) {
        return read_detail<OPTIMISTIC>(mutex, shared_val, dst);
    }
    INLINE bool pessimistic_read(Mutex& mutex, const void* shared_val, void* dst) {
        return read_detail<READ_RESERVE>(mutex, shared_val, dst);
    }
    INLINE bool read_for_update(Mutex& mutex, const void* shared_val, void* dst) {
        return read_detail<WRITE_RESERVE>(mutex, shared_val, dst);
    }
    INLINE bool write(Mutex& mutex, void *shared_val, const void *src) {
        unused(shared_val, src);
        const uintptr_t key = uintptr_t(&mutex);
        typename Vec::iterator it = find_entry(key);
        if (it == vec_.end()) {
            vec_.emplace_back(Lock(mutex, ord_id_));
            OpEntryL& ope = vec_.back();
            Lock& lk = ope.lock;
            lk.blind_write();
            ope.info.set(allocate_local_val(), shared_val);
            copy_value(get_local_val_ptr(ope.info), src);
            return true;
        }
        Lock& lk = it->lock;
        if (!lk.upgrade_if_necessary()) return false;
        copy_value(get_local_val_ptr(it->info), src);
        return true;
    }

    /*
     * Pre-commit phase:
     * (1) call reserve_all_blind_writes().
     * (2) call protect_all().
     * (3) call verify_and_unlock().
     * (4) call update_and_unlock().
     *
     * The point between protect_all() and verify_and_unlock() is serialization point.
     * The point between verify_and_unlock() and update_and_unlock() is strictness point.
     */

    INLINE void reserve_all_blind_writes() {
        for (OpEntryL& ope : vec_) {
            Lock& lk = ope.lock;
            if (lk.is_state(LockState::PRE_BLIND_WRITE)) {
                lk.blind_write_reserve();
            }
        }
    }
    /**
     * Here all the reserved write (BLIND_WRITE and READ_MODIFY_WRITE) is unprotected (if not intercepted).
     * For BLIND_WRITE, reserve_all_blind_writes() ensures it.
     * For READ_MODIFY_WRITE, upgrade() and read_and_reserve<true>() ensure it.
     */
    INLINE bool protect_all() {
        for (OpEntryL& ope : vec_) {
            Lock& lk = ope.lock;
            if (lk.is_state(LockState::BLIND_WRITE)) {
                if (!lk.protect<LockState::BLIND_WRITE>()) return false;
            } else if (lk.is_state(LockState::READ_MODIFY_WRITE)) {
                if (!lk.protect<LockState::READ_MODIFY_WRITE>()) return false;
            } else {
                assert(lk.is_state(LockState::READ));
            }
        }
        SERIALIZATION_POINT_BARRIER();
        return true;
    }
    INLINE bool verify_and_unlock() {
        for (OpEntryL& ope : vec_) {
            Lock& lk = ope.lock;
            // read-modify-write entries have been checked in protect_all() already.
            if (lk.is_state(LockState::READ)) {
                if (!lk.is_unchanged()) return false;
                // S2PL allows unlocking of read locks here.
                lk.unlock();
            }
        }
        return true;
    }
    INLINE void update_and_unlock() {
        for (OpEntryL& ope : vec_) {
            Lock& lk = ope.lock;
            if (lk.is_state(LockState::PROTECTED)) {
                lk.update();
                // writeback.
                copy_value(ope.info.sharedVal, get_local_val_ptr(ope.info));
                lk.unlock();
            }
        }
        clear();
    }
    INLINE void clear() {
        index_.clear();
        vec_.clear();
        local_.clear();
    }
    INLINE bool is_empty() const { return vec_.empty(); }

private:
    INLINE typename Vec::iterator find_entry(uintptr_t key) {
        // at most 4KiB scan.
        const size_t threshold = 4096 / sizeof(OpEntryL);
        if (vec_.size() > threshold) {
            // create indexes.
            for (size_t i = index_.size(); i < vec_.size(); i++) {
                index_[vec_[i].lock.get_mutex_id()] = i;
            }
            // use indexes.
            UMap::iterator it = index_.find(key);
            if (it == index_.end()) {
                return vec_.end();
            } else {
                size_t idx = it->second;
                return vec_.begin() + idx;
            }
        }
        // linear search.
        return std::find_if(
            vec_.begin(), vec_.end(),
            [&](const OpEntryL& ope) {
                return ope.lock.get_mutex_id() == key;
            });
    }
    INLINE void* get_local_val_ptr(const LocalValInfo& info) {
#ifndef NO_PAYLOAD
        return (info.localValIdx == UINT64_MAX ? nullptr : &local_[info.localValIdx]);
#else
        unused(info);
        return nullptr;
#endif
    }
    INLINE size_t allocate_local_val() {
#ifndef NO_PAYLOAD
        const size_t idx = local_.size();
        local_.resize(idx + 1);
        return idx;
#else
        return 0;
#endif
    }

    INLINE void copy_value(void* dst, const void* src) {
#ifndef NO_PAYLOAD
        ::memcpy(dst, src, value_size_);
#else
        unused(dst, src);
#endif
    }
};


} // namespace cas


/**
 * Starvation-free version.
 * atomic exchange is used like MCS locking protocol.
 */
namespace mcs {


enum RequestType : uint8_t
{
    RESERVE_FOR_READ = 0,
    RESERVE_FOR_WRITE = 1,
    PROTECT = 2,
    UNRESERVE_FOR_READ = 3,
    UNRESERVE_FOR_WRITE = 4,
    UNPROTECT = 5,
};

enum Response : uint8_t
{
    // Internally used.
    WAITING = 0,
    OWNER = 1,
    SHOULD_WAIT = 2,

    // Messages that means the request has been processed.
    SUCCESSED = 3,
    FAILED = 4,
};


/**
 *
 */
struct Request
{
    alignas(CACHE_LINE_SIZE)
    Request* next;  // for linked list as a request queue.

    RequestType type;
    uint32_t ord_id; // requestor's ord_id.
    Response res; // response from the owner.

    /**
     * CAUSION: uninitialized. call init() to initialize the object.
     */
    Request() = default;

    void init(RequestType type0, uint32_t ord_id0) {
        next = nullptr;
        type = type0;
        ord_id = ord_id0;
        res = WAITING;
        release_memory_barrier();
    }

    Request(const Request& rhs) = delete;
    Request& operator=(const Request& rhs) = delete;

    Request(Request&& rhs) noexcept : Request() { swap(rhs); }
    Request&& operator=(Request&& rhs) noexcept { swap(rhs); return *this; }

    Response local_spin_wait() {
        Response res0;
        while ((res0 = load_acquire(res)) == WAITING) _mm_pause();
        store_release(res, WAITING);
        return res0;
    }

private:
    void swap(Request& rhs) noexcept {
        std::swap(next, rhs.next);
        std::swap(type, rhs.type);
        std::swap(ord_id, rhs.ord_id);
        std::swap(res, rhs.res);

        // QQQ
    }

    // QQQQQ
};


/**
 * These are tags stored in the tail pointer.
 * They never be the pointer value.
 */
Request* const UNOWNED = (Request*)0;
Request* const OWNED = (Request*)1;


/**
 * using MCS-like Lock Template.
 */
class Lock
{



    // QQQ
};


class Mutex
{
    alignas(sizeof(uintptr_t))
    Request* tail_;
    Request* head_;
    Request* waiting_;
    MutexData md_;
public:
    Mutex2() : tail_(UNOWNED), head_(nullptr), waiting_(nullptr), md_() {
        md_.init();
    }

    /**
     * RETURN:
     *   OWNER
     */
    Response enqueue(Request& req) {
        Request* prev = exchange(tail_, &req);
        if (prev == UNOWNED) {
            return OWNER;
        }
        if (prev == OWNED) {
            store_release(head_, &req);
            Response res = req.local_spin_wait();
            unused(res); assert(res == OWNER);
            return OWNER;
        }
        // prev is the pointer of the previous request.
        store_release(prev->next, &req);
        req.local_spin_wait();

    }







    /**
     * This is thread-safe code.
     */
    template <typename Func>
    void do_request(Request& req, Func&& owner_task) {
        Request* prev = exchange(tail_, &req);
        if (prev == UNOWNED) {
            if (do_owner_task(std::forward<Func>(owner_task))) {
                msg = local_spin_wait();

            }
            // do owner task.


        } else if (prev == OWNED) {
            store_release(head_, &req);









        // QQQQQ
    }
private:
    /**
     * This is not thread-safe code.
     * RETURN:
     *   true if the worker should wait.
     */
    template <typename Func>
    bool do_owner_task(Func&& owner_task) {





        // QQQQQ
    }

    Message local_spin_wait() {


    }


    // QQQQQ
};





/**
 *
 */
class Lock
{
private:
    Mutex* mutex_;
    LockData ld_;

    alignas(CACHE_LINE_SIZE)
    Request req_;

    // QQQQQ


public:
    INLINE Lock() : mutex_(nullptr), ld_(), req_() {
    }
    INLINE Lock(Mutex& mutex, uint32_t ord_id) : mutex_(&mutex), ld_(ord_id), req_() {
    }
    INLINE ~Lock() {
        unlock();
    }

    void init(Mutex& mutex, uint32_t ord_id) {
        mutex_ = &mutex;
        ld_.init(ord_id);
    }

    // copy constructor/assign operator are removed.
    Lock(const Lock&) = delete;
    Lock& operator=(const Lock&) = delete;

    // move constructor/assign operator.
    Lock(Lock&& rhs) noexcept : Lock() { swap(rhs); }
    Lock& operator=(Lock&& rhs) noexcept { swap(rhs); return *this; }






private:
    /**
     * CAUSION: req_ object is not swappable during in request queue.
     */
    void swap(Lock& rhs) noexcept {
        std::swap(mutex_, rhs.mutex_);
        std::swap(ld_, rhs.ld_);
        std::swap(req_, rhs.req_);
    }





    // QQQ

};


/**
 *
 */
class LockSet2
{


    // QQQ
};


} // namespace mcs

} // namespace licc2
} // namespace lock
} // namespace cybozu
