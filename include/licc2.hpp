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
#include "cache_line_size.hpp"
#include "list_util.hpp"
#include "mcslikelock.hpp"


namespace cybozu {
namespace lock {
namespace licc2 {


const uint32_t MAX_ORD_ID = UINT32_MAX;


/**
 * Mutex data.
 */
struct MutexData
{
    // To avoid the BUG: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=90511
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
    alignas(sizeof(uint64_t))
    union {
        uint64_t obj;
        struct {
            union {
                uint32_t ord_id;
                struct {
                    // lower bits on little endian architecture.
                    uint32_t worker_id:10;
                    uint32_t epoch_id:22;
                };
            };
            union {
                uint32_t state;
                struct {
                    uint32_t version:30;
                    uint32_t protected_:1;
                    uint32_t is_writer:1;
                };
            };
        };
    };
#pragma GCC diagnostic pop

    INLINE MutexData() = default; // CAUSION: not initialized

    INLINE MutexData(uint64_t x) noexcept : MutexData() { obj = x; }
    INLINE operator uint64_t() const { return obj; }

    INLINE void init() {
        ord_id = MAX_ORD_ID;
        state = 0;
    }
    std::string str() const {
        return fmtstr("MutexData{ord:%x worker:%x epoch:%x ver:%u protected:%u is_writer:%u}"
                      , ord_id, worker_id, epoch_id, version, protected_, is_writer);
    }

    template <bool allow_protected = false>
    INLINE bool is_valid(uint32_t version0) const {
        return (allow_protected || !protected_) && version == version0;
    }

    INLINE bool is_unreserved() const {
        return ord_id == MAX_ORD_ID;
    }
    INLINE bool is_shared() const {
        return ord_id != MAX_ORD_ID && !is_writer;
    }
    INLINE bool is_unreserved_or_shared() const {
        return ord_id == MAX_ORD_ID || !is_writer;
    }
    INLINE bool can_intercept(uint32_t ord_id0) const {
        // equality is required to be included for reserving-again.
        return ord_id0 <= ord_id;
    }
    INLINE bool can_read_reserve(uint32_t ord_id0) const {
        return is_unreserved_or_shared() || can_intercept(ord_id0);
    }
    INLINE bool can_write_reserve(uint32_t ord_id0) const {
        return is_unreserved() || can_intercept(ord_id0);
    }
    INLINE bool can_read_reserve_without_changing(uint32_t ord_id0) const {
        return !protected_ && !is_writer && ord_id < ord_id0;
    }
    INLINE void prepare_read_reserve(uint32_t ord_id0) {
        is_writer = 0;
        ord_id = std::min(ord_id, ord_id0);
    }
    INLINE void prepare_write_reserve(uint32_t ord_id0) {
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


template <LockState to_state>
constexpr void assert_from_lock_states(LockState from_state)
{
    unused(from_state);
    if (to_state == LockState::READ)  {
        assert(is_lock_state_in(from_state, {LockState::INIT, LockState::READ}));
    } else if (to_state == LockState::READ_MODIFY_WRITE) {
        assert(is_lock_state_in(from_state, {LockState::INIT, LockState::READ, LockState::READ_MODIFY_WRITE}));
    } else if (to_state == LockState::BLIND_WRITE) {
        assert(is_lock_state_in(from_state, {LockState::PRE_BLIND_WRITE, LockState::BLIND_WRITE}));
    } else {
        BUG();
    }
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
    INLINE LockData() = default; // CAUSION: not initialied.
#else
    INLINE LockData() : state(LockState::INIT), updated(false), ord_id(MAX_ORD_ID), version(0) {
    }
#endif
    INLINE explicit LockData(uint32_t ord_id0) : LockData() { init(ord_id0); }

    INLINE void init(uint32_t ord_id0) {
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

    INLINE bool is_state(LockState st) const {
        return st == state;
    }
    INLINE bool is_state_in(std::initializer_list<LockState> st_list) const {
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

    INLINE MutexOpCreator() = default; // CAUSION. not initialized.

#if 0
    // MUST_WAIT or IMPOSSIBLE.
    INLINE explicit MutexOpCreator(MutexOpCapability res) : capability(res), ld(), md() {
        assert(res != POSSIBLE);
    }
#endif

    // Valid initial value.
    INLINE MutexOpCreator(LockData ld0, MutexData md0)
        : capability(POSSIBLE), ld(ld0), md(md0) {
    }

    INLINE explicit operator bool() const { return possible(); }
    INLINE bool possible() const { return capability == POSSIBLE; }

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
#if 1
    INLINE MutexOpCreator invisible_read() const {
        MutexOpCreator moc(*this);
        LockData& ld = moc.ld; MutexData& md = moc.md;
        assert(ld.state == LockState::INIT);
        ld.state = LockState::READ;
        ld.version = md.version;
        return moc;
    }
#endif
#if 1
    /**
     * No need to change mutex object.
     */
    INLINE MutexOpCreator blind_write() const {
        MutexOpCreator moc(*this);
        LockData& ld = moc.ld;
        assert(ld.state == LockState::INIT);
        ld.state = LockState::PRE_BLIND_WRITE;
        return moc;
    }
#endif

    template <LockState to_state, bool checks_version>
    INLINE MutexOpCreator reserve() const {
        static_assert(is_lock_state_in(to_state, {LockState::READ, LockState::READ_MODIFY_WRITE, LockState::BLIND_WRITE}));
        MutexOpCreator moc(*this);
        if (!moc) return moc;
        LockData& ld = moc.ld; MutexData& md = moc.md;
        assert_from_lock_states<to_state>(ld.state);
        if (checks_version && (md.version != ld.version || md.protected_)) {
            moc.capability = IMPOSSIBLE;
            return moc;
        }
        bool can_reserve = (to_state == LockState::READ)
            ? md.can_read_reserve(ld.ord_id)
            : md.can_write_reserve(ld.ord_id);
        if (!can_reserve || md.protected_) {
            moc.capability = MUST_WAIT;
            return moc;
        }
        if (to_state == LockState::READ) {
            md.prepare_read_reserve(ld.ord_id);
        } else {
            md.prepare_write_reserve(ld.ord_id);
        }
        ld.state = to_state;
        ld.version = md.version;
        return moc;
    }
    template <bool checks_version>
    INLINE MutexOpCreator protect() const {
        constexpr LockState from_state = checks_version ? LockState::READ_MODIFY_WRITE : LockState::BLIND_WRITE;
        unused(from_state);
        MutexOpCreator moc(*this);
        if (!moc) return moc;
        LockData& ld = moc.ld; MutexData& md = moc.md;
        assert(ld.state == from_state);
        if ((checks_version && ld.version != md.version)
            || ld.ord_id != md.ord_id
            || md.protected_) {
            moc.capability = IMPOSSIBLE;
            return moc;
        }
        // If the lock object is read reserved,
        // we consider the upgrade reservation will be processed at the same time.
        ld.state = LockState::PROTECTED;
        md.ord_id = MAX_ORD_ID;
        md.protected_ = 1;
        return moc;
    }
    template <LockState from_state>
    INLINE MutexOpCreator unlock_special() const {
        MutexOpCreator moc(*this);
        if (!moc) return moc;
        LockData& ld = moc.ld; MutexData& md = moc.md;
        assert(ld.state == from_state);
        switch (from_state) {
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
            BUG();
        }
        ld.state = LockState::INIT;
        return moc;
    }
    INLINE MutexOpCreator unlock_general() const {
        switch (ld.state) {
        case LockState::INIT:
            return unlock_special<LockState::INIT>();
        case LockState::READ:
            return unlock_special<LockState::READ>();
        case LockState::PRE_BLIND_WRITE:
            return unlock_special<LockState::PRE_BLIND_WRITE>();
        case LockState::BLIND_WRITE:
            return unlock_special<LockState::BLIND_WRITE>();
        case LockState::READ_MODIFY_WRITE:
            return unlock_special<LockState::READ_MODIFY_WRITE>();
        case LockState::PROTECTED:
            return unlock_special<LockState::PROTECTED>();
        default:
            BUG();
        }
    }
};


/**
 * Mutex must have load() member function.
 */
template <typename Mutex>
INLINE void invisible_read(Mutex& mutex, LockData& ld, const void* shared, void* local, size_t size)
{
    unused(shared, local, size);
    MutexData md0 = mutex.load();
    for (;;) {
        _mm_pause();
        if (md0.protected_) {
            md0 = mutex.load();
            continue;
        }
#ifndef NO_PAYLOAD
        ::memcpy(local, shared, size);
#endif
        acquire_fence();
        MutexData md1 = mutex.load();
        if (!md1.is_valid(md0.version)) {
            md0 = md1;
            continue;
        }
        ld.version = md0.version;
        ld.state = LockState::READ;
        return;
    }
}


/**
 * Simple CAS-only locking.
 * Starfation-freeness depends on possibility of occurrence of starvation on CAS operations.
 */
namespace cas {


struct Mutex
{
    MutexData md;

    INLINE Mutex() : md() { md.init(); }

    INLINE MutexData load() const { return load_acquire(md.obj); }
    INLINE void store(MutexData md0) { store_release(md.obj, md0.obj); }
    INLINE bool cas_acq_rel(MutexData& md0, MutexData md1) {
        return compare_exchange(md.obj, md0.obj, md1.obj);
    }
    INLINE bool cas_acq(MutexData& md0, MutexData md1) {
        return compare_exchange_acquire(md.obj, md0.obj, md1.obj);
    }
    INLINE bool cas_rel(MutexData& md0, MutexData md1) {
        return compare_exchange_release(md.obj, md0.obj, md1.obj);
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
        unlock_general();
    }

    INLINE void init(Mutex& mutex, uint32_t ord_id) {
        mutex_ = &mutex;
        ld_.init(ord_id);
    }

    // copy constructor/assign operator are removed.
    Lock(const Lock&) = delete;
    Lock& operator=(const Lock&) = delete;

    // move constructor/assign operator.
    INLINE Lock(Lock&& rhs) noexcept : Lock() { swap(rhs); }
    INLINE Lock& operator=(Lock&& rhs) noexcept { swap(rhs); return *this; }

    INLINE void swap(Lock& rhs) noexcept {
        std::swap(mutex_, rhs.mutex_);
        std::swap(ld_, rhs.ld_);
    }

    /**
     * INIT --> READ.
     */
    INLINE void invisible_read(const void *shared, void *local, size_t size) {
        licc2::invisible_read(*mutex_, ld_, shared, local, size);
    }
    /**
     * WRITE_RESERVE = true
     *   INIT --> READ_MODIFY_WRITE
     * otherwise
     *   INIT --> READ
     */
    template <bool does_write_reserve>
    INLINE void read_and_reserve_detail(const void *shared, void *local, size_t size) {
        unused(shared, local, size);
        constexpr LockState to_state = does_write_reserve
            ? LockState::READ_MODIFY_WRITE
            : LockState::READ;
        MutexData md0 = mutex_->load();
        for (;;) {
            _mm_pause();
            auto moc1 = MutexOpCreator(ld_, md0).reserve<to_state, false>();
            if (moc1.capability == MUST_WAIT) {
                md0 = mutex_->load();
                continue;
            }
            assert(moc1.capability == POSSIBLE);
            assert(!moc1.md.protected_);
#ifndef NO_PAYLOAD
            ::memcpy(local, shared, size);
#endif
            acquire_fence();
            if (!does_write_reserve && md0 == moc1.md) {
                // CAS is not required but verify is required.
                MutexData md1 = mutex_->load();
                if (md1.is_valid(md0.version)) {
                    ld_ = moc1.ld;
                    return;
                }
                md0 = md1;
                continue;
            }
            if (mutex_->cas_acq(md0, moc1.md)) {
                ld_ = moc1.ld;
                return;
            }
            // CAS failed and continue.
        }
    }
    INLINE void read_and_reserve(const void* shared, void* local, size_t size) {
        read_and_reserve_detail<false>(shared, local, size);
    }
    INLINE void read_for_update(const void* shared, void* local, size_t size) {
        read_and_reserve_detail<true>(shared, local, size);
    }
    template <bool does_write_reserve, bool is_retry>
    INLINE MutexData reserve_for_read(MutexData md0) {
        constexpr LockState to_state = does_write_reserve
            ? LockState::READ_MODIFY_WRITE
            : LockState::READ;
        for (;;) {
            _mm_pause();
            auto moc1 = MutexOpCreator(ld_, md0).reserve<to_state, false>();
            if (moc1.capability == MUST_WAIT) {
                md0 = mutex_->load();
                continue;
            }
            assert(moc1.capability == POSSIBLE);
            assert(!moc1.md.protected_);
            if ((!does_write_reserve && md0 == moc1.md) || mutex_->cas_acq(md0, moc1.md)) {
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
    template <bool does_write_reserve = false>
    INLINE void read_and_reserve2(const void* shared, void* local, size_t size) {
        unused(shared, local, size);
        MutexData md0 = mutex_->load();
        md0 = reserve_for_read<does_write_reserve, false>(md0);
        for (;;) {
            _mm_pause();
#ifndef NO_PAYLOAD
            ::memcpy(local, shared, size);
#endif
            acquire_fence();
            MutexData md1 = mutex_->load();
            if (md1.is_valid(md0.version)) return;
            md0 = reserve_for_read<does_write_reserve, true>(md1);
        }
    }
    /**
     * Try to keep reservation for READ and READ_MODIFY_WRITE state.
     * This checks version unchanged.
     */
    template <LockState lock_state>
    INLINE bool try_keep_reservation() {
        static_assert(is_lock_state_in(lock_state, {LockState::READ, LockState::READ_MODIFY_WRITE}));
        assert(ld_.state == lock_state);
        MutexData md0 = mutex_->load();
        for (;;) {
            auto moc1 = MutexOpCreator(ld_, md0).reserve<lock_state, true>();
            if (moc1.capability == IMPOSSIBLE) return false;
            if (moc1.capability == MUST_WAIT) {
                md0 = mutex_->load();
                continue;
            }
            assert(moc1.capability == POSSIBLE);
            if (md0 == moc1.md || mutex_->cas_acq(md0, moc1.md)) {
                ld_ = moc1.ld;
                return true;
            }
            // CAS failed and continue.
        }
    }
    INLINE void blind_write() {
        assert(ld_.state == LockState::INIT);
        ld_.state = LockState::PRE_BLIND_WRITE;
    }
    INLINE void reserve_for_blind_write() {
        assert(ld_.state == LockState::PRE_BLIND_WRITE);
        MutexData md0 = mutex_->load();
        for (;;) {
            _mm_pause();
            auto moc1 = MutexOpCreator(ld_, md0).reserve<LockState::BLIND_WRITE, false>();
            if (moc1.capability == MUST_WAIT) {
                md0 = mutex_->load();
                continue;
            }
            assert(moc1.capability == POSSIBLE);
            assert(md0 != moc1.md);
            if (mutex_->cas_acq(md0, moc1.md)) {
                ld_ = moc1.ld;
                return;
            }
            // CAS failed and continue.
        }
    }
    INLINE bool upgrade() {
        assert(ld_.is_state(LockState::READ));
        MutexData md0 = mutex_->load();
        for (;;) {
            _mm_pause();
            auto moc1 = MutexOpCreator(ld_, md0).reserve<LockState::READ_MODIFY_WRITE, true>();
            if (moc1.capability == IMPOSSIBLE) return false;
            if (moc1.capability == MUST_WAIT) {
                md0 = mutex_->load();
                continue;
            }
            assert(moc1.capability == POSSIBLE);
            assert(md0 != moc1.md);
            if (mutex_->cas_acq(md0, moc1.md)) {
                ld_ = moc1.ld;
                return true;
            }
            // CAS failed and continue.
        }
    }
    template <bool checks_version>
    INLINE bool protect() {
        constexpr LockState to_state = checks_version ? LockState::READ_MODIFY_WRITE : LockState::BLIND_WRITE;
        assert(ld_.state == to_state);
        MutexData md0 = mutex_->load();
        for (;;) {
            _mm_pause();
            auto moc1 = MutexOpCreator(ld_, md0)
                .reserve<to_state, checks_version>()
                .template protect<checks_version>();
            if (!moc1.possible()) return false;
            if (mutex_->cas_acq_rel(md0, moc1.md)) {
                ld_ = moc1.ld;
                return true;
            }
            // CAS failed and continue.
        }
    }
    INLINE void unlock_general() noexcept {
        if (mutex_ == nullptr) return;
        if (ld_.is_state_in({LockState::INIT, LockState::PRE_BLIND_WRITE})) {
            mutex_ = nullptr;
            return;
        }
        MutexData md0 = mutex_->load();
        if (ld_.is_state_in({LockState::READ, LockState::BLIND_WRITE, LockState::READ_MODIFY_WRITE}) &&
            ld_.ord_id != md0.ord_id) {
            mutex_ = nullptr;
            return;
        }
        for (;;) {
            _mm_pause();
            auto moc1 = MutexOpCreator(ld_, md0).unlock_general();
            assert(moc1.possible());
            if (md0 == moc1.md || mutex_->cas_rel(md0, moc1.md)) {
                ld_ = moc1.ld;
                mutex_ = nullptr;
                return;
            }
            // CAS failed and continue.
        }
    }
    template <LockState from_state>
    INLINE void unlock_special() {
        assert(mutex_ != nullptr);
        if (is_lock_state_in(from_state, {LockState::INIT, LockState::PRE_BLIND_WRITE})) {
            mutex_ = nullptr;
            return;
        }
        MutexData md0 = mutex_->load();
        if (is_lock_state_in(from_state, {LockState::READ, LockState::BLIND_WRITE, LockState::READ_MODIFY_WRITE}) &&
            ld_.ord_id != md0.ord_id) {
            mutex_ = nullptr;
            return;
        }
        for (;;) {
            _mm_pause();
            auto moc1 = MutexOpCreator(ld_, md0).unlock_special<from_state>();
            assert(moc1.possible());
            if (md0 == moc1.md || mutex_->cas_rel(md0, moc1.md)) {
                ld_ = moc1.ld;
                mutex_ = nullptr;
                return;
            }
            // CAS failed and continue.
        }
    }
    template <bool allow_protected = false>
    INLINE bool is_unchanged() const {
        assert(mutex_);
        return mutex_->load().is_valid<allow_protected>(ld_.version);
    }
    INLINE void update() {
        assert(ld_.state == LockState::PROTECTED);
        ld_.updated = true;
    }

    INLINE uintptr_t get_mutex_id() const { return uintptr_t(mutex_); }

    INLINE bool is_state(LockState st) const { return ld_.is_state(st); }
    INLINE bool is_state_in(std::initializer_list<LockState> st_list) const { return ld_.is_state_in(st_list); }
};


} // namespace cas


/**
 * Starvation-free version.
 * atomic exchange is used like MCS locking protocol.
 */
namespace mcs {


enum class RequestType : uint8_t
{
    READ = 0,
    BLIND_WRITE = 1,
    READ_MODIFY_WRITE = 2,
    PROTECT = 3,
    UNLOCK = 4,
};


INLINE bool is_request_type_in(RequestType type, std::initializer_list<RequestType> type_list)
{
    for (RequestType type0 : type_list) {
        if (type == type0) return true;
    }
    return false;
}


/**
 * Initial state is WAITING.
 * The owner will set the value OWNER or DONE.
 * OWNER means the request must be the owner.
 * DONE means the request is done.
 */
enum Message : uint8_t
{
    WAITING = 0,
    OWNER = 1,
    DONE = 2,
};


/**
 * Request to change a mutex state.
 */
struct Request
{
    using Message = Message;

    alignas(CACHE_LINE_SIZE)
    Request* next;  // for linked list as a request queue.

    RequestType type;
    bool checks_version; // If true, the request checks that version is unchanged.
    Message msg; // message from the owner. internally used.
    bool succeeded; // result of the request.
    uint32_t ord_id; // requestor's ord_id.

    // Current lock data should be set before enquing the request.
    // If the request is successfully done, new lockdata will be set.
    // Only the owner can access this member.
    LockData ld;

    /**
     * CAUSION: uninitialized. call init() to initialize the object.
     */
    INLINE Request() = default;

    INLINE void init(RequestType type0, LockData ld0, bool checks_version0) {
        next = nullptr;
        type = type0;
        checks_version = checks_version0;
        ord_id = ld0.ord_id;
        msg = WAITING;
        succeeded = false;
        ld = ld0;
    }

    Request(const Request& rhs) = delete;
    Request& operator=(const Request& rhs) = delete;

    INLINE Request(Request&& rhs) noexcept : Request() { swap(rhs); }
    INLINE Request& operator=(Request&& rhs) noexcept { swap(rhs); return *this; }

    INLINE void swap(Request& rhs) noexcept {
        std::swap(next, rhs.next);
        std::swap(type, rhs.type);
        std::swap(checks_version, rhs.checks_version);
        std::swap(msg, rhs.msg);
        std::swap(succeeded, rhs.succeeded);
        std::swap(ord_id, rhs.ord_id);
        std::swap(ld, rhs.ld);
    }

    INLINE bool operator<(const Request& rhs) const noexcept { return ord_id < rhs.ord_id; }

    INLINE Request* get_next_ptr() {
        Request* next0;
        while ((next0 = load(next)) == nullptr) _mm_pause();
        return next0;
    }
    INLINE void notify(Message msg0) { store_release(msg, msg0); }

    INLINE void set_next(Request* next0) { store_release(next, next0); }
    INLINE void delegate_ownership() { notify(OWNER); }
    INLINE void wait_for_ownership() {
        Message msg0 = local_spin_wait();
        unused(msg0); assert(msg0 == OWNER);
    }
    INLINE Message local_spin_wait() {
        Message msg0;
        while ((msg0 = load_acquire(msg)) == WAITING) _mm_pause();
        store(msg, WAITING);
        return msg0;
    }
};


using ReqList = NodeListT<Request>;



class Mutex
{
    alignas(sizeof(uintptr_t))
    uintptr_t tail_;
    // alignas(CACHE_LINE_SIZE)
    Request* head_;
    ReqList waiting_;
    MutexData md_;
public:
    INLINE Mutex() : tail_(mcslike::UNOWNED), head_(nullptr), waiting_(), md_() {
        md_.init();
    }
    /**
     * Everyone can call load() to read a mutex state.
     */
    INLINE MutexData load() const { return load_acquire(md_); }
    /**
     * Store operations are allowed only to the owner.
     */
    INLINE void store(MutexData md) { store_release(md_, md); }

#if 0
    // cas is not used because md modifying operations will be done through the request queue only.
    INLINE bool cas(MutexData& md0, MutexData md1) { return compare_exchange(md_, md0, md1); }
#endif

    INLINE bool do_request(Request& req) {
        Message msg = mcslike::do_request_sync(
            req, tail_, head_, [&](Request& tail) { owner_task(req, tail); });
        assert(msg == DONE); unused(msg);
        return req.succeeded;
    }

private:
    INLINE void owner_task(Request& head, Request& tail) {
        ReqList protect_list;
        MutexData md0 = load();
        bool version_changed = owner_process_unlock_requests(&head, &tail, protect_list, waiting_, md0);
        owner_process_protect_requests(protect_list, md0);
        owner_process_reserve_requests(waiting_, md0);
        if (version_changed) owner_fail_checking_version_requests(waiting_);
    }
    /**
     * process unlock (unreserve and unprotect) operations.
     * head and tail is included.
     * RETURN:
     *   true if version was updated.
     *   false if version was unchanged.
     */
    INLINE bool owner_process_unlock_requests(Request* head, Request* tail, ReqList& protect_list, ReqList& waiting_list, MutexData& md0) {
        bool version_changed = false;
        Request* req = head;
        while (req != nullptr) {
            Request* next = (req == tail ? nullptr : req->get_next_ptr());

            // The following insert operations breaks req->next pointer.
            if (req->type == RequestType::PROTECT) {
                insert_sort<Request>(protect_list, req);
            } else if (req->type == RequestType::UNLOCK) {
                version_changed |= owner_process_one_unlock_request(*req, md0);
                req->notify(DONE); // DO NOT touch the request from now.
            } else {
                insert_sort<Request>(waiting_list, req);
            }
            req = next;
        }
        return version_changed;
    }
    INLINE bool owner_process_one_unlock_request(Request& req, MutexData& md0) {
        auto moc1 = MutexOpCreator(req.ld, md0).unlock_general();
        assert(moc1.possible());
        store(moc1.md);
        md0 = moc1.md;
        req.ld = moc1.ld;
        ::store(req.succeeded, true);
        return moc1.md.version != md0.version;
    }
    INLINE void owner_process_protect_requests(ReqList& protect_list, MutexData& md0) {
        while (!protect_list.empty()) {
            Request& req = *protect_list.front();
            owner_process_one_protect_request(req, md0);
            protect_list.pop_front(); // do this before notification.
            req.notify(DONE);
        }
    }
    INLINE void owner_process_one_protect_request(Request& req, MutexData& md0) {
        assert(::load(req.type) == RequestType::PROTECT);
        MutexOpCreator moc0(req.ld, md0);
        MutexOpCreator moc1;
        if (::load(req.checks_version)) moc1 = moc0.reserve<LockState::READ_MODIFY_WRITE, true>().protect<true>();
        else moc1 = moc0.reserve<LockState::BLIND_WRITE, false>().protect<false>();
        if (!moc1.possible()) {
            ::store(req.succeeded, false);
            return;
        }
        store(moc1.md);
        md0 = moc1.md;
        req.ld = moc1.ld;
        ::store(req.succeeded, true);
    }
    INLINE void owner_process_reserve_requests(ReqList& waiting_list, MutexData& md0) {
        while (!waiting_list.empty()) {
            Request& req = *waiting_list.front();
            if (!owner_process_one_reserve_request(req, md0)) return;
            waiting_list.pop_front(); // do this before notification.
            req.notify(DONE);
        }
    }
    /**
     * RETURN:
     *   true if the request has been done (with success or failure).
     */
    INLINE bool owner_process_one_reserve_request(Request& req, MutexData& md0) {
        MutexOpCreator moc0(req.ld, md0);
        MutexOpCreator moc1;
        switch(::load(req.type)) {
        case RequestType::READ:
            assert(req.ld.state == LockState::INIT || req.ld.state == LockState::READ);
            if (::load(req.checks_version)) moc1 = moc0.reserve<LockState::READ, true>();
            else moc1 = moc0.reserve<LockState::READ, false>();
            break;
        case RequestType::BLIND_WRITE:
            assert(req.ld.state == LockState::PRE_BLIND_WRITE || req.ld.state == LockState::BLIND_WRITE);
            // do not use req.checks_version.
            moc1 = moc0.reserve<LockState::BLIND_WRITE, false>();
            break;
        case RequestType::READ_MODIFY_WRITE:
            assert(req.ld.state == LockState::INIT || req.ld.state == LockState::READ || req.ld.state == LockState::READ_MODIFY_WRITE);
            if (::load(req.checks_version)) moc1 = moc0.reserve<LockState::READ_MODIFY_WRITE, true>();
            else moc1 = moc0.reserve<LockState::READ_MODIFY_WRITE, false>();
            break;
        default:
            BUG();
        }
        if (moc1.capability == MUST_WAIT) {
            return false;
        }
        if (moc1.capability == IMPOSSIBLE) {
            ::store(req.succeeded, false);
            return true;
        }
        assert(moc1.possible());
        store(moc1.md);
        md0 = moc1.md;
        req.ld = moc1.ld;
        ::store(req.succeeded, true);
        return true;
    }
    /**
     * Notify requests with failure that checks version unchanged.
     */
    INLINE void owner_fail_checking_version_requests(ReqList& waiting_list) {
        ReqList tmp_list;
        while (!waiting_list.empty()) {
            Request& req = *waiting_list.front();
            waiting_list.pop_front();
            if (::load(req.checks_version)) {
                ::store(req.succeeded, false);
                req.notify(DONE);
            } else {
                tmp_list.push_back(&req);
            }
        }
        waiting_list = std::move(tmp_list);
    }
};


/**
 * using MCS-like Lock Template.
 */
class Lock
{
private:
    Mutex* mutex_;
    LockData ld_;
    Request req_;

public:
    INLINE Lock() : mutex_(nullptr), ld_(), req_() {
    }
    INLINE Lock(Mutex& mutex, uint32_t ord_id) : mutex_(&mutex), ld_(ord_id), req_() {
    }
    INLINE ~Lock() {
        unlock_general();
    }

    INLINE void init(Mutex& mutex, uint32_t ord_id) {
        mutex_ = &mutex;
        ld_.init(ord_id);
    }

    // copy constructor/assign operator are removed.
    Lock(const Lock&) = delete;
    Lock& operator=(const Lock&) = delete;

    // move constructor/assign operator.
    INLINE Lock(Lock&& rhs) noexcept : Lock() { swap(rhs); }
    INLINE Lock& operator=(Lock&& rhs) noexcept { swap(rhs); return *this; }

    /**
     * CAUSION: req_ object is not swappable during in request queue.
     */
    INLINE void swap(Lock& rhs) noexcept {
        std::swap(mutex_, rhs.mutex_);
        std::swap(ld_, rhs.ld_);
        std::swap(req_, rhs.req_);
    }


    INLINE void invisible_read(const void *shared, void *local, size_t size) {
        licc2::invisible_read(*mutex_, ld_, shared, local, size);
    }

    template <RequestType req_type>
    INLINE void read_and_reserve_detail(const void *shared, void *local, size_t size) {
        unused(shared, local, size);
        MutexData md0 = mutex_->load();
        for (;;) {
#if 1
            if (req_type == RequestType::READ && md0.can_read_reserve_without_changing(ld_.ord_id)) {
                // fast path.
                ld_.state = LockState::READ;
                ld_.version = md0.version;
            } else {
                // slow path.
                bool ret = do_request(req_type, false);
                unused(ret); assert(ret);
            }
#else
            bool ret = do_request(req_type, false);
            unused(ret); assert(ret);
#endif
#ifndef NO_PAYLOAD
            ::memcpy(local, shared, size);
#endif
            acquire_fence();
            md0 = mutex_->load();
            if (md0.is_valid(ld_.version)) return;
        }
    }
    INLINE void read_and_reserve(const void *shared, void *local, size_t size) {
        assert(ld_.is_state_in({LockState::INIT, LockState::READ}));
        read_and_reserve_detail<RequestType::READ>(shared, local, size);
    }
    INLINE void read_for_update(const void* shared, void* local, size_t size) {
        assert(ld_.is_state_in({LockState::INIT, LockState::READ_MODIFY_WRITE}));
        read_and_reserve_detail<RequestType::READ_MODIFY_WRITE>(shared, local, size);
    }
    template <LockState lock_state>
    INLINE bool try_keep_reservation() {
        static_assert(is_lock_state_in(lock_state, {LockState::READ, LockState::READ_MODIFY_WRITE}));
        assert(ld_.is_state(lock_state));
        constexpr RequestType req_type = (lock_state == LockState::READ ? RequestType::READ : RequestType::READ_MODIFY_WRITE);
        // fast path.
        MutexData md0 = mutex_->load();
        if (!md0.is_valid(ld_.version)) return false;
        if (md0.ord_id == ld_.ord_id) return true;
        if (req_type == RequestType::READ && md0.can_read_reserve_without_changing(ld_.ord_id)) return true;
        // slow path.
        return do_request(req_type, true);
    }
    INLINE void blind_write() {
        assert(ld_.is_state(LockState::INIT));
        ld_.state = LockState::PRE_BLIND_WRITE;
    }
    INLINE void reserve_for_blind_write() {
        assert(ld_.is_state(LockState::PRE_BLIND_WRITE));
        bool ret = do_request(RequestType::BLIND_WRITE, false);
        unused(ret); assert(ret);
    }
    INLINE bool upgrade() {
        assert(ld_.is_state(LockState::READ));
        return do_request(RequestType::READ_MODIFY_WRITE, true);
    }
    template <bool checks_version>
    INLINE bool protect() {
        constexpr LockState lock_state = (checks_version ? LockState::READ_MODIFY_WRITE : LockState::BLIND_WRITE);
        unused(lock_state); assert(ld_.is_state(lock_state));
        return do_request(RequestType::PROTECT, checks_version);
    }
    template <LockState from_state>
    INLINE void unlock_special() noexcept {
        assert(mutex_ != nullptr);
        if (is_lock_state_in(from_state, {LockState::INIT, LockState::PRE_BLIND_WRITE})) {
            mutex_ = nullptr;
            return;
        }
        if (is_lock_state_in(from_state, {LockState::READ, LockState::BLIND_WRITE, LockState::READ_MODIFY_WRITE})) {
            MutexData md0 = mutex_->load();
            if (md0.ord_id != ld_.ord_id) {
                mutex_ = nullptr;
                return;
            }
        }
        bool ret = do_request(RequestType::UNLOCK, false);
        unused(ret); assert(ret);
        mutex_ = nullptr;
    }
    INLINE void unlock_general() noexcept {
        if (mutex_ == nullptr) return;
        if (ld_.is_state_in({LockState::INIT, LockState::PRE_BLIND_WRITE})) {
            mutex_ = nullptr;
            return;
        }
        if (ld_.is_state_in({LockState::READ, LockState::BLIND_WRITE, LockState::READ_MODIFY_WRITE})) {
            MutexData md0 = mutex_->load();
            if (md0.ord_id != ld_.ord_id) {
                mutex_ = nullptr;
                return;
            }
        }
        bool ret = do_request(RequestType::UNLOCK, false);
        unused(ret); assert(ret);
        mutex_ = nullptr;
    }

    template <bool allow_protected = false>
    INLINE bool is_unchanged() const {
        assert(mutex_);
        return mutex_->load().is_valid<allow_protected>(ld_.version);
    }
    INLINE void update() {
        assert(ld_.state == LockState::PROTECTED);
        ld_.updated = true;
    }

    INLINE uintptr_t get_mutex_id() const { return uintptr_t(mutex_); }

    INLINE bool is_state(LockState st) const { return ld_.is_state(st); }
    INLINE bool is_state_in(std::initializer_list<LockState> st_list) const { return ld_.is_state_in(st_list); }

private:
    INLINE bool do_request(RequestType type, bool checks_version) {
        assert(mutex_ != nullptr);
        req_.init(type, ld_, checks_version);
        if (mutex_->do_request(req_)) {
            ld_ = req_.ld;
            return true;
        }
        return false;
    }
};


} // namespace mcs


template <typename Mutex, typename Lock>
class LockSet
{
public:
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
    INLINE void init(size_t value_size, size_t nr_reserve) {
        value_size_ = value_size;
        if (value_size == 0) value_size++;
        local_.setSizes(value_size);

        vec_.reserve(nr_reserve);
        local_.reserve(nr_reserve);
    }
    INLINE void set_ord_id(uint32_t ord_id) {
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
            OpEntryL& ope = vec_.emplace_back(Lock(mutex, ord_id_));
            ope.info.set(allocate_local_val(), (void*)shared_val);
            Lock& lk = ope.lock;
            void* local_val = get_local_val_ptr(ope.info);
            if (read_type == OPTIMISTIC) {
                lk.invisible_read(shared_val, local_val, value_size_);
            } else if (read_type == READ_RESERVE) {
                lk.read_and_reserve(shared_val, local_val, value_size_);
            } else if (read_type == WRITE_RESERVE) {
                lk.read_for_update(shared_val, local_val, value_size_);
            } else {
                BUG();
            }
            copy_value(dst, local_val);
            return true;
        }
#if 1  // early verify or keep reservation if required.
        Lock& lk = it->lock;
        if (lk.is_state(LockState::READ)) {
            if (read_type == OPTIMISTIC) {
                if (!lk.is_unchanged()) return false;
            } else {
                if (!lk.template try_keep_reservation<LockState::READ>()) return false;
            }
        } else if (lk.is_state(LockState::READ_MODIFY_WRITE)) {
            if (!lk.template try_keep_reservation<LockState::READ_MODIFY_WRITE>()) return false;
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
            OpEntryL& ope = vec_.emplace_back(Lock(mutex, ord_id_));
            Lock& lk = ope.lock;
            lk.blind_write();
            ope.info.set(allocate_local_val(), shared_val);
            copy_value(get_local_val_ptr(ope.info), src);
            return true;
        }
        Lock& lk = it->lock;
        if (lk.is_state(LockState::READ) && !lk.upgrade()) return false;
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
                lk.reserve_for_blind_write();
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
                if (!lk.template protect<false>()) return false;
            } else if (lk.is_state(LockState::READ_MODIFY_WRITE)) {
                if (!lk.template protect<true>()) return false;
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
                lk.template unlock_special<LockState::READ>();
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
                lk.template unlock_special<LockState::PROTECTED>();
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
        constexpr size_t threshold = 4096 / sizeof(OpEntryL);
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


/**
 * This is used as tag.
 */
struct PqMcsLike
{
};

} // namespace licc2
} // namespace lock
} // namespace cybozu
