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
    MutexData(const MutexData&) noexcept = default;
    MutexData& operator=(const MutexData&) noexcept = default;

    MutexData(uint64_t x) : MutexData() { *(uint64_t *)this = x; }
    operator uint64_t() const { return *(uint64_t *)this; }

    // Reference of uint64_t.
    uint64_t& ref() { return *(uint64_t *)this; }
    const uint64_t& ref() const { return *(uint64_t *)this; }
    // Value of uint64_t explicitly.  (x.val() is shorter than uint64_t(x).)
    uint64_t val() const { return *this; }

    bool operator==(const MutexData& rhs) const {
        return *(uint64_t *)this == *(uint64_t *)&rhs;
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

    template <bool allow_protected = false>
    bool is_valid(uint32_t version0) const {
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
    LockData() = default; // CAUSION: not initialied.
#else
    LockData() : state(LockState::INIT), updated(false), ord_id(MAX_ORD_ID), version(0) {
    }
#endif
    explicit LockData(uint32_t ord_id0) : LockData() { init(ord_id0); }

    LockData(const LockData&) noexcept = default;
    LockData& operator=(const LockData&) noexcept = default;

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

#if 0
    // MUST_WAIT or IMPOSSIBLE.
    explicit MutexOpCreator(MutexOpCapability res) : capability(res), ld(), md() {
        assert(res != POSSIBLE);
    }
#endif

    // Valid initial value.
    MutexOpCreator(LockData ld0, MutexData md0) : capability(POSSIBLE), ld(ld0), md(md0) {
    }

    MutexOpCreator(const MutexOpCreator&) noexcept = default;
    MutexOpCreator& operator=(const MutexOpCreator&) noexcept = default;

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
#if 1
    MutexOpCreator invisible_read() const {
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
    MutexOpCreator blind_write() const {
        MutexOpCreator moc(*this);
        LockData& ld = moc.ld;
        assert(ld.state == LockState::INIT);
        ld.state = LockState::PRE_BLIND_WRITE;
        return moc;
    }
#endif

    template <LockState to_state, bool is_write, bool checks_version>
    MutexOpCreator xxx_reserve() const {
        MutexOpCreator moc(*this);
        if (!moc) return moc;
        LockData& ld = moc.ld; MutexData& md = moc.md;
        assert_from_lock_states<to_state>(ld.state);
        if (checks_version && (md.version != ld.version || md.protected_)) {
            moc.capability = IMPOSSIBLE;
            return moc;
        }
        bool can_reserve = is_write
            ? md.can_write_reserve(ld.ord_id)
            : md.can_read_reserve(ld.ord_id);
        if (!can_reserve || md.protected_) {
            moc.capability = MUST_WAIT;
            return moc;
        }
        if (is_write) {
            md.prepare_write_reserve(ld.ord_id);
        } else {
            md.prepare_read_reserve(ld.ord_id);
        }
        ld.state = to_state;
        ld.version = md.version;
        return moc;
    }
    template <bool checks_version>
    MutexOpCreator xxx_protect() const {
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
    MutexOpCreator xxx_unlock_special() const {
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
    MutexOpCreator unlock_general() const {
        switch (ld.state) {
        case LockState::INIT:
            return xxx_unlock_special<LockState::INIT>();
        case LockState::READ:
            return xxx_unlock_special<LockState::READ>();
        case LockState::PRE_BLIND_WRITE:
            return xxx_unlock_special<LockState::PRE_BLIND_WRITE>();
        case LockState::BLIND_WRITE:
            return xxx_unlock_special<LockState::BLIND_WRITE>();
        case LockState::READ_MODIFY_WRITE:
            return xxx_unlock_special<LockState::READ_MODIFY_WRITE>();
        case LockState::PROTECTED:
            return xxx_unlock_special<LockState::PROTECTED>();
        default:
            BUG();
        }
    }
};


template <typename Mutex>
void invisible_read(Mutex& mutex, LockData& ld, const void* shared, void* local, size_t size)
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
        acquire_memory_barrier();
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
        unlock_general();
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
        licc2::invisible_read(*mutex_, ld_, shared, local, size);
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
            constexpr LockState to_state = do_write_reserve
                ? LockState::READ_MODIFY_WRITE
                : LockState::READ;
            MutexOpCreator moc1 = moc0.xxx_reserve<to_state, do_write_reserve, false>();
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
            constexpr LockState to_state = do_write_reserve
                ? LockState::READ_MODIFY_WRITE
                : LockState::READ;
            MutexOpCreator moc1 = moc0.xxx_reserve<to_state, do_write_reserve, true>();
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
            MutexOpCreator moc1 = [&]() {
                if (lock_state == LockState::READ) {
                    return moc0.xxx_reserve<LockState::READ, false, true>();
                } else {
                    return moc0.xxx_reserve<LockState::READ_MODIFY_WRITE, true, true>();
                }
            }();
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
            MutexOpCreator moc1 = moc0.xxx_reserve<LockState::BLIND_WRITE, true, false>();
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
            MutexOpCreator moc1 = moc0.xxx_reserve<LockState::READ_MODIFY_WRITE, true, true>();
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
    template <bool checks_version>
    bool protect() {
        constexpr LockState to_state = checks_version ? LockState::READ_MODIFY_WRITE : LockState::BLIND_WRITE;
        assert(ld_.state == to_state);
        MutexData md0 = mutex_->load();
        for (;;) {
            _mm_pause();
            MutexOpCreator moc0(ld_, md0);
            MutexOpCreator moc1 = moc0.xxx_reserve<to_state, true, checks_version>();
            MutexOpCreator moc2 = moc1.xxx_protect<checks_version>();
            if (!moc2.possible()) return false;
            if (mutex_->cas(md0, moc2.md)) {
                ld_ = moc2.ld;
                return true;
            }
            // CAS failed and continue.
        }
    }
    INLINE void unlock_general() noexcept {
        if (mutex_ == nullptr) return;
        if (ld_.state == LockState::INIT || ld_.state == LockState::PRE_BLIND_WRITE) return;
        MutexData md0 = mutex_->load();
        for (;;) {
            _mm_pause();
            MutexOpCreator moc0(ld_, md0);
            MutexOpCreator moc1 = moc0.unlock_general();
            assert(moc1.possible());
            if (md0 == moc1.md || mutex_->cas(md0, moc1.md)) {
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
        MutexData md0 = mutex_->load();
        for (;;) {
            _mm_pause();
            MutexOpCreator moc0(ld_, md0);
            MutexOpCreator moc1 = moc0.xxx_unlock_special<from_state>();
            assert(moc1);
            if (md0 == moc1.md || mutex_->cas(md0, moc1.md)) {
                ld_ = moc1.ld;
                mutex_ = nullptr;
                return;
            }
            // CAS failed and continue.
        }
    }
    template <bool allow_protected = false>
    bool is_unchanged() const {
        assert(mutex_);
        return mutex_->load().is_valid<allow_protected>(ld_.version);
    }
    void update() {
        assert(ld_.state == LockState::PROTECTED);
        ld_.updated = true;
    }

    uintptr_t get_mutex_id() const { return uintptr_t(mutex_); }

    bool is_state(LockState st) const { return ld_.is_state(st); }
    bool is_state_in(std::initializer_list<LockState> st_list) const { return ld_.is_state_in(st_list); }

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
                if (!lk.is_unchanged()) return false;
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
                if (!lk.protect<false>()) return false;
            } else if (lk.is_state(LockState::READ_MODIFY_WRITE)) {
                if (!lk.protect<true>()) return false;
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
#if 1
                lk.unlock_special<LockState::READ>();
#else
                lk.unlock_general();
#endif
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
#if 1
                lk.unlock_special<LockState::PROTECTED>();
#else
                lk.unlock_general();
#endif
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


bool is_request_type_in(RequestType type, std::initializer_list<RequestType> type_list)
{
    for (RequestType type0 : type_list) {
        if (type == type0) return true;
    }
    return false;
}


bool is_unlock_request(RequestType type)
{
    return is_request_type_in(type, {UNRESERVE_FOR_READ, UNRESERVE_FOR_WRITE, UNPROTECT});
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
    alignas(CACHE_LINE_SIZE)
    Request* next;  // for linked list as a request queue.

    // alignas(CACHE_LINE_SIZE)
    RequestType type;
    Message msg; // message from the owner. internally used.
    bool succeeded; // result of the request.
    uint32_t ord_id; // requestor's ord_id.

    // Current lock data should be set begore enquing the request.
    // If the request is successfully done, new lockdata will be set.
    LockData ld;

    /**
     * CAUSION: uninitialized. call init() to initialize the object.
     */
    Request() = default;

    void init(RequestType type0, LockData ld0) {
        next = nullptr;
        type = type0;
        ord_id = ld0.ord_id;
        msg = WAITING;
        succeeded = false;
        ld = ld0;
    }

    Request(const Request& rhs) = delete;
    Request& operator=(const Request& rhs) = delete;

    Request(Request&& rhs) noexcept : Request() { swap(rhs); }
    Request& operator=(Request&& rhs) noexcept { swap(rhs); return *this; }

    bool operator<(const Request& rhs) const { return ord_id < rhs.ord_id; }

    Message local_spin_wait() {
        Message msg0;
        while ((msg0 = load_acquire(msg)) == WAITING) _mm_pause();
        store_release(msg, WAITING);
        return msg0;
    }
    Request* get_next_ptr() {
        Request* next0;
        while ((next0 = load_acquire(next)) == nullptr) _mm_pause();
        return next0;
    }
    void swap(Request& rhs) noexcept {
        std::swap(next, rhs.next);
        std::swap(type, rhs.type);
        std::swap(msg, rhs.msg);
        std::swap(succeeded, rhs.succeeded);
        std::swap(ord_id, rhs.ord_id);
        std::swap(ld, rhs.ld);
    }
};


using ReqList = NodeListT<Request>;


/**
 * These are tags stored in the tail pointer.
 * They never be the pointer value.
 */
Request* const UNOWNED = (Request*)0;
Request* const OWNED = (Request*)1;


class Mutex
{
    alignas(sizeof(uintptr_t))
    Request* tail_;
    Request* head_;
    ReqList waiting_;
    MutexData md_;
public:
    Mutex() : tail_(UNOWNED), head_(nullptr), waiting_(), md_() {
        md_.init();
    }

    MutexData load() const {
        return load_acquire(md_.ref());
    }
    void store(MutexData md) {
        store_release(md_.ref(), md.ref());
    }
#if 0 // cas is not used because md modifying operations will be done through the request queue only.
    bool cas(MutexData& before, MutexData after) {
        return compare_exchange(md_.ref(), before.ref(), after.ref());
    }
#endif

    /**
     * Enqueue an request.
     *
     * RETURN:
     *   true if you become owner.
     *   The owner must call begin_owner_task() and do owner task and then end_owner_task().
     */
    bool enqueue(Request& req) {
        Request* prev = exchange(tail_, &req);
        if (prev == UNOWNED) {
            return true;
        }
        if (prev == OWNED) {
            store_release(head_, &req);
            Message msg = req.local_spin_wait();
            unused(msg); assert(msg == OWNER);
            return true;
        }
        // prev is the pointer of the previous request.
        store_release(prev->next, &req);
        Message msg = req.local_spin_wait();
        unused(msg); assert(msg == DONE);
        return false;
    }

    /**
     * The owner request itself is the head.
     *
     * tail is the tail of the dequeued request list.
     * If the request list includes one request, the tail indicates the owner.
     *
     * waiting should be an empty list.
     */
    void begin_owner_task(Request*& tail, ReqList& waiting) {
        tail = exchange(tail_, OWNED);
        waiting = std::move(waiting_);
    }
    /**
     * waiting will be the new head of the waiting list.
     */
    void end_owner_task(Request& req, ReqList&& waiting) {
        waiting_ = std::move(waiting);

        // This must be done during the request is owner.
        const bool done = (req.msg == DONE);
        ::store(req.msg, WAITING);

        Request* tail = load_acquire(tail_);
        if (tail != OWNED || !compare_exchange(tail_, tail, UNOWNED)) {
            Request* head;
            while ((head = load_acquire(head_)) == nullptr) _mm_pause();
            ::store(head_, nullptr);
            // release ownership.
            store_release(head->msg, OWNER);
        }
        // The request is not the owner now.
        if (!done) req.local_spin_wait();
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


    void invisible_read(const void *shared, void *local, size_t size) {
        licc2::invisible_read(*mutex_, ld_, shared, local, size);
    }
    template <bool do_write_reserve = false>
    void read_and_reserve(const void *shared, void *local, size_t size) {
        unused(shared, local, size);

        req_.init(RESERVE_FOR_READ, ld_);
        if (mutex_->enqueue(req_)) do_owner_task();
        if (req_.succeeded) {
            // QQQQQ
        }

        // QQQ
    }
    template <LockState lock_state>
    bool try_keep_reservation() {
        // QQQ
    }
    void blind_write() {
        assert(ld_.state == LockState::INIT);
        ld_.state = LockState::PRE_BLIND_WRITE;
    }
    void blind_write_reserve() {
        // QQQ
    }
    bool upgrade() {
        // QQQ
        return false;
    }
    bool upgrade_if_necessary() {
        if (ld_.state != LockState::READ) {
            // no need to upgrade.
            return true;
        }
        return upgrade();
    }
    template <LockState lock_state>
    bool protect() {
        // QQQ
    }
    void unlock() noexcept {
        if (mutex_ == nullptr) return;

        // QQQ



        mutex_ = nullptr;
    }

    template <bool allow_protected = false>
    bool is_unchanged() const {
        assert(mutex_);
        return mutex_->load().is_valid<allow_protected>(ld_.version);
    }
    void update() {
        assert(ld_.state == LockState::PROTECTED);
        ld_.updated = true;
    }

    uintptr_t get_mutex_id() const { return uintptr_t(mutex_); }

    bool is_state(LockState st) const { return ld_.is_state(st); }
    bool is_state_in(std::initializer_list<LockState> st_list) const { return ld_.is_state_in(st_list); }

private:
    /**
     * CAUSION: req_ object is not swappable during in request queue.
     */
    void swap(Lock& rhs) noexcept {
        std::swap(mutex_, rhs.mutex_);
        std::swap(ld_, rhs.ld_);
        std::swap(req_, rhs.req_);
        // QQQ
    }

    /**
     * Owner must do almost all the md modifying operations.
     */
    void do_owner_task() {
        Request *tail;
        ReqList waiting;
        mutex_->begin_owner_task(tail, waiting);
        owner_process_unlock_operations(&req_, tail, waiting);
        owner_process_lock_operations(waiting);
        mutex_->end_owner_task(req_, std::move(waiting));
    }
    /**
     * process unlock (unreserve and unprotect) operations.
     * head and tail is included.
     */
    void owner_process_unlock_operations(Request* head, Request* tail, ReqList& waiting) {
        while (head != nullptr) {
            bool inserts_waiting = false;
            if (is_unlock_request(head->type)) {
                MutexData md0 = mutex_->load();
                MutexOpCreator moc0(head->ld, md0);
                MutexOpCreator moc1 = moc0.unlock_general();
                assert(moc1.possible());
                if (md0 != moc1.md) mutex_->store(moc1.md);
                head->ld = moc1.ld;
                head->succeeded = true;
                store_release(head->msg, DONE);
            } else {
                inserts_waiting = true;
            }
            Request* next;
            if (head != tail) {
                next = head->get_next_ptr();
            } else {
                next = nullptr;
            }
            if (inserts_waiting) {
                insert_sort<Request>(waiting, head); // it breaks head->next pointer.
            }
            head = next;
        }
    }
    void owner_process_lock_operations(ReqList& waiting) {
        while (!waiting.empty()) {
            Request& req = *waiting.front();
            if (!owner_process_lock_operation(req)) break;
            waiting.pop_front(); // do this before notification.
            store_release(req.msg, DONE);
        }
    }
    /**
     * RETURN:
     *   true if the request has been done (with success or failure).
     */
    bool owner_process_lock_operation(Request& req) {
#if 0
        MutexData md0 = mutex_->load(); // Only the owner can change the mutex state.
        MutexOpCreator moc0(req.ld, md0);
        switch(req.type) {
        case RESERVE_FOR_READ:
            MutexOpCreator moc1 = moc0.read_reserve_1st<LockState::INIT>();
            if (!moc1.possible()) {
                assert(moc1.capability == MUST_WAIT);
                return false;
            }
            mutex_->store(moc1.md);
            req.ld = moc1.ld;
            return true;
        case RESERVE_FOR_WRITE:
            MutexOpCreator moc1 = moc0.blind_write_reserve();


            // QQQQQ
            break;
        case PROTECT:
            // QQQ
            break;
        default:
            BUG();
        }
#else
        unused(req);
        return false;
#endif
    }
};


/**
 *
 */
class LockSet
{


    // QQQ
};


} // namespace mcs

} // namespace licc2
} // namespace lock
} // namespace cybozu
