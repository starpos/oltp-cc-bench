#pragma once
/**
 * @file
 * @brief Transaction processing utilities.
 * @author Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 *
 * (C) 2016 Cybozu Labs, Inc.
 */

#include "constexpr_util.hpp"


/**
 * Transaction Id generator.
 *
 * It reduces the number of atomic_fetch_add() call in order
 * not to be scalability bottleneck.
 * It loses fairness (first-in, high-priority rule) a bit, while it is negligible effect.
 * Smaller TxId have higher priority.
 * Long transactions will have higher priority relatively,
 * because TxId is not changed in retries.
 */

#ifndef USE_64BIT_TXID
using TxId = uint32_t;
#else
using TxId = uint64_t;
#endif

const TxId MaxTxId = TxId(-1);


class LocalTxIdGenerator
{
private:
    TxId val_;
    TxId mask_;
    TxId delta_;
    bool hasNext_;
public:
    LocalTxIdGenerator()
        : val_()
        , mask_()
        , delta_()
        , hasNext_(false) {
    }
    LocalTxIdGenerator(size_t fixedBits, size_t allocBits, TxId begin)
        : val_(begin)
        , mask_((~(MaxTxId << allocBits)) << fixedBits)
        , delta_(TxId(1) << fixedBits)
        , hasNext_(true) {
    }
    bool hasNext() const { return hasNext_; }
    /**
     * MaxTxId will not returned because it is special value.
     */
    TxId get() {
        assert(hasNext_);
        TxId ret = val_;
        val_ += delta_;
        hasNext_ = (val_ & mask_) != 0 && val_ != MaxTxId;
        return ret;
    }
};


class GlobalTxIdGenerator
{
private:
    static constexpr size_t CACHE_LINE_SIZE = 64;
    alignas(CACHE_LINE_SIZE)
    TxId counter_;
    uint8_t fixedBits_;
    uint8_t allocBits_;
public:
    GlobalTxIdGenerator() : counter_(0), fixedBits_(), allocBits_() {}
    /**
     * counter_'s layout.
     *
     * XXXXYYYYYYYYYYZZ
     * 31             0
     *
     * X: local generator can use all bits in this range.
     * Y, Z: local generator must use this value.
     *
     * Local generator will generates the value as follows:
     *
     * YYYYYYYYYYXXXXZZ
     * 31             0
     *
     * fixedBits: length of Z
     * allocBits: length of X
     *
     * Local generator will have 2^allocBits TxIds with one call of get().
     * So the number of atomic_fetch_add() call will be reduced.
     *
     * 2^fixedBits should be > concurrency.
     * 2^allocBits should make atomic_fetch_add() overhead ignorable.
     */
    GlobalTxIdGenerator(uint8_t fixedBits, uint8_t allocBits)
        : GlobalTxIdGenerator() {
        init(fixedBits, allocBits);
    }
    void init(uint8_t fixedBits, uint8_t allocBits) {
        if (fixedBits < 1) {
            throw std::runtime_error("too small fixed bits.");
        }
        if (allocBits < 1) {
            throw std::runtime_error("too small alloc bits.");
        }
        if (fixedBits + allocBits >= 28) {
            throw std::runtime_error("too large fixed/alloc bits.");
        }
        counter_ = 0;
        fixedBits_ = fixedBits;
        allocBits_ = allocBits;
    }
    LocalTxIdGenerator get() {
        const TxId v = __atomic_fetch_add(&counter_, 1, __ATOMIC_RELAXED);
        const TxId mask = MaxTxId << fixedBits_;
        const TxId begin = ((v & mask) << allocBits_) | (v & ~mask);;
        return LocalTxIdGenerator(fixedBits_, allocBits_, begin);
    }
    TxId sniff() const {
        const TxId v = __atomic_load_n(&counter_, __ATOMIC_RELAXED);
        const TxId mask = MaxTxId << fixedBits_;
        const TxId begin = ((v & mask) << allocBits_) | (v & ~mask);;
        return begin;
    }
};


class TxIdGenerator
{
private:
    GlobalTxIdGenerator *globalG_;
    LocalTxIdGenerator localG_;
public:
    TxIdGenerator() : globalG_(), localG_() {}
    explicit TxIdGenerator(GlobalTxIdGenerator *globalG) : TxIdGenerator() {
        init(globalG);
    }
    void init(GlobalTxIdGenerator *globalG) {
        if (globalG == nullptr) {
            throw std::runtime_error("TxIdGenerator::init: globalG must not be null");
        }
        globalG_ = globalG;
    }
    TxId get() {
        assert(globalG_ != nullptr);
        if (!localG_.hasNext()) localG_ = globalG_->get();
        return localG_.get();
    }
};


class SimpleTxIdGenerator
{
private:
    static constexpr size_t CACHE_LINE_SIZE = 64;
    alignas(CACHE_LINE_SIZE)
    TxId id_;
public:
    SimpleTxIdGenerator() : id_(0) {}
    /**
     * MaxTxId will not returned because it is special value.
     */
    TxId get() {
        TxId x = getLocal();
        if (x == MaxTxId) x = getLocal();
        return x;
    }
    TxId sniff() const {
        return __atomic_load_n(&id_, __ATOMIC_RELAXED);
    }
private:
    TxId getLocal() {
        return __atomic_fetch_add(&id_, 1, __ATOMIC_RELAXED);
    }
};



/**
 * Priority Id generator.
 *
 * This does not have scalability bottleneck because atomic_fetch_add() is never used.
 * Different worker threads does not use the same priority id space
 * while they are almost fair.
 * There is a drawback compared with TxIdGenerator that
 * you must specify priority information explicitly.
 */

template <size_t bits>
struct PriId
{
    static_assert(bits <= 64, "bits must be <= 64.");
    static_assert(bits >= 3, "bits must be >= 3.");
    union {
        uint64_t value:bits;
        struct {
            // LSB (in little endian)
            uint64_t fixed:(bits - 2);
            uint64_t alloc:1;
            uint64_t pri:1;
            // MSB
        };
    };
};

/**
 * Do not change fixed bits.
 * 0 and MAX_VALUE will not be generated because they are special values.
 */
template <size_t bits>
class LocalPriIdGenerator
{
private:
    PriId<bits> priId_;
    constexpr static uint64_t MAX_VALUE = GetMaxValue(bits);
public:
    void init(uint64_t basePriId) {
        priId_.value = basePriId;
#if 0
        ::printf("LocalPriIdGenerator: %" PRIu64 "\n"
                 "pri: %" PRIu64 " alloc: %" PRIu64 " fixed: %" PRIu64 "\n"
                 , priId_.value, priId_.pri, priId_.alloc, priId_.fixed);
#endif
    }
    uint64_t get(uint64_t pri) {
        priId_.pri = pri;
        priId_.alloc++;
#if 0
        if (priId_.value == 0) {
            priId_.alloc++;
        }
#endif
        assert(priId_.value != 0);
        assert(priId_.value != MAX_VALUE);
        return priId_.value;
    }
};

template <size_t bits>
class GlobalPriIdGenerator
{
private:
    uint64_t fixed_:(bits - 2);
public:
    GlobalPriIdGenerator() : fixed_(1) {}
    /**
     * This is not thread-safe.
     * Please call this function outside of worker thread
     * and pass the returned generator to the worker thread.
     */
    LocalPriIdGenerator<bits> get() {
        if (fixed_ == GetMaxValue(bits - 2)) {
            throw std::runtime_error("GlobalPriIdGenerator: bits too small.");
        }
        LocalPriIdGenerator<bits> gen;
        gen.init(fixed_);
        fixed_++;
        return gen;
    }
};
