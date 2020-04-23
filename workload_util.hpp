#pragma once
/**
 * Workload utility.
 */
#include <cinttypes>
#include <vector>
#include <string>
#include "cybozu/exception.hpp"
#include "util.hpp"
#include "inline.hpp"
#include "zipf.hpp"


enum TxMode : uint8_t
{
    USE_LAST_WRITE_TX = 0,
    USE_FIRST_WRITE_TX = 1,
    USE_READONLY_TX = 2,
    USE_WRITEONLY_TX = 3,
    // USE_HALF_AND_HALF_TX = 4,  // use USE_MIX_TX instead.
    USE_MIX_TX = 5,
    USE_LAST_WRITE_HC_TX = 6,
    USE_FIRST_WRITE_HC_TX = 7,
    USE_LAST_WRITE_SAME_TX = 8,
    USE_FIRST_WRITE_SAME_TX = 9,
    TXMODE_MAX = 10,
};


template <typename Random, typename Mode>
using GetModeFuncType = Mode (*)(Random&, size_t, size_t, size_t, size_t);


template <typename Random, typename Mode, TxMode txMode>
INLINE Mode getModeT(Random& rand, size_t nrOp, size_t nrWr, size_t wrRatio, size_t idx)
{
    switch (txMode) {
    case USE_MIX_TX:
        return rand() < wrRatio ? Mode::X : Mode::S;
    case USE_READONLY_TX:
        return Mode::S;
    case USE_WRITEONLY_TX:
        return Mode::X;
    case USE_FIRST_WRITE_TX:
    case USE_FIRST_WRITE_HC_TX:
    case USE_FIRST_WRITE_SAME_TX:
        return idx < nrWr ? Mode::X : Mode::S;
    default:
        assert(txMode == USE_LAST_WRITE_TX ||
               txMode == USE_LAST_WRITE_HC_TX ||
               txMode == USE_LAST_WRITE_SAME_TX);
        return idx >= nrOp - nrWr ? Mode::X : Mode::S;
    }
}


template <typename Random, typename Mode>
INLINE GetModeFuncType<Random, Mode> selectGetModeFunc(bool isLongTx, TxMode shortTxMode, TxMode longTxMode)
{
    struct {
        TxMode txMode;
        GetModeFuncType<Random, Mode> func;
    } table[] = {
        {USE_LAST_WRITE_TX, getModeT<Random, Mode, USE_LAST_WRITE_TX>},
        {USE_FIRST_WRITE_TX, getModeT<Random, Mode, USE_FIRST_WRITE_TX>},
        {USE_READONLY_TX, getModeT<Random, Mode, USE_READONLY_TX>},
        {USE_WRITEONLY_TX, getModeT<Random, Mode, USE_WRITEONLY_TX>},
        {USE_MIX_TX, getModeT<Random, Mode, USE_MIX_TX>},
        {USE_LAST_WRITE_HC_TX, getModeT<Random, Mode, USE_LAST_WRITE_HC_TX>},
        {USE_FIRST_WRITE_HC_TX, getModeT<Random, Mode, USE_FIRST_WRITE_HC_TX>},
        {USE_LAST_WRITE_SAME_TX, getModeT<Random, Mode, USE_LAST_WRITE_SAME_TX>},
        {USE_FIRST_WRITE_SAME_TX, getModeT<Random, Mode, USE_FIRST_WRITE_SAME_TX>},
    };
    for (size_t i = 0; i < sizeof(table)/sizeof(table[0]); i++) {
        if (isLongTx) {
            if (table[i].txMode == longTxMode) return table[i].func;
        } else {
            if (table[i].txMode == shortTxMode) return table[i].func;
        }
    }
    throw cybozu::Exception(__func__) << "Bad transaction mode" << isLongTx << shortTxMode << longTxMode;
}


/**
 * deprected
 */
template <typename Random, typename Mode>
DEPRECATED INLINE Mode getMode(Random& rand, const std::vector<bool>& isWriteV,
                               bool isLongTx, TxMode shortTxMode, TxMode longTxMode,
                               size_t nrOp, size_t nrWr, size_t i)
{
    if (isLongTx) {
        switch (longTxMode) {
        case USE_MIX_TX:
            {
                using RInt = typename Random::ResultType;
                double ratio = (double)nrWr / (double)(nrOp) * (double)(RInt)(-1);
                return (RInt)ratio > rand() ? Mode::X : Mode::S;
            }
        case USE_READONLY_TX:
            return Mode::S;
        case USE_FIRST_WRITE_TX:
        case USE_FIRST_WRITE_SAME_TX:
            return i < nrWr ? Mode::X : Mode::S;
        default:
            assert(longTxMode == USE_LAST_WRITE_TX ||
                   longTxMode == USE_LAST_WRITE_SAME_TX);
            return i >= nrOp - nrWr ? Mode::X : Mode::S;
        }
    } else {
        switch (shortTxMode) {
        case USE_MIX_TX:
            return isWriteV[i] ? Mode::X : Mode::S;
        case USE_READONLY_TX:
            return Mode::S;
        case USE_WRITEONLY_TX:
            return Mode::X;
        case USE_FIRST_WRITE_TX:
        case USE_FIRST_WRITE_HC_TX:
        case USE_FIRST_WRITE_SAME_TX:
            return i < nrWr ? Mode::X : Mode::S;
        default:
            assert(shortTxMode == USE_LAST_WRITE_TX ||
                   shortTxMode == USE_LAST_WRITE_HC_TX ||
                   shortTxMode == USE_LAST_WRITE_SAME_TX);
            return i >= nrOp - nrWr ? Mode::X : Mode::S;
        }
    }
}


template <typename Random>
DEPRECATED INLINE size_t getRecordIdx(Random& rand, bool isLongTx, int shortTxMode, int longTxMode,
                                      size_t nrMu, size_t nrOp, size_t idx, size_t& firstRecIdx,
                                      bool usesZipf, FastZipf& fastZipf)
{
    unused(longTxMode);
    if (isLongTx) {
        if (longTxMode == USE_FIRST_WRITE_SAME_TX) {
            if (idx == 0) {
                return 0;
            } else {
                assert(nrMu > 1);
                return rand() % (nrMu - 1) + 1; // non-zero.
            }
        }
        if (longTxMode == USE_LAST_WRITE_SAME_TX) {
            if (idx == nrOp - 1) {
                return 0;
            } else {
                assert(nrMu > 1);
                return rand() % (nrMu - 1) + 1; // non-zero.
            }
        }
    } else {
        if (shortTxMode == USE_LAST_WRITE_HC_TX || shortTxMode == USE_FIRST_WRITE_HC_TX) {
            if (idx == 0) {
                firstRecIdx = rand() & 0x1;
                return firstRecIdx;
            } else if (idx == nrOp - 1) {
                return 1 - firstRecIdx;
            } else {
                return rand() % nrMu;
            }
        }
        if (shortTxMode == USE_FIRST_WRITE_SAME_TX) {
            if (idx == 0) {
                assert(nrMu > 0);
                //return rand() % std::min<size_t>(10, nrMu); // typically first 10 records.
                return 0;
            } else {
                assert(nrMu > 1);
                return rand() % (nrMu - 1) + 1; // non-zero.
            }
        }
        if (shortTxMode == USE_LAST_WRITE_SAME_TX) {
            if (idx == nrOp - 1) {
                return 0;
            } else {
                assert(nrMu > 1);
                return rand() % (nrMu - 1) + 1; // non-zero.
            }
        }
    }
    if (usesZipf) return fastZipf();
    return rand() % nrMu;
}



template <typename Random>
using GetRecordIdxType = size_t (*)(Random&, FastZipf&, size_t, size_t, size_t, size_t&);


template <typename Random, bool isLongTx, TxMode txMode, bool usesZipf>
INLINE size_t getRecordIdxT(Random& rand, FastZipf& fastZipf, size_t nrMu, size_t nrOp, size_t idx, size_t& firstRecIdx)
{
    if (usesZipf) return fastZipf();

    if (isLongTx) {
        switch (txMode) {
        case USE_FIRST_WRITE_SAME_TX:
            if (idx == 0) return 0;
            assert(nrMu > 1);
            return rand() % (nrMu - 1) + 1; // non-zero;
        case USE_LAST_WRITE_SAME_TX:
            if (idx == nrOp - 1) return 0;
            assert(nrMu > 1);
            return rand() % (nrMu - 1) + 1; // non-zero.
        default:
            return rand() % nrMu; // uniform distribution.
        }
    }
    // for short tx.
    switch (txMode) {
    case USE_LAST_WRITE_HC_TX:
    case USE_FIRST_WRITE_HC_TX:
        if (idx == 0) {
            firstRecIdx = rand() & 0x1;
            return firstRecIdx;
        } else if (idx == nrOp - 1) {
            return 1 - firstRecIdx;
        } else {
            return rand() % nrMu;
        }
    case USE_FIRST_WRITE_SAME_TX:
        if (idx == 0) {
            assert(nrMu > 0);
            //return rand() % std::min<size_t>(10, nrMu); // typically first 10 records.
            return 0;
        }
        assert(nrMu > 1);
        return rand() % (nrMu - 1) + 1; // non-zero.
    case USE_LAST_WRITE_SAME_TX:
        if (idx == nrOp - 1) {
            return 0;
        }
        assert(nrMu > 1);
        return rand() % (nrMu - 1) + 1; // non-zero.
    default:
        return rand() % nrMu;
    }
}


template <typename Random, bool isLongTx, TxMode txMode>
INLINE GetRecordIdxType<Random> selectGetRecordIdx2(bool usesZipf)
{
    if (usesZipf) {
        return getRecordIdxT<Random, isLongTx, txMode, true>;
    } else {
        return getRecordIdxT<Random, isLongTx, txMode, false>;
    }
}


template <typename Random>
INLINE GetRecordIdxType<Random> selectGetRecordIdx(bool isLongTx, TxMode shortTxMode, TxMode longTxMode, bool usesZipf)
{
    if (isLongTx) {
        switch (longTxMode) {
        case USE_LAST_WRITE_TX:
            return selectGetRecordIdx2<Random, true, USE_LAST_WRITE_TX>(usesZipf);
        case USE_FIRST_WRITE_TX:
            return selectGetRecordIdx2<Random, true, USE_FIRST_WRITE_TX>(usesZipf);
        case USE_READONLY_TX:
            return selectGetRecordIdx2<Random, true, USE_READONLY_TX>(usesZipf);
        case USE_WRITEONLY_TX:
            return selectGetRecordIdx2<Random, true, USE_WRITEONLY_TX>(usesZipf);
        case USE_MIX_TX:
            return selectGetRecordIdx2<Random, true, USE_MIX_TX>(usesZipf);
        case USE_LAST_WRITE_HC_TX:
            return selectGetRecordIdx2<Random, true, USE_LAST_WRITE_HC_TX>(usesZipf);
        case USE_FIRST_WRITE_HC_TX:
            return selectGetRecordIdx2<Random, true, USE_FIRST_WRITE_HC_TX>(usesZipf);
        case USE_LAST_WRITE_SAME_TX:
            return selectGetRecordIdx2<Random, true, USE_LAST_WRITE_SAME_TX>(usesZipf);
        case USE_FIRST_WRITE_SAME_TX:
            return selectGetRecordIdx2<Random, true, USE_FIRST_WRITE_SAME_TX>(usesZipf);
        default:
            throw cybozu::Exception(__func__) << "bad transaction mode" << isLongTx << shortTxMode << longTxMode << usesZipf;
        }
    }
    // short tx.
    switch (shortTxMode) {
    case USE_LAST_WRITE_TX:
        return selectGetRecordIdx2<Random, false, USE_LAST_WRITE_TX>(usesZipf);
    case USE_FIRST_WRITE_TX:
        return selectGetRecordIdx2<Random, false, USE_FIRST_WRITE_TX>(usesZipf);
    case USE_READONLY_TX:
        return selectGetRecordIdx2<Random, false, USE_READONLY_TX>(usesZipf);
    case USE_WRITEONLY_TX:
        return selectGetRecordIdx2<Random, false, USE_WRITEONLY_TX>(usesZipf);
    case USE_MIX_TX:
        return selectGetRecordIdx2<Random, false, USE_MIX_TX>(usesZipf);
    case USE_LAST_WRITE_HC_TX:
        return selectGetRecordIdx2<Random, false, USE_LAST_WRITE_HC_TX>(usesZipf);
    case USE_FIRST_WRITE_HC_TX:
        return selectGetRecordIdx2<Random, false, USE_FIRST_WRITE_HC_TX>(usesZipf);
    case USE_LAST_WRITE_SAME_TX:
        return selectGetRecordIdx2<Random, false, USE_LAST_WRITE_SAME_TX>(usesZipf);
    case USE_FIRST_WRITE_SAME_TX:
        return selectGetRecordIdx2<Random, false, USE_FIRST_WRITE_SAME_TX>(usesZipf);
    default:
        throw cybozu::Exception(__func__) << "bad transaction mode" << isLongTx << shortTxMode << longTxMode << usesZipf;
    }
}


struct AccessInfo
{
    uint64_t key:63;
    uint64_t is_write:1;

    INLINE bool operator<(const AccessInfo& rhs) const { return key < rhs.key; }

    std::string str() const {
        std::stringstream ss;
        ss << (is_write ? "W" : "R") << " " << key;
        return ss.str();
    }

    template <typename Mode>
    INLINE void get(uint64_t& key0, Mode& mode) {
        key0 = key;
        mode = is_write ? Mode::X : Mode::S;
    }
};


using AccessInfoVec = std::vector<AccessInfo>;


template <typename Random, typename Mode>
INLINE void fillAccessInfoVec(
    Random& rand, FastZipf& fastZipf,
    GetModeFuncType<Random, Mode>& getMode, GetRecordIdxType<Random>& getRecordIdx,
    size_t nrMu, size_t wrRatio, AccessInfoVec& out)
{
    const size_t nrOp = out.size();
    size_t firstRecIdx;
    const size_t nrWr = size_t((double)wrRatio / (double)SIZE_MAX * (double)nrOp);
    for (size_t i = 0; i < nrOp; i++) {
        auto& ai = out[i];
        ai.key = getRecordIdx(rand, fastZipf, nrMu, nrOp, i, firstRecIdx);
        ai.is_write = (getMode(rand, nrOp, nrWr, wrRatio, i) == Mode::X);
    }
}
