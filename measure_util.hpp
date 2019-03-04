#pragma once
#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <algorithm>
#include <limits>
#include <type_traits>
#include <thread>
#include "util.hpp"
#include "random.hpp"
#include "cmdline_option.hpp"
#include "thread_util.hpp"
#include "cybozu/exception.hpp"
#include "time.hpp"
#include "arch.hpp"
#include "cache_line_size.hpp"
#include "inline.hpp"
#include "zipf.hpp"
#include "atomic_wrapper.hpp"


void sleepMs(size_t ms)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}


template <typename Random>
void fillMuIdVecLoop(std::vector<size_t>& muIdV, Random& rand, size_t max)
{
    for (size_t i = 0; i < muIdV.size(); i++) {
      retry:
        size_t v = rand() % max;
        for (size_t j = 0; j < i; j++) {
            if (muIdV[j] == v) goto retry;
        }
        muIdV[i] = v;
    }
}


template <typename Random>
void fillMuIdVecHash(std::vector<size_t>& muIdV, Random& rand, size_t max)
{
    std::unordered_set<size_t> set;
    std::unordered_set<size_t>::iterator it;
    for (size_t i = 0; i < muIdV.size(); i++) {
        for (;;) {
            size_t v = rand() % max;
            bool b;
            std::tie(it, b) = set.insert(v);
            if (!b) continue;
            muIdV[i] = v;
            break;
        }
    }
}

template <typename Random>
void fillMuIdVecTree(std::vector<size_t>& muIdV, Random& rand, size_t max)
{
    assert(muIdV.size() <= max);
    std::set<size_t> set;
    for (size_t i = 0; i < max; i++) set.insert(i);

    for (size_t i = 0; i < muIdV.size(); i++) {
        assert(!set.empty());
        size_t v = rand() % max;
        std::set<size_t>::iterator it = set.lower_bound(v);
        if (it == set.end()) --it;
        v = *it;
        set.erase(it);
        muIdV[i] = v;
    }
}

template <typename Random>
void fillMuIdVecArray(std::vector<size_t>& muIdV, Random& rand, size_t max, std::vector<size_t>& tmpV)
{
    assert(max > 1);
    assert(muIdV.size() <= max);
    tmpV.resize(max);
    for (size_t i = 0; i < max; i++) tmpV[i] = i;
    const size_t n = muIdV.size();
    for (size_t i = 0; i < n; i++) {
        size_t j = i + (rand() % (max - i));
        std::swap(tmpV[i], tmpV[j]);
        muIdV[i] = tmpV[i];
    }
    muIdV[n - 1] = tmpV[n - 1];
}

template <typename Random>
INLINE void fillModeVec(std::vector<bool>& isWriteV, Random& rand, size_t nrWrite, std::vector<size_t>& tmpV)
{
    const size_t max = isWriteV.size();
    assert(nrWrite <= max);
#if 0
    for (size_t i = 0; i < nrWrite; i++) {
        isWriteV[i] = true;
    }
    for (size_t i = nrWrite; i < max; i++) {
        isWriteV[i] = false;
    }
    for (size_t i = 0; i < nrWrite; i++) {
        size_t j = i + (rand() % (max - i));
        std::swap(isWriteV[i], isWriteV[j]);
    }
#else
    tmpV.resize(max);
    for (size_t i = 0; i < max; i++) tmpV[i] = i;
    for (size_t i = 0; i < nrWrite; i++) {
        size_t j = i + (rand() % (max - i));
        std::swap(tmpV[i], tmpV[j]);
    }
    for (size_t i = 0; i < max; i++) isWriteV[i] = false;
    for (size_t i = 0; i < nrWrite; i++) isWriteV[tmpV[i]] = true;
#endif
}


template <typename Random>
class DistinctRandom
{
    using Value = typename Random::ResultType;
    Random& rand_;
    Value max_;
    std::unordered_set<Value> set_;
public:
    explicit DistinctRandom(Random& rand)
        : rand_(rand), max_(std::numeric_limits<Value>::max()), set_() {
    }
    Value operator()() {
        assert(max_ > 0);
        Value v;
        typename std::unordered_set<Value>::iterator it;
        bool ret = false;
        while (!ret) {
            v = rand_() % max_;
            std::tie(it, ret) = set_.insert(v);
        }
        return v;
    }
    void clear(Value max) {
        assert(max > 0);
        max_ = max;
        set_.clear();
    }
};


template <typename Random>
class BoolRandom
{
    Random rand_;
    typename Random::ResultType value_;
    uint16_t counts_;
public:
    explicit BoolRandom(Random& rand) : rand_(rand), value_(0), counts_(0) {}
    bool operator()() {
        if (counts_ == 0) {
            value_ = rand_();
            counts_ = sizeof(typename Random::ResultType) * 8;
        }
        const bool ret = value_ & 0x1;
        value_ >>= 1;
        --counts_;
        return ret;
    }
    void reset() {
        counts_ = 0;
    }
};


struct RetryCounts
{
    using Pair = std::pair<size_t, size_t>;

    std::unordered_map<size_t, size_t> retryCounts;

    void add(size_t nrRetry, size_t nr = 1) {
        std::unordered_map<size_t, size_t>::iterator it = retryCounts.find(nrRetry);
        if (it == retryCounts.end()) {
            retryCounts.emplace(nrRetry, nr);
        } else {
            it->second += nr;
        }
    }
    void merge(const RetryCounts& rhs) {
        for (const Pair& p : rhs.retryCounts) {
            add(p.first, p.second);
        }
    }
    friend std::ostream& out(std::ostream& os, const RetryCounts& rc, bool verbose) {
        std::vector<Pair> v;
        v.reserve(rc.retryCounts.size());
        for (const Pair& p : rc.retryCounts) {
            v.push_back(p);
        }
        std::sort(v.begin(), v.end());

#if 0
        if (verbose) {
            for (const Pair& p : v) {
                os << cybozu::util::formatString("%5zu %zu\n", p.first, p.second);
            }
        } else {
            if (!v.empty()) {
                os << cybozu::util::formatString("max_retry %zu", v.back().first);
            } else {
                os << "max_retry 0";
            }
        }
#else
        unused(verbose);
#endif
        return os;
    }
    friend std::ostream& operator<<(std::ostream& os, const RetryCounts& rc) {
        return out(os, rc, true);
    }
    std::string str(bool verbose = true) const {
        std::stringstream ss;
        out(ss, *this, verbose);
        return ss.str();
    }
};


struct Result1
{
    RetryCounts rcS;
    RetryCounts rcL;
    size_t value[6];
    Result1() : rcS(), rcL(), value() {}
    void operator+=(const Result1& rhs) {
        rcS.merge(rhs.rcS);
        rcL.merge(rhs.rcL);
        for (size_t i = 0; i < 6; i++) {
            value[i] += rhs.value[i];
        }
    }
    size_t nrCommit() const { return value[0] + value[1]; }
    void incCommit(bool isLongTx) { value[isLongTx ? 1 : 0]++; }
    void addCommit(bool isLongTx, size_t v) { value[isLongTx ? 1 : 0] += v; }
    void incAbort(bool isLongTx) { value[isLongTx ? 3 : 2]++; }
    void incIntercepted(bool isLongTx) { value[isLongTx ? 5 : 4]++; }
    void addRetryCount(bool isLongTx, size_t nrRetry) {
#if 0
        if (isLongTx) {
            rcL.add(nrRetry);
        } else {
            rcS.add(nrRetry);
        }
#else
        unused(isLongTx);
        unused(nrRetry);
#endif
    }

    friend std::ostream& operator<<(std::ostream& os, const Result1& res) {
        const bool verbose = false; // QQQ
#if 0
        os << cybozu::util::formatString(
            "commit S %zu L %zu  abort S %zu L %zu  intercepted S %zu L %zu"
            , res.value[0], res.value[1]
            , res.value[2], res.value[3]
            , res.value[4], res.value[5]);
        os << "  ";
        out(os, res.rcS, verbose);
        os << "  ";
        out(os, res.rcL, verbose);
        os << "\n";
        return os;
#else
        os << cybozu::util::formatString(
            "commitS:%zu commitL:%zu abortS:%zu abortL:%zu interceptedS:%zu interceptedL:%zu"
            , res.value[0], res.value[1]
            , res.value[2], res.value[3]
            , res.value[4], res.value[5]);
        if (verbose) {
            // not yet implemented.
        }
        return os;
#endif
    }
    std::string str() const {
        std::stringstream ss;
        ss << *this;
        return ss.str();
    }
};


/**
 * For workloads with several kinds of long transactions.
 */
struct Result2
{
    struct Data {
        size_t txSize;

        size_t nrCommit;
        size_t nrAbort;

        Data() : nrCommit(0), nrAbort(0) {
        }

        void operator+=(const Data& rhs) {
            nrCommit += rhs.nrCommit;
            nrAbort += rhs.nrAbort;
        }
    };

    using Umap = std::unordered_map<size_t, Data>;
    Umap umap_;  // key: txSize

    void incCommit(size_t txSize) {
        umap_[txSize].nrCommit++;
    }

    void incAbort(size_t txSize) {
        umap_[txSize].nrAbort++;
    }

    void addRetryCount(size_t txSize, size_t nrRetry) {
        unused(txSize);
        unused(nrRetry);
        // not implemented yet.
    }

    size_t nrCommit() const {
        size_t total = 0;
        for (const Umap::value_type &p : umap_) {
            total += p.second.nrCommit;
        }
        return total;
    }

    void operator+=(const Result2& res) {
        for (const Umap::value_type &p : res.umap_) {
            umap_[p.first] += p.second;
        }
    }

    std::string str() const {
        std::vector<Data> v;
        v.reserve(umap_.size());
        for (const Umap::value_type &p : umap_) {
            v.push_back(p.second);
            v.back().txSize = p.first;
        }
        std::sort(v.begin(), v.end(), [](const Data &a, const Data &b) {
                return a.txSize < b.txSize;
            });

        std::stringstream ss;
        for (const Data& d : v) {
            ss << " " << "nrCommit_" << d.txSize << ":" << d.nrCommit;
            ss << " " << "nrAbort_" << d.txSize << ":" << d.nrAbort;
        }
        return ss.str();
    }
};


void waitForAllTrue(const std::vector<uint8_t>& v)
{
    for (;;) {
        if (std::all_of(v.cbegin(), v.cend(), [](const uint8_t& b) { return loadAcquire(b) != 0; })) {
            break;
        }
        sleepMs(100);
    }
}


template <typename SharedData, typename Worker, typename Result>
void runExec(const CmdLineOption& opt, SharedData& shared, Worker&& worker, Result& res)
{
    const size_t nrTh = opt.nrTh;

    bool start = false;
    bool quit = false;
    bool shouldQuit = false;
    std::vector<uint8_t> readyV(nrTh, 0);
    cybozu::thread::ThreadRunnerSet thS;
    std::vector<Result> resV(nrTh);
    for (size_t i = 0; i < nrTh; i++) {
        thS.add([&,i]() {
            resV[i] = worker(i, readyV[i], start, quit, shouldQuit, shared);
        });
    }
    thS.start();
    waitForAllTrue(readyV);
    storeRelease(start, true);
    size_t sec = 0;
    for (size_t i = 0; i < opt.runSec; i++) {
        if (opt.verbose) {
            ::printf("%zu\n", i);
        }
        sleepMs(1000);
        sec++;
        if (shouldQuit) break;
    }
    storeRelease(quit, true);
    thS.join();
    for (size_t i = 0; i < nrTh; i++) {
        if (opt.verbose) {
            ::printf("worker %zu  %s\n", i, resV[i].str().c_str());
        }
        res += resV[i];
    }
    ::printf("%s tps:%.03f %s\n"
             , opt.str().c_str()
             , res.nrCommit() / (double)opt.runSec
             , res.str().c_str());
    ::fflush(::stdout);
}

enum TxMode
{
    USE_LAST_WRITE_TX = 0,
    USE_FIRST_WRITE_TX = 1,
    USE_READONLY_TX = 2,
    USE_WRITEONLY_TX = 3,
    USE_HALF_AND_HALF_TX = 4,
    USE_MIX_TX = 5,
    USE_LAST_WRITE_HC_TX = 6,
    USE_FIRST_WRITE_HC_TX = 7,
    USE_LAST_WRITE_SAME_TX = 8,
    USE_FIRST_WRITE_SAME_TX = 9,
    TXMODE_MAX = 10,
};

enum TxIdGenType
{
    SCALABLE_TXID_GEN = 0,
    BULK_TXID_GEN = 1,
    SIMPLE_TXID_GEN = 2,
    EPOCH_TXID_GEN = 3,
};


#if 0 // deprecated
template <typename Random, typename Mode>
class GetModeFunc
{
    Mode (GetModeFunc::*getMode_)(size_t);
    BoolRandom<Random>& boolRand_;
    const std::vector<bool>& isWriteV_;
    size_t sz_;
    size_t nrWr_;

public:
    GetModeFunc(BoolRandom<Random>& boolRand, const std::vector<bool>& isWriteV,
                bool isLongTx, int shortTxMode, int longTxMode, size_t sz, size_t nrWr)
        : getMode_(), boolRand_(boolRand), isWriteV_(isWriteV), sz_(sz), nrWr_(nrWr) {
        if (isLongTx) {
            switch (longTxMode) {
            case USE_HALF_AND_HALF_TX:
                getMode_ = &GetModeFunc::getLong<USE_HALF_AND_HALF_TX>;
                break;
            case USE_READONLY_TX:
                getMode_ = &GetModeFunc::getLong<USE_READONLY_TX>;
                break;
            case USE_FIRST_WRITE_TX:
                getMode_ = &GetModeFunc::getLong<USE_FIRST_WRITE_TX>;
                break;
            default:
                getMode_ = &GetModeFunc::getLong<USE_LAST_WRITE_TX>;
            }
        } else {
            switch (shortTxMode) {
            case USE_MIX_TX:
                getMode_ = &GetModeFunc::getShort<USE_MIX_TX>;
                break;
            case USE_HALF_AND_HALF_TX:
                getMode_ = &GetModeFunc::getShort<USE_HALF_AND_HALF_TX>;
                break;
            case USE_READONLY_TX:
                getMode_ = &GetModeFunc::getShort<USE_READONLY_TX>;
                break;
            case USE_WRITEONLY_TX:
                getMode_ = &GetModeFunc::getShort<USE_WRITEONLY_TX>;
                break;
            case USE_FIRST_WRITE_TX:
                getMode_ = &GetModeFunc::getShort<USE_FIRST_WRITE_TX>;
                break;
            case USE_LAST_WRITE_HC_TX:
                getMode_ = &GetModeFunc::getShort<USE_LAST_WRITE_HC_TX>;
                break;
            default:
                getMode_ = &GetModeFunc::getShort<USE_LAST_WRITE_TX>;
            }
        }
    }
    Mode operator()(size_t i) {
        return (this->*getMode_)(i);
    }
    template <int shortTxMode>
    Mode getShort(size_t i) {
        switch (shortTxMode) {
        case USE_MIX_TX:
            return isWriteV_[i] ? Mode::X : Mode::S;
        case USE_HALF_AND_HALF_TX:
            return boolRand_() ? Mode::X : Mode::S;
        case USE_READONLY_TX:
            return Mode::S;
        case USE_WRITEONLY_TX:
            return Mode::X;
        case USE_FIRST_WRITE_TX:
        case USE_FIRST_WRITE_HC_TX:
            return i < nrWr_ ? Mode::X : Mode::S;
        default:
            assert(shortTxMode == USE_LAST_WRITE_TX || shortTxMode == USE_LAST_WRITE_HC_TX);
            return i >= sz_ - nrWr_ ? Mode::X : Mode::S;
        }
    }
    template <int longTxMode>
    Mode getLong(size_t i) {
        switch (longTxMode) {
        case USE_HALF_AND_HALF_TX:
            return boolRand_() ? Mode::X : Mode::S;
        case USE_READONLY_TX:
            return Mode::S;
        case USE_FIRST_WRITE_TX:
            return i < nrWr_ ? Mode::X : Mode::S;
        default:
            assert(longTxMode == USE_LAST_WRITE_TX);
            return i >= sz_ - nrWr_ ? Mode::X : Mode::S;
        }
    }
};
#endif


template <typename Random, typename Mode>
INLINE Mode getMode(Random& rand, BoolRandom<Random>& boolRand, const std::vector<bool>& isWriteV,
                    bool isLongTx, int shortTxMode, int longTxMode,
                    size_t nrOp, size_t nrWr, size_t i)
{
    if (isLongTx) {
        switch (longTxMode) {
        case USE_HALF_AND_HALF_TX:
            return boolRand() ? Mode::X : Mode::S;
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
        case USE_HALF_AND_HALF_TX:
            return boolRand() ? Mode::X : Mode::S;
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


#if 0 // deprected.
template <typename Random>
class GetRecordIdxFunc
{
    size_t (GetRecordIdxFunc::*getIdx_)(size_t);
    Random& rand_;
    size_t nrMu_;
    size_t nrOp_;
    int firstRecIdx_; // state for USE_LAST_WRITE_HC_TX.

public:
    GetRecordIdxFunc(Random& rand, bool isLongTx, int shortTxMode, int longTxMode, size_t nrMu, size_t nrOp)
        : getIdx_(), rand_(rand), nrMu_(nrMu), nrOp_(nrOp), firstRecIdx_() {
        if (isLongTx) {
            unused(longTxMode);
            getIdx_ = &GetRecordIdxFunc::getLong;
        } else {
            switch (shortTxMode) {
            case USE_LAST_WRITE_HC_TX:
                getIdx_ = &GetRecordIdxFunc::getShort<USE_LAST_WRITE_HC_TX>;
                if (nrOp_ < 2) {
                    throw cybozu::Exception("USE_LAST_WRITE_HC_TX requires more than 2 mutexes.");
                }
                break;
            default:
                getIdx_ = &GetRecordIdxFunc::getShort<TXMODE_MAX>;
            }
        }
    }

    size_t operator()(size_t i) {
        return (this->*getIdx_)(i);
    }

    size_t getLong(size_t) {
        return rand_() % nrMu_;
    }

    template <int shortTxMode>
    size_t getShort(size_t i) {
        switch (shortTxMode) {
        case USE_LAST_WRITE_HC_TX:
            if (i == 0) {
                firstRecIdx_ = rand_() & 0x1;
                return firstRecIdx_;
            } else if (i == nrOp_ - 1) {
                return 1 - firstRecIdx_;
            } else {
                return rand() % nrMu_;
            }
        default:
            return rand_() % nrMu_;
        }
    }
};
#endif

template <typename Random>
INLINE size_t getRecordIdx(Random& rand, bool isLongTx, int shortTxMode, int longTxMode,
                           size_t nrMu, size_t nrOp, size_t i, size_t& firstRecIdx,
                           bool usesZipf, FastZipf& fastZipf)
{
    unused(longTxMode);
    if (isLongTx) {
        if (longTxMode == USE_FIRST_WRITE_SAME_TX) {
            if (i == 0) {
                return 0;
            } else {
                assert(nrMu > 1);
                return rand() % (nrMu - 1) + 1; // non-zero.
            }
        }
        if (longTxMode == USE_LAST_WRITE_SAME_TX) {
            if (i == nrOp - 1) {
                return 0;
            } else {
                assert(nrMu > 1);
                return rand() % (nrMu - 1) + 1; // non-zero.
            }
        }
    } else {
        if (shortTxMode == USE_LAST_WRITE_HC_TX || shortTxMode == USE_FIRST_WRITE_HC_TX) {
            if (i == 0) {
                firstRecIdx = rand() & 0x1;
                return firstRecIdx;
            } else if (i == nrOp - 1) {
                return 1 - firstRecIdx;
            } else {
                return rand() % nrMu;
            }
        }
        if (shortTxMode == USE_FIRST_WRITE_SAME_TX) {
            if (i == 0) {
                assert(nrMu > 0);
                //return rand() % std::min<size_t>(10, nrMu); // typically first 10 records.
                return 0;
            } else {
                assert(nrMu > 1);
                return rand() % (nrMu - 1) + 1; // non-zero.
            }
        }
        if (shortTxMode == USE_LAST_WRITE_SAME_TX) {
            if (i == nrOp - 1) {
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
void backOff(uint64_t& t0, size_t retry, Random& rand)
{
    const uint64_t t1 = cybozu::time::rdtscp();
    const uint64_t tdiff = std::max<uint64_t>(t1 - t0, 2);
    auto randState = rand.getState();
    randState += retry;
    rand.setState(randState);
    uint64_t waitTic = rand() % (tdiff << std::min<size_t>(retry + 1, 4));
    //uint64_t waitTic = rand() % (tdiff << 18);
    uint64_t t2 = t1;
    while (t2 - t1 < waitTic) {
        _mm_pause();
        t2 = cybozu::time::rdtscp();
    }
    t0 = t2;
}


template <typename Vec, typename Opt>
void initRecordVector(Vec& v, const Opt& opt)
{
#ifdef USE_PARTITION
#ifdef MUTEX_ON_CACHELINE
    v.setSizes(opt.nrTh, opt.getNrMuPerTh(), opt.payload, CACHE_LINE_SIZE);
#else
    v.setSizes(opt.nrTh, opt.getNrMuPerTh(), opt.payload);
#endif
#else
#ifdef MUTEX_ON_CACHELINE
    v.setPayloadSize(opt.payload, CACHE_LINE_SIZE);
#else
    v.setPayloadSize(opt.payload);
#endif
    v.resize(opt.getNrMu());
#endif
}
