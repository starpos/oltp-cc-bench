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
#include <array>
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


/**
 * To enable retry counting.
 * This takes a bit overhead.
 */
#if 0
#define USE_RETRY_COUNT
#else
#undef USE_RETRY_COUNT
#endif

/**
 * To enable latency histogram.
 * This takes a bit overhead.
 */
#if 0
#define USE_LATENCY_HISTOGRAM
#else
#undef USE_LATENCY_HISTOGRAM
#endif


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


#if 0
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
#endif


template <typename Random>
INLINE void fillModeVec2(std::vector<bool>& isWriteV, Random& rand, size_t wrPct)
{
    const size_t max = isWriteV.size();
    assert(wrPct >= 0 && wrPct <= 100);
    for (size_t i = 0; i < max; i++) {
        isWriteV[i] = (rand() % 100 < wrPct);
    }
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


/**
 * Histogram of size_t values.
 */
struct Histogram
{
    static constexpr size_t HISTOGRAM_SIZE = sizeof(size_t) * 8;
    std::array<size_t, HISTOGRAM_SIZE + 1> data;

    Histogram() : data() {
        for (auto& e : data) e = 0;
    }
    void add(size_t value) {
        if (value == 0) {
            data[0]++;
            return;
        }
        static_assert(sizeof(unsigned long) == sizeof(size_t));
        const size_t b = __builtin_clzl(value);
        assert(b >= 0);
        assert(b < HISTOGRAM_SIZE);
        data[HISTOGRAM_SIZE - b]++;  // HISTOGRAM_SIZE - b is in range [1, 64].
    }
    void merge(const Histogram& rhs) {
        for (size_t i = 0; i <= HISTOGRAM_SIZE; i++) {
            this->data[i] += rhs.data[i];
        }
    }
    const size_t& operator[](size_t i) const {
#ifndef NDEBUG
        if (i > HISTOGRAM_SIZE) {
            throw cybozu::Exception("Histogram::operator[] error") << i;
        }
#endif
        return data[i];
    }

    /**
     * 0: xxx
     * 1: xxx
     * 2: xxx 2, 3
     * 2^2: xxx [4, 8)
     * 2^3: xxx [8, 16)
     * 2^4: xxx [16, 32)
     * ...
     * 2^63: xxx [2^63, 2^64)
     */
    void put_to(std::ostream& os) const {
        size_t max = HISTOGRAM_SIZE + 1;
        while (max > 0) {
            if (data[max - 1] != 0) break;
            --max;
        }
        for (size_t i = 0; i <= 2; i++) {
            os << i << ": " << data[i] << "\n";
        }
        for (size_t i = 3; i < max; i++) {
            os << "2^" << (i - 1) << ": " << data[i] << "\n";
        }
    }
};


std::ostream& operator<<(std::ostream& os, const Histogram& h)
{
    h.put_to(os);
    return os;
}


struct RetryCounts
{
    using Umap = std::unordered_map<size_t, size_t>;
    using Pair = std::pair<size_t, size_t>;
    Umap retryCounts;

    void add(size_t nrRetry, size_t nr = 1) {
        std::unordered_map<size_t, size_t>::iterator it = retryCounts.find(nrRetry);
        if (it == retryCounts.end()) {
            retryCounts.emplace(nrRetry, nr);
        } else {
            it->second += nr;
        }
    }
    void merge(const RetryCounts& rhs) {
        for (const Umap::value_type& p : rhs.retryCounts) {
            add(p.first, p.second);
        }
    }
    friend std::ostream& out(std::ostream& os, const RetryCounts& rc, bool verbose) {
        std::vector<Pair> v;
        v.reserve(rc.retryCounts.size());
        for (const Umap::value_type& p : rc.retryCounts) {
            v.push_back(p);
        }
        std::sort(v.begin(), v.end());

#ifdef USE_RETRY_COUNT
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
    Histogram latencyH;
    size_t value[6];
    Result1() : rcS(), rcL(), value() {}
    void operator+=(const Result1& rhs) {
        rcS.merge(rhs.rcS);
        rcL.merge(rhs.rcL);
        latencyH.merge(rhs.latencyH);
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
        unused(isLongTx, nrRetry);
#ifdef USE_RETRY_COUNT
        if (isLongTx) {
            rcL.add(nrRetry);
        } else {
            rcS.add(nrRetry);
        }
#endif
    }
    void addLatency(size_t latency) {
        unused(latency);
#ifdef USE_LATENCY_HISTOGRAM
        latencyH.add(latency);
#endif
    }
    friend std::ostream& operator<<(std::ostream& os, const Result1& res) {
        os << cybozu::util::formatString(
            "commitS:%zu commitL:%zu abortS:%zu abortL:%zu interceptedS:%zu interceptedL:%zu"
            , res.value[0], res.value[1]
            , res.value[2], res.value[3]
            , res.value[4], res.value[5]);
#ifdef USE_RETRY_COUNT
        os << "\n" << "  " << res.rcS << "  " << res.rcL;
#endif
#ifdef USE_LATENCY_HISTOGRAM
        os << "\n" << res.latencyH;
#endif
        return os;
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
            try {
                resV[i] = worker(i, readyV[i], start, quit, shouldQuit, shared);
            } catch (std::exception& e) {
                ::fprintf(::stderr, "error workerid:%zu message:%s\n", i, e.what());
            } catch (...) {
                ::fprintf(::stderr, "unknown error workerid:%zu\n", i);
            }
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
