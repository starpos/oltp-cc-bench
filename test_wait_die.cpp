#include <thread>
#include <vector>
#include <cstring>
#include <sstream>
#include "wait_die.hpp"
#include "random.hpp"
#include "tx_util.hpp"
#include "sleep.hpp"
#include "cache_line_size.hpp"
#include "cybozu/test.hpp"
#include "cybozu/array.hpp"


using namespace cybozu::wait_die;

using Lock = WaitDieLock4;
using Mode = WaitDieLock4::Mode;
using Mutex = WaitDieLock4::Mutex;
using Header = Mutex::Header;
using Message = Mutex::Message;
using Request = Mutex::Request;


/**
 * WaitDieLock4 test.
 */
CYBOZU_TEST_AUTO(simple_mutex_test)
{
    Mutex mutex;

    // unlocked.
    {
        Request req(10, RequestType::READ_LOCK);
        bool ret = mutex.do_request(req);
        CYBOZU_TEST_EQUAL(ret, true);
        const Header h0 = mutex.load_header();
        CYBOZU_TEST_EQUAL(h0.tx_id, 10U);
        CYBOZU_TEST_EQUAL(h0.readers, 1U);
        CYBOZU_TEST_ASSERT(h0.is_read_locked());
    }
    // read_locked 1.
    {
        Request req(10, RequestType::READ_UNLOCK);
        bool ret = mutex.do_request(req);
        CYBOZU_TEST_EQUAL(ret, true);
        const Header h0 = mutex.load_header();
        CYBOZU_TEST_EQUAL(h0.tx_id, MAX_TXID);
        CYBOZU_TEST_ASSERT(h0.is_unlocked());
    }
    // unlocked.
    {
        Request req(10, RequestType::READ_LOCK);
        bool ret = mutex.do_request(req);
        CYBOZU_TEST_EQUAL(ret, true);
    }
    // read_locked 1
    {
        Request req(20, RequestType::READ_LOCK);
        // std::cout << mutex << std::endl;
        bool ret = mutex.do_request(req);
        // std::cout << mutex << std::endl;
        CYBOZU_TEST_EQUAL(ret, true);
        const Header h0 = mutex.load_header();
        CYBOZU_TEST_EQUAL(h0.tx_id, 10U);
        CYBOZU_TEST_EQUAL(h0.readers, 2U);
    }
    // read_locked 2
    {
        Request req(20, RequestType::UPGRADE);
        bool ret = mutex.do_request(req);
        CYBOZU_TEST_EQUAL(ret, false);
    }
    // read_locked 2
    {
        Request req(20, RequestType::READ_UNLOCK);
        bool ret = mutex.do_request(req);
        CYBOZU_TEST_EQUAL(ret, true);
        const Header h0 = mutex.load_header();
        CYBOZU_TEST_EQUAL(h0.tx_id, 10U);
        CYBOZU_TEST_EQUAL(h0.readers, 1U);
    }
    // read_locked 1
    {
        Request req(30, RequestType::WRITE_LOCK);
        bool ret = mutex.do_request(req);
        CYBOZU_TEST_EQUAL(ret, false);
    }
    // read_locked 1
    {
        Request req(10, RequestType::READ_UNLOCK);
        bool ret = mutex.do_request(req);
        CYBOZU_TEST_EQUAL(ret, true);
        const Header h0 = mutex.load_header();
        CYBOZU_TEST_EQUAL(h0.tx_id, MAX_TXID);
        CYBOZU_TEST_ASSERT(h0.is_unlocked());
    }
    // unlocked.
    {
        Request req(10, RequestType::WRITE_LOCK);
        bool ret = mutex.do_request(req);
        CYBOZU_TEST_EQUAL(ret, true);
        const Header h0 = mutex.load_header();
        CYBOZU_TEST_EQUAL(h0.tx_id, 10U);
        CYBOZU_TEST_ASSERT(h0.is_write_locked());
    }
    // write_locked.
    {
        Request req(20, RequestType::WRITE_LOCK);
        bool ret = mutex.do_request(req);
        CYBOZU_TEST_EQUAL(ret, false);
    }
    // write_locked.
    {
        Request req(20, RequestType::READ_LOCK);
        bool ret = mutex.do_request(req);
        CYBOZU_TEST_EQUAL(ret, false);
    }
    // write_locked.
    {
        Request req(10, RequestType::WRITE_UNLOCK);
        bool ret = mutex.do_request(req);
        CYBOZU_TEST_EQUAL(ret, true);
        const Header h0 = mutex.load_header();
        CYBOZU_TEST_EQUAL(h0.tx_id, MAX_TXID);
        CYBOZU_TEST_ASSERT(h0.is_unlocked());
    }
    // unlocked.
    {
        Request req(10, RequestType::READ_LOCK);
        bool ret = mutex.do_request(req);
        CYBOZU_TEST_EQUAL(ret, true);
    }
    // read locked 1.
    {
        Request req(10, RequestType::UPGRADE);
        bool ret = mutex.do_request(req);
        CYBOZU_TEST_EQUAL(ret, true);
        const Header h0 = mutex.load_header();
        CYBOZU_TEST_EQUAL(h0.tx_id, 10U);
        CYBOZU_TEST_ASSERT(h0.is_write_locked());
    }
    // write locked.
    {
        Request req(10, RequestType::WRITE_UNLOCK);
        bool ret = mutex.do_request(req);
        CYBOZU_TEST_EQUAL(ret, true);
        const Header h0 = mutex.load_header();
        CYBOZU_TEST_EQUAL(h0.tx_id, MAX_TXID);
        CYBOZU_TEST_ASSERT(h0.is_unlocked());
    }
    // unlocked.
    {
        const Header h0 = mutex.load_header();
        CYBOZU_TEST_ASSERT(h0.is_unlocked());
    }
}


/**
 * This is test for WaitDieLock4.
 */
CYBOZU_TEST_AUTO(simple_lock_test)
{
    Mutex mutex;

    {
        WaitDieLock4 lk;
        bool ret = lk.readLock(mutex, 10);
        CYBOZU_TEST_ASSERT(ret);
    }
    CYBOZU_TEST_ASSERT(mutex.load_header().is_unlocked());
    {
        WaitDieLock4 lk;
        bool ret = lk.writeLock(mutex, 10);
        CYBOZU_TEST_ASSERT(ret);
    }
    CYBOZU_TEST_ASSERT(mutex.load_header().is_unlocked());
    {
        WaitDieLock4 lk;
        bool ret = lk.readLock(mutex, 10);
        CYBOZU_TEST_ASSERT(ret);
        ret = lk.upgrade();
        CYBOZU_TEST_ASSERT(ret);
    }
    CYBOZU_TEST_ASSERT(mutex.load_header().is_unlocked());
}


struct Data
{
    cybozu::AlignedArray<uint8_t, CACHE_LINE_SIZE, false> data_;
    size_t nr_lines_;

    union U {
        uint64_t value;
        struct {
            uint32_t id;
            uint32_t count;
        };
    };

    U u_;

    std::vector<size_t> id_count_;
    size_t read_count_;
    size_t read_abort_;
    size_t write_abort_;

    void reset(uint32_t id, uint32_t max_id, size_t nr_lines) {
        u_.id = id;
        u_.count = 1;
        CYBOZU_TEST_ASSERT(nr_lines >= 2);
        nr_lines_ = nr_lines;
        data_.resize(CACHE_LINE_SIZE * nr_lines);
        ::memset(data_.data(), 0, data_.size());
        CYBOZU_TEST_ASSERT(id < max_id);
        id_count_.clear();
        id_count_.resize(max_id);
        read_count_ = 0;
        read_abort_ = 0;
        write_abort_ = 0;
    }
    void write(void* data_out) {
        const size_t limit = nr_lines_ * CACHE_LINE_SIZE;
        for (size_t pos = 0; pos < limit; pos += CACHE_LINE_SIZE) {
            ::memcpy(&data_[pos], &u_, sizeof(u_));
        }
        ::memcpy(data_out, data_.data(), data_.size());
        u_.count++;
    }
    void read(const void* data_in) {
        ::memcpy(data_.data(), data_in, data_.size());
        read_count_++;
    }
    void verify() {
        U u0;
        ::memcpy(&u0, &data_[0], sizeof(u0));
        const size_t limit = nr_lines_ * CACHE_LINE_SIZE;
        for (size_t pos = CACHE_LINE_SIZE; pos < limit; pos += CACHE_LINE_SIZE) {
            U u1;
            ::memcpy(&u1, &data_[pos], sizeof(u1));
            CYBOZU_TEST_EQUAL(u0.id, u1.id);
            CYBOZU_TEST_EQUAL(u0.count, u1.count);
        }
        id_count_[u0.id]++;
    }
    void add_read_abort() { read_abort_++; }
    void add_write_abort() { write_abort_++; }

    void print() const {
        if (id_count_.empty()) return;
        std::stringstream ss;
        ss << fmtstr("worker %zu read %zu (abort %zu) write %zu (abort %zu) id_count %zu"
                     , u_.id
                     , read_count_, read_abort_
                     , u_.count, write_abort_
                     , id_count_[0]);
        for (size_t i = 1; i < id_count_.size(); i++) {
            ss << fmtstr(", %zu", id_count_[i]);
        }
        ::printf("%s\n", ss.str().c_str());
    }
};


template <typename WaitDieLock>
void worker(size_t idx, size_t max_id, uint64_t seed, bool& quit,
            EpochGenerator& epochGen,
            typename WaitDieLock::Mutex& mutex,
            void* shared_data, size_t nr_lines, size_t max_retry)
{
    cybozu::util::Xoroshiro128Plus rand(seed + idx);
    EpochTxIdGenerator<9, 2> epochTxIdGen(idx + 1, epochGen);

    Data data;
    data.reset(idx, max_id, nr_lines);

    while (!load_acquire(quit)) {
        TxId tx_id = epochTxIdGen.get();
        if ((rand() & 0xff) < 0x80) {
            size_t retry = 0;
            for (; retry < max_retry; retry++) {
                WaitDieLock lk;
                if (!lk.readLock(mutex, tx_id)) {
                    continue;
                }
                data.read(shared_data);
                data.verify();
                break;
            }
            if (retry == max_retry) data.add_read_abort();
        } else {
            size_t retry = 0;
            for (; retry < max_retry; retry++) {
                WaitDieLock lk;
                if (!lk.writeLock(mutex, tx_id)) {
                    continue;
                }
                data.write(shared_data);
                break;
            }
            if (retry == max_retry) data.add_write_abort();
        }
    }
#if 0
    data.print();
#endif
}


template <typename WaitDieLock>
void test_read_write_lock(size_t nr_lines, size_t nr_threads, size_t period_ms, size_t max_retry)
{
    cybozu::AlignedArray<uint8_t, CACHE_LINE_SIZE, false> shared_data;
    shared_data.resize(CACHE_LINE_SIZE * nr_lines);
    Data data;
    data.reset(0, nr_threads, nr_lines);
    data.write(shared_data.data());

    CacheLineAligned<bool> quit(false);
    CacheLineAligned<typename WaitDieLock::Mutex> mutex;
    const uint64_t seed = ::time(0);
    EpochGenerator epochGen;

    std::vector<std::thread> th_v;
    for (size_t i = 0; i < nr_threads; i++) {
        th_v.emplace_back(worker<WaitDieLock>
                          , i, nr_threads, seed, std::ref(quit.value)
                          , std::ref(epochGen), std::ref(mutex.value)
                          , shared_data.data(), nr_lines, max_retry);
    }
    sleep_ms(period_ms);
    store_release(quit.value, true);
    for (size_t i = 0; i < nr_threads; i++) th_v[i].join();
}


CYBOZU_TEST_AUTO(read_write_lock_test)
{
#if 0
    using WaitDieLock = WaitDieLock2;
#elif 0
    using WaitDieLock = WaitDieLock3;
#elif 1
    using WaitDieLock = WaitDieLock4;
#endif
    test_read_write_lock<WaitDieLock>(4096 / CACHE_LINE_SIZE, 64, 1000, 100000);
}
