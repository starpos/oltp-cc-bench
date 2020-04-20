#include "wait_die.hpp"
#include "cybozu/test.hpp"

using namespace cybozu::wait_die;

using Lock = WaitDieLock4;
using Mode = WaitDieLock4::Mode;
using Mutex = WaitDieLock4::Mutex;
using Header = Mutex::Header;
using Message = Mutex::Message;
using Request = Mutex::Request;


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
