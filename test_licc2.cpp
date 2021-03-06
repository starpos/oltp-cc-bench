#include <vector>
#include "cybozu/test.hpp"
#include "licc2.hpp"
#include <cstdio>
#include <thread>


using namespace cybozu::lock::licc2;



template <typename C>
void verify_possible(const C& moc_v)
{
    static_assert(std::is_same<typename C::value_type, MutexOpCreator>::value);
    for (const MutexOpCreator& moc : moc_v) {
        ::printf("%s\n", moc.str().c_str());
        CYBOZU_TEST_ASSERT(moc.possible());
    }
}


void verify_unlockable(const MutexOpCreator& moc0)
{
    MutexOpCreator moc1 = moc0.unlock_general();
    CYBOZU_TEST_ASSERT(moc1.possible());
    CYBOZU_TEST_EQUAL(moc1.ld.state, LockState::INIT);
}


CYBOZU_TEST_AUTO(test_read_reserve)
{
    MutexData md0; md0.init();
    uint32_t ord_id = 10;
    LockData ld0(ord_id);

    std::vector<MutexOpCreator> v;
    v.emplace_back(ld0, md0);
    verify_unlockable(v.back());
    v.push_back(v.back().reserve<LockState::READ, false>());
    verify_unlockable(v.back());
    v.push_back(v.back().reserve<LockState::READ, true>());
    verify_unlockable(v.back());
    v.push_back(v.back().unlock_special<LockState::READ>());

    verify_possible(v);
}


CYBOZU_TEST_AUTO(test_read_reserve_and_upgrade)
{
    MutexData md0; md0.init();
    uint32_t ord_id = 10;
    LockData ld0(ord_id);

    std::vector<MutexOpCreator> v;
    v.emplace_back(ld0, md0);
    v.push_back(v.back().reserve<LockState::READ, false>());
    verify_unlockable(v.back());
    v.push_back(v.back().reserve<LockState::READ, true>());
    verify_unlockable(v.back());
    v.push_back(v.back().reserve<LockState::READ_MODIFY_WRITE, true>());
    verify_unlockable(v.back());
    v.push_back(v.back().protect<true>());
    v.push_back(v.back().unlock_special<LockState::PROTECTED>());

    verify_possible(v);
}


CYBOZU_TEST_AUTO(test_blind_write_reserve)
{
    MutexData md0; md0.init();
    uint32_t ord_id = 10;
    LockData ld0(ord_id);

    std::vector<MutexOpCreator> v;
    v.emplace_back(ld0, md0);
    v.push_back(v.back().blind_write());
    verify_unlockable(v.back());
    v.push_back(v.back().reserve<LockState::BLIND_WRITE, false>());
    verify_unlockable(v.back());
    v.push_back(v.back().reserve<LockState::BLIND_WRITE, false>());
    verify_unlockable(v.back());
    v.push_back(v.back().protect<false>());
    v.push_back(v.back().unlock_special<LockState::PROTECTED>());

    for (MutexOpCreator& moc : v) {
        ::printf("%s\n", moc.str().c_str());
        CYBOZU_TEST_ASSERT(moc.possible());
    }
}


void worker()
{


}


#if 0
CYBOZU_TEST_AUTO(test_multi_thread_read)
{


    std::vector<std::thread> th_v;

    th_v.emplace_back();







}
#endif
