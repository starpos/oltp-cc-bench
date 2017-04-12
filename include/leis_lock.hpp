#pragma once
/**
 * @file
 * brief Leis2016 algorithm (reader-writer version).
 * @author Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 *
 * (C) 2016 Cybozu Labs, Inc.
 */

#include <map>
#include <vector>
#include "lock.hpp"

namespace cybozu {
namespace lock {

/**
 * Usage:
 *   Call lock() to lock a resource.
 *   If lock() returns true, continue your transaction.
 *   Otherwise, you must rollback transaction's change, then call recover(),
 *   then restart your transaction (some locks are kept).
 *   In the end of your transaction, call unlock() to release all locks.
 */
class LeisLockSet
{
public:
    using Lock = XSLock;
    using Mutex = Lock::Mutex;
    using Mode = Mutex::Mode;

private:
    using Map = std::map<Mutex*, Lock>;
    Map map_;

    // Temporary used in recover().
    std::vector<bool> tmp_;

    // Temporary variables for lock() and recover().
    Mutex *mutex_;
    Mode mode_;
    Map::iterator bgn_; // begin of M.

public:
    ~LeisLockSet() noexcept {
        unlock();
    }
    bool lock(Mutex *mutex, Mode mode) {
        Map::iterator it = map_.lower_bound(mutex);
        if (it == map_.end()) {
            // M is empty.
            map_.emplace(mutex, Lock(mutex, mode)); // blocking
            return true;
        }
        Mutex *mu = it->first;
        if (mu == mutex) { // already exists
            Lock& lk = it->second;
            if (mode == Mode::X && lk.isShared()) {
                if (lk.tryUpgrade()) {
                    return true;
                }
            } else {
                return true;
            }
        }
        {
            Lock lk;
            if (lk.tryLock(mutex, mode)) { // non-blocking.
                map_.emplace(mutex, std::move(lk));
                return true;
            }
        }

        mutex_ = mutex;
        mode_ = mode;
        bgn_ = it;
        return false;
    }
    void recover() {
        // release locks in M.
        assert(tmp_.empty());
        Map::iterator it = bgn_;
        while (it != map_.end()) {
            Lock& lk = it->second;
            tmp_.push_back(lk.isShared());
            lk.unlock();
            ++it;
        }

        // lock the target.
        it = bgn_;
        size_t i = 0;
        if (bgn_ != map_.end() && bgn_->first == mutex_) {
            Lock& lk = it->second;
            assert(tmp_[0]); // isShared.
            assert(mode_ == Mode::X);
            lk.lock(mutex_, mode_); // blocking.
            ++it;
            i = 1;
        } else {
            map_.emplace(mutex_, Lock(mutex_, mode_)); // blocking
        }

        // re-lock mutexes in M.
        while (it != map_.end()) {
            Mutex *mu = it->first;
            Lock& lk = it->second;
            assert(i < tmp_.size());
            lk.lock(mu, tmp_[i] ? Mode::S : Mode::X); // blocking
            ++it;
            ++i;
        }

        tmp_.clear();
    }
    void unlock() noexcept {
#if 1
        map_.clear(); // automatically unlocked in their destructor.
#else
        // debug code.
        Map::iterator it = map_.begin();
        while (it != map_.end()) {
            Mutex *mu = it->first;
            it = map_.erase(it);
        }
#endif
    }
    bool empty() const {
        return map_.empty();
    }
    size_t size() const {
        return map_.size();
    }
};

}} // namespace cybozu::lock
