#pragma once
/**
 * Lock data.
 */
#include "util.hpp"
#include "lock.hpp"

namespace cybozu {
namespace lock {


#if 1
class LockStateXS
{
public:
    enum class Mode : uint8_t { INVALID = 0, X, S, };
private:
#if 1
    union {
        uint8_t obj;
        struct {
            uint8_t xFlag:1;
            uint8_t sCount:7;
        };
    };
#else
    uint8_t xFlag:1;
    uint8_t sCount:7;
#endif
public:
    LockStateXS() : obj(0) {}
    //LockStateXS() : xFlag(0), sCount(0) {}
    bool get(Mode mode) const {
        return getCount(mode) > 0;
    }
    uint32_t getCount(Mode mode) const {
        if (mode == Mode::X) {
            return xFlag;
        } else {
            assert(mode == Mode::S);
            return sCount;
        }
    }
    bool canSet(Mode mode) const {
        if (mode == Mode::X) {
            return isUnlocked();
        } else {
            assert(mode == Mode::S);
            return xFlag == 0 && sCount != 0x7f;
        }
    }
    void set(Mode mode) {
        assert(canSet(mode));
        if (mode == Mode::X) {
            xFlag = 1;
        } else {
            assert(mode == Mode::S);
            sCount++;
        }
    }
    bool canClear(Mode mode) const {
        if (mode == Mode::X) {
            return xFlag == 1;
        } else {
            assert(mode == Mode::S);
            return sCount != 0;
        }
    }
    void clear(Mode mode) {
        assert(canClear(mode));
        if (mode == Mode::X) {
            xFlag = 0;
        } else {
            assert(mode == Mode::S);
            sCount--;
        }
    }
    void clearAll() {
#if 0
        xFlag = 0;
        sCount = 0;
#else
        obj = 0;
#endif
    }
    bool isUnlocked() const {
#if 0
            return xFlag == 0 && sCount == 0;
#else
            return obj == 0;
#endif
    }
    std::string str() const {
        return cybozu::util::formatString(
            "X %u S %u"
            , getCount(Mode::X)
            , getCount(Mode::S));
    }
};
static_assert(sizeof(LockStateXS) == 1, "sizeof(LockStateXS) must be 1");
#else
class LockStateXS
{
public:
    enum class Mode : uint8_t { INVALID = 0, X, S, };
private:
    /**
     * 0-6(7bit): S lock count.
     * 7(1bit): X lock flag.
     */
    uint8_t state_;
public:
    LockStateXS() : state_(0) {}
    bool get(Mode mode) const {
        return getCount(mode) > 0;
    }
    uint32_t getCount(Mode mode) const {
        switch (mode) {
        case Mode::X:
            return (state_ & (0x1 << 7)) == 0 ? 0 : 1;
        case Mode::S:
            return (state_ & ~(0x1 << 7));
        default:
            throw std::runtime_error("LockStateXS::getCount(): bad mode");
        }
    }
    bool canSet(Mode mode) const {
        uint8_t tmp = state_;
        return setInternal(mode, tmp);
    }
    void set(Mode mode) {
        setInternal(mode, state_);
    }
    bool canClear(Mode mode) const {
        uint8_t tmp = state_;
        return clearInternal(mode, tmp);
    }
    void clear(Mode mode) {
        clearInternal(mode, state_);
    }
    void clearAll() {
        state_ = 0;
    }
    bool isUnlocked() const {
        return state_ == 0;
    }
    std::string str() const {
        return cybozu::util::formatString(
            "X %u S %u"
            , getCount(Mode::X)
            , getCount(Mode::S));
    }
private:
    bool setInternal(Mode mode, uint8_t& state) const {
        switch (mode) {
        case Mode::X:
            if (!isUnlocked()) return false;
            state |= (0x1 << 7);
            return true;
        case Mode::S:
            if (get(Mode::X)) return false;
            if ((state & ~(0x1 << 7)) == 0x7f) return false;
            state++;
            return true;
        default:
            throw std::runtime_error("LockStateXS::setInternal(): bad mode");
        }
    }
    bool clearInternal(Mode mode, uint8_t& state) const {
        if (!get(mode)) return false;
        switch (mode) {
        case Mode::X:
            state &= ~(0x1 << 7);
            return true;
        case Mode::S:
            state--;
            return true;
        default:
            throw std::runtime_error("LockStateXS::clearInternal(): bad mode");
        }
    }
};
#endif

class LockStateMG
{
public:
    enum class Mode : uint8_t { INVALID = 0, X, S, IX, IS, SIX, };
private:
    /**
     *   0-6(7bit):   S or IX lock count.
     *   7(1bit):     X lock flag.
     *   8(1bit):     SIX lock flag.
     *   9(1bit):     S if 0, IX if 1.
     *   10-15(6bit): IS lock count.
     */
    uint16_t state_;
public:
    LockStateMG() : state_(0) {}
    bool get(Mode mode) const {
        return getCount(mode) > 0;
    }
    uint32_t getCount(Mode mode) const {
        switch (mode) {
        case Mode::X:
            return (state_ & (0x1 << 7)) != 0 ? 1 : 0;
        case Mode::S:
            if ((state_ & (0x1 << 9)) != 0) return 0;
            return state_ & 0x7F;
        case Mode::IX:
            if ((state_ & (0x1 << 9)) == 0) return 0;
            return state_ & 0x7F;
        case Mode::IS:
            return (state_ >> 10) & 0x3F;
        case Mode::SIX:
            return (state_ & (0x1 << 8)) != 0 ? 1 : 0;
        default:
            throw std::runtime_error("LockStateMG::getCount(): bad mode");
        }
    }
    bool canSet(Mode mode) const {
        uint16_t tmp = state_;
        return setInternal(mode, tmp);
    }
    void set(Mode mode) {
        setInternal(mode, state_);
    }
    bool canClear(Mode mode) const {
        uint16_t tmp = state_;
        return clearInternal(mode, tmp);
    }
    void clear(Mode mode) {
        clearInternal(mode, state_);
    }
    void clearAll() {
        state_ = 0;
    }
    bool isUnlocked() const {
        // return !getAny({Mode::X, Mode::S, Mode::IX, Mode::IS, Mode::SIX});
        return (state_ & ~(0x1 << 9)) == 0;
    }
    /**
     * Debug
     */
    std::string str() const {
        return cybozu::util::formatString(
            "X %u S %u IX %u IS %u SIX %u"
            , getCount(Mode::X)
            , getCount(Mode::S)
            , getCount(Mode::IX)
            , getCount(Mode::IS)
            , getCount(Mode::SIX));
    }
private:
    bool getAny(std::initializer_list<Mode> li) const {
        for (Mode mode : li) {
            if (get(mode)) return true;
        }
        return false;
    }
    bool setInternal(Mode mode, uint16_t &state) const {
        uint16_t tmp;
        Mode m;
        switch (mode) {
        case Mode::X:
            if (!isUnlocked()) return false;
            state |= (0x1 << 7);
            return true;
        case Mode::S:
        case Mode::IX:
            m = (mode == Mode::S ? Mode::IX : Mode::S);
            if (getAny({m, Mode::SIX, Mode::X})) return false;
            tmp = state & 0x7F;
            if (tmp == 0x7F) return false; // counter is max.
            if (mode == Mode::S) {
                state &= ~(0x1 << 9);
            } else {
                state |= (0x1 << 9);
            }
            state++;
            return true;
        case Mode::IS:
            if (get(Mode::X)) return false;
            tmp = (state >> 10) & 0x3F;
            if (tmp == 0x3F) return false; // couinter is max.
            tmp++;
            state &= ~(0x3F << 10);
            state |= (tmp << 10);
            return true;
        case Mode::SIX:
            if (getAny({Mode::IX, Mode::S, Mode::SIX, Mode::X})) return false;
            state |= (0x1 << 8);
            return true;
        default:
            throw std::runtime_error("LockStateMG::setInternal(): bad mode.");
        }
    }
    bool clearInternal(Mode mode, uint16_t& state) const {
        if (!get(mode)) return false;
        uint16_t tmp;
        switch (mode) {
        case Mode::X:
            state &= ~(0x1 << 7);
            return true;
        case Mode::S:
        case Mode::IX:
            state--;
            return true;
        case Mode::IS:
            tmp = (state >> 10) & 0x3F;
            tmp--;
            state &= ~(0x3F << 10);
            state |= (tmp << 10);
            return true;
        case Mode::SIX:
            state &= ~(0x1 << 8);
            return true;
        default:
            throw std::runtime_error("LockStateMG::clearInternal(): bad mode.");
        }
    }
};


}} // namespace cybozu::lock
