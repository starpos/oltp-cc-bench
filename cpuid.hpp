#pragma once
/*
 * Data for CPU affinity setting.
 */

#include <vector>
#include <string>
#include <map>
#include <list>
#include "process.hpp"


//Dual Xeon (6c12t x2)
#if 0
const std::vector<size_t> CpuId_({0, 1, 2, 3, 4, 5,
            6, 7, 8, 9, 10, 11,
            12, 13, 14, 15, 16, 17,
            18, 19, 20, 21, 22, 23}); // SOCKET
#elif 0
const std::vector<size_t> CpuId_({0, 2, 4, 6, 8, 10,
            1, 3, 5, 7, 9, 11,
            12, 14, 16, 18, 20, 22,
            13, 15, 17, 19, 21, 23}); // CORE
#elif 0
const std::vector<size_t> CpuId_({0, 12, 2, 14, 4, 16,
            6, 18, 8, 20, 10, 22,
            1, 13, 3, 15, 5, 17,
            7, 19, 9, 21, 11, 23}); // HT
#endif

//Single core-i7 (6c12t  x1)
#if 0
const std::vector<size_t> CpuId_({
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}); // SOCKET

#elif 0
const std::vector<size_t> CpuId_({
        0, 2, 4, 6, 8, 10, 1, 3, 5, 7, 9, 11}); // CORE

#elif 0
const std::vector<size_t> CpuId_({
        0, 6, 1, 7, 2, 8, 3, 9, 4, 10, 5, 11}); // HT
#endif


//Dual Xeon (8c16t x2)
#if 0
const std::vector<size_t> CpuId_({0, 2, 4, 6, 8, 10, 12, 14,
            1, 3, 5, 7, 9, 11, 13, 15,
            16, 18, 20, 22, 24, 26, 28, 30,
            17, 19, 21, 23, 25, 27, 29, 31}); // CORE
#endif


struct CpuTopology
{
    uint id;
    uint core;
    uint socket;
    uint node; // NUMA node.
    uint thread; // thread in core.

    std::string str() const {
        return cybozu::util::formatString("id %u  core %u  socket %u  node %u  thread %u"
             , id, core, socket, node, thread);
    }
};


std::vector<CpuTopology> getCpuTopologies()
{
    std::map<std::tuple<uint, uint, uint>, uint> map;
    using StrVec = std::vector<std::string>;
    const StrVec args = {"/usr/bin/lscpu", "-p"};
    const std::string ret = cybozu::process::call(args);
    const StrVec r = cybozu::util::splitString(ret, "\n");
    std::vector<CpuTopology> topo;
    for (const std::string& s : r) {
        if (s.empty()) continue;
        if (s[0] == '#') continue;
        const StrVec v = cybozu::util::splitString(s, ",");
        if (v.size() < 4) continue;
        const uint id =     uint(::atoi(v[0].c_str()));
        const uint core =   uint(::atoi(v[1].c_str()));
        const uint socket = uint(::atoi(v[2].c_str()));
        const uint node =   uint(::atoi(v[3].c_str()));
        auto key = std::make_tuple(core, socket, node);
        auto it = map.find(key);
        uint thread = 0;
        if (it == map.end()) map.emplace(key, 0);
        else thread = ++(it->second);
        topo.push_back({id, core, socket, node, thread});
    }
    return topo;
}


enum class CpuAffinityMode : uint8_t { NONE, NODE, CORE, THREAD, CUSTOM1, };


const std::pair<CpuAffinityMode, const char*> affinityModeTable_[] =
{
    {CpuAffinityMode::NONE, "NONE"},
    {CpuAffinityMode::NODE, "NODE"},
    {CpuAffinityMode::CORE, "CORE"},
    {CpuAffinityMode::THREAD, "THREAD"},
    {CpuAffinityMode::CUSTOM1, "CUSTOM1"},
};


std::string cpuAffinityModeToStr(CpuAffinityMode amode)
{
    const auto& table = affinityModeTable_;
    const size_t nr = sizeof(table) / sizeof(table[0]);
    for (size_t i = 0; i < nr; i++) {
        if (amode == table[i].first) return table[i].second;
    }
    throw std::runtime_error("cpuAffinityModeToStr: such mode not found");
}


CpuAffinityMode parseCpuAffinityMode(const std::string& amodeStr)
{
    const auto& table = affinityModeTable_;
    const size_t nr = sizeof(table) / sizeof(table[0]);
    for (size_t i = 0; i < nr; i++) {
        if (amodeStr == table[i].second) return table[i].first;
    }
    throw std::runtime_error("parseCpuAffinityMode: mode not found");
}


/**
 * Shuffle input CpuTopologies.
 *
 * Input list: (0, a), (0, b), (0, c), (1, d), (1, f), (1, g)
 * Output list: (0, a), (1, d), (0, b), (1, f), (0, c), (1, g)
 *
 * Usage:
 * (1) push phase
 *     call push() several imes.
 * (2) pop phase
 *     call initPop() once, then,
 *     call pop() while empty() returns false.
 */
template <typename Key>
class Shuffler
{
    using Map = std::map<Key, std::list<CpuTopology> >;
    Map m_;

    typename Map::iterator it_;

public:
    Shuffler() : m_() {
    }
    void push(Key key, const CpuTopology& t) {
        m_[key].push_back(t);
    }

    void initPop() {
        it_ = m_.begin();
        if (it_ == m_.end()) throw std::runtime_error("Shuffler: no data");
    }
    CpuTopology pop() {
        while (it_ != m_.end() && it_->second.empty()) it_ = m_.erase(it_);
        if (it_ == m_.end()) throw std::runtime_error("Shuffler: no data");
        assert(!it_.empty());
        CpuTopology ret = it_->second.front();
        it_->second.pop_front();
        ++it_;
        if (it_ == m_.end()) it_ = m_.begin();
        return ret;
    }
    bool empty() const {
        for (const typename Map::value_type &p : m_) {
            if (!p.second.empty()) return false;
        }
        return true;
    }
};


std::vector<uint> getCpuIdList(CpuAffinityMode amode)
{
    std::vector<CpuTopology> topo = getCpuTopologies();
    std::vector<uint> ret;
    ret.reserve(topo.size());

    if (amode == CpuAffinityMode::CUSTOM1) {
        /*
         * Prefers inter-socket communication.
         */
        Shuffler<uint> s;
        for (const CpuTopology& t : topo) s.push(t.socket, t);
        s.initPop();
        while (!s.empty()) ret.push_back(s.pop().id);
        return ret;
    }

    std::function<bool (const CpuTopology&, const CpuTopology&)> less;
    if (amode == CpuAffinityMode::NODE) {
        less = [](const CpuTopology& a, const CpuTopology& b) {
            return std::make_tuple(a.thread, a.core, a.node, a.socket) < std::make_tuple(b.thread, b.core, b.node, b.socket);
        };
    } else if (amode == CpuAffinityMode::CORE) {
        less = [](const CpuTopology& a, const CpuTopology& b) {
            return std::make_tuple(a.thread, a.node, a.socket, a.core) < std::make_tuple(b.thread, b.node, b.socket, b.core);
        };
    } else if (amode == CpuAffinityMode::THREAD) {
        less = [](const CpuTopology& a, const CpuTopology& b) {
            return std::make_tuple(a.node, a.socket, a.core) < std::make_tuple(b.node, b.socket, b.core);
        };
    } else {
        less = [](const CpuTopology& a, const CpuTopology& b) {
            return a.id < b.id;
        };
    }
    std::sort(topo.begin(), topo.end(), less);
    for (const CpuTopology& t : topo) {
        ret.push_back(t.id);
    }
    return ret;
}


void setCpuAffinityModeVec(const std::string& amodeStr, std::vector<uint>& cpuId)
{
    const CpuAffinityMode amode = parseCpuAffinityMode(amodeStr);
    cpuId = getCpuIdList(amode);
}
