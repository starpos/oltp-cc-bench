#pragma once

#include <cinttypes>
#include "cache_line_size.hpp"

namespace cybozu {
namespace util {


class Balancer2x2
{
    alignas(CACHE_LINE_SIZE)
    uint8_t value;
public:
    Balancer2x2() : value(0) {}
    /**
     * returns: 0 or 1.
     */
    uint8_t get() {
        return __atomic_fetch_add(&value, 1, __ATOMIC_RELAXED) % 2;
    }
};


class CountingNetwork4
{
    struct Node {
        uint8_t balancerIdx;
        uint8_t nextIdx[2];
    };
    struct Output {
        alignas(CACHE_LINE_SIZE)
        uint64_t value;
    };
    Output output_[4];
    Balancer2x2 balancers_[6];
    Node network_[3][4];
public:
    CountingNetwork4() : output_(), balancers_(), network_() {
        for (size_t i = 0; i < 4; i++) {
            output_[i].value = i;
        }
        network_[0][0] = {0, {0, 1}};
        network_[0][1] = {0, {0, 1}};
        network_[0][2] = {1, {2, 3}};
        network_[0][3] = {1, {2, 3}};
        network_[1][0] = {2, {0, 3}};
        network_[1][1] = {3, {1, 2}};
        network_[1][2] = {3, {1, 2}};
        network_[1][3] = {2, {0, 3}};
        network_[2][0] = {4, {0, 1}};
        network_[2][1] = {4, {0, 1}};
        network_[2][2] = {5, {2, 3}};
        network_[2][3] = {5, {2, 3}};
    }
    uint64_t get(uint64_t threadId) {
        uint8_t idx = threadId % 4;
        for (size_t i = 0; i < 3; i++) {
            Node& node = network_[i][idx];
            idx = node.nextIdx[balancers_[node.balancerIdx].get()];
        }
        return __atomic_fetch_add(&output_[idx].value, 4, __ATOMIC_RELAXED);
    }
};


class CountingNetwork8
{
    struct Node {
        uint8_t balancerIdx;
        uint8_t nextIdx[2];
    };
    struct Output {
        alignas(CACHE_LINE_SIZE)
        uint64_t value;
    };
    Output output_[8];
    Balancer2x2 balancers_[24];
    Node network_[6][8];
public:
    CountingNetwork8() : output_(), balancers_(), network_() {
        for (size_t i = 0; i < 8; i++) {
            output_[i].value = i;
        }
        network_[0][0] = {0, {0, 2}};
        network_[0][1] = {1, {1, 3}};
        network_[0][2] = {0, {0, 2}};
        network_[0][3] = {1, {1, 3}};
        network_[0][4] = {2, {4, 6}};
        network_[0][5] = {3, {5, 7}};
        network_[0][6] = {2, {4, 6}};
        network_[0][7] = {3, {5, 7}};

        network_[1][0] = {4, {0, 1}};
        network_[1][1] = {4, {0, 1}};
        network_[1][2] = {5, {2, 3}};
        network_[1][3] = {5, {2, 3}};
        network_[1][4] = {6, {4, 5}};
        network_[1][5] = {6, {4, 5}};
        network_[1][6] = {7, {6, 7}};
        network_[1][7] = {7, {6, 7}};

        network_[2][0] = {8,  {0, 4}};
        network_[2][1] = {9,  {1, 5}};
        network_[2][2] = {10, {2, 6}};
        network_[2][3] = {11, {3, 7}};
        network_[2][4] = {8,  {0, 4}};
        network_[2][5] = {9,  {1, 5}};
        network_[2][6] = {10, {2, 6}};
        network_[2][7] = {11, {3, 7}};

        network_[3][0] = {12, {0, 5}};
        network_[3][1] = {13, {1, 4}};
        network_[3][2] = {14, {2, 7}};
        network_[3][3] = {15, {3, 6}};
        network_[3][4] = {13, {1, 4}};
        network_[3][5] = {12, {0, 5}};
        network_[3][6] = {15, {3, 6}};
        network_[3][7] = {14, {2, 7}};

        network_[4][0] = {16, {0, 6}};
        network_[4][1] = {17, {1, 7}};
        network_[4][2] = {18, {2, 4}};
        network_[4][3] = {19, {3, 5}};
        network_[4][4] = {18, {2, 4}};
        network_[4][5] = {19, {3, 5}};
        network_[4][6] = {16, {0, 6}};
        network_[4][7] = {17, {1, 7}};

        network_[5][0] = {20, {0, 7}};
        network_[5][1] = {21, {1, 2}};
        network_[5][2] = {21, {1, 2}};
        network_[5][3] = {22, {3, 4}};
        network_[5][4] = {22, {3, 4}};
        network_[5][5] = {23, {5, 6}};
        network_[5][6] = {23, {5, 6}};
        network_[5][7] = {20, {0, 7}};
    }
    uint64_t get(uint64_t threadId) {
        uint8_t idx = threadId % 8;
        for (size_t i = 0; i < 6; i++) {
            Node& node = network_[i][idx];
            idx = node.nextIdx[balancers_[node.balancerIdx].get()];
        }
        return __atomic_fetch_add(&output_[idx].value, 8, __ATOMIC_RELAXED);
    }
};


}} // namespace cybozu::util
