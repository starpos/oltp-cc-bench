#include "measure_util.hpp"
#include "cybozu/test.hpp"


CYBOZU_TEST_AUTO(histogram1)
{
    Histogram h;

    CYBOZU_TEST_EQUAL(h[0], 0);
    h.add(0);
    CYBOZU_TEST_EQUAL(h[0], 1);

    CYBOZU_TEST_EQUAL(h[1], 0);
    h.add(1);
    CYBOZU_TEST_EQUAL(h[1], 1);

    CYBOZU_TEST_EQUAL(h[2], 0);
    h.add(2);
    CYBOZU_TEST_EQUAL(h[2], 1);
    h.add(3);
    CYBOZU_TEST_EQUAL(h[2], 2);

    CYBOZU_TEST_EQUAL(h[3], 0);
    h.add(4);
    CYBOZU_TEST_EQUAL(h[3], 1);

    CYBOZU_TEST_EQUAL(h[4], 0);
    h.add(8);
    CYBOZU_TEST_EQUAL(h[4], 1);

    CYBOZU_TEST_EQUAL(h[64], 0);
    h.add(size_t(-1));
    CYBOZU_TEST_EQUAL(h[64], 1);

    std::cout << h;
}
