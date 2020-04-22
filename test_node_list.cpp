#include "list_util.hpp"
#include "random.hpp"
#include "cybozu/test.hpp"


struct TestNode
{
    TestNode* next;
    TestNode() : next(nullptr) {
    }
};


CYBOZU_TEST_AUTO(test_nodelist)
{
    NodeListT<TestNode> nodelist;
    TestNode v[10];

    nodelist.print();
    CYBOZU_TEST_ASSERT(nodelist.size_debug() == 0);

    nodelist.push_back(&v[0]);
    CYBOZU_TEST_ASSERT(nodelist.size_debug() == 1);
    CYBOZU_TEST_ASSERT(nodelist.front() == &v[0]);
    nodelist.print();
    nodelist.push_back(&v[1]);
    nodelist.print();
    nodelist.pop_front();
    nodelist.print();
    nodelist.pop_front();
    CYBOZU_TEST_ASSERT(nodelist.size_debug() == 0);
    nodelist.print();

    NodeListT<TestNode> nodelist2;
    nodelist.push_back_list(std::move(nodelist2));
    CYBOZU_TEST_ASSERT(nodelist.size_debug() == 0);

    nodelist.push_back(&v[0]);
    nodelist.push_back(&v[1]);
    nodelist2.push_back(&v[2]);
    nodelist2.push_back(&v[3]);
    nodelist.push_back_list(std::move(nodelist2));
    CYBOZU_TEST_ASSERT(nodelist.size_debug() == 4);
    CYBOZU_TEST_ASSERT(nodelist2.size_debug() == 0);

    CYBOZU_TEST_ASSERT(nodelist.front() == &v[0]);
    nodelist.pop_front();
    CYBOZU_TEST_ASSERT(nodelist.front() == &v[1]);
    nodelist.pop_front();
    CYBOZU_TEST_ASSERT(nodelist.front() == &v[2]);
    nodelist.pop_front();
    CYBOZU_TEST_ASSERT(nodelist.front() == &v[3]);
    nodelist.pop_front();
    CYBOZU_TEST_ASSERT(nodelist.front() == nullptr);

    nodelist.print();
    nodelist2.print();

    nodelist2.push_back(&v[2]);
    nodelist2.push_back(&v[3]);
    nodelist.push_back_list(std::move(nodelist2));
    CYBOZU_TEST_ASSERT(nodelist.size_debug() == 2);
    CYBOZU_TEST_ASSERT(nodelist2.size_debug() == 0);
    CYBOZU_TEST_ASSERT(nodelist.front() == &v[2]);
    nodelist.pop_front();
    CYBOZU_TEST_ASSERT(nodelist.front() == &v[3]);
    nodelist.pop_front();
    CYBOZU_TEST_ASSERT(nodelist.front() == nullptr);


    for (size_t s0 = 0; s0 < 5; s0++) {
        for (size_t s1 = 0; s1 < 5; s1++) {
            NodeListT<TestNode> n0, n1;
            for (size_t i = 0; i < s0; i++) n0.push_back(&v[i]);
            for (size_t i = s0; i < s0 + s1; i++) n1.push_back(&v[i]);
            n0.push_back_list(std::move(n1));
            CYBOZU_TEST_ASSERT(n0.size_debug() == s0 + s1);
            CYBOZU_TEST_ASSERT(n1.size_debug() == 0);

            TestNode* node = n0.front();
            for (size_t i = 0; i < n0.size_debug(); i++) {
                CYBOZU_TEST_ASSERT(node == &v[i]);
                node = node->next;
            }
        }
    }
}


struct TestNode2
{
    TestNode2* next;
    size_t order;
    size_t value;

    TestNode2() : next(nullptr), order(0), value(0) {
    }
    bool operator<(const TestNode2& rhs) const { return order < rhs.order; }

    void print() const {
        ::printf("TestNode2: next %p order %zu value %zu\n", next, order, value);
    }
};


template <typename Node>
void verify_node_list_equality(const NodeListT<Node>& list, Node* array, size_t nr)
{
    unused(array); unused(nr);
    CYBOZU_TEST_ASSERT(list.size_debug() == nr);
    size_t i = 0;
    Node* node = list.front();
    while (node != nullptr) {
        CYBOZU_TEST_ASSERT(node == &array[i]);
        i++;
        node = node->next;
    }
    CYBOZU_TEST_ASSERT(i == nr);
}


void print(const NodeListT<TestNode2>& nodelist)
{
    using Node = TestNode2;
    Node* node = nodelist.front();
    while (node != nullptr) {
        ::printf("%p %zu\n", node, node->order);
        node = node->next;
    }
}


std::vector<size_t> gen_shuffled_list(size_t nr)
{
    std::vector<size_t> v(nr);
    for (size_t i = 0; i < nr; i++) {
        v[i] = i;
    }

    cybozu::util::Random<size_t> rand(0, nr - 1);
    for (size_t i = 0; i < nr; i++) {
        size_t j = rand();
        if (i == j) continue;
        std::swap(v[i], v[j]);
    }

    return v;
}


void print(const std::vector<size_t>& slist)
{
    for (const size_t val : slist) {
        ::printf("%zu ", val);
    }
    ::printf("\n");
}


CYBOZU_TEST_AUTO(test_insert_sort)
{
    TestNode2 v[10];
    for (size_t i = 0; i < 10; i++) v[i].order = i;

    {
        NodeListT<TestNode2> nodelist;
        for (size_t i = 0; i < 10; i++) {
            insert_sort<TestNode2>(nodelist, &v[i]);
        }
        verify_node_list_equality(nodelist, v, 10);
    }
    {
        NodeListT<TestNode2> nodelist;
        for (size_t i = 10; i > 0; i--) {
            insert_sort<TestNode2>(nodelist, &v[i - 1]);
        }
        verify_node_list_equality(nodelist, v, 10);
    }

    for (size_t i = 0; i < 10; i++) {
        NodeListT<TestNode2> nodelist;
        std::vector<size_t> slist = gen_shuffled_list(10);
        print(slist);
        for (size_t i : slist) {
            insert_sort<TestNode2>(nodelist, &v[i]);
#if 0
            print(nodelist);
            ::printf("\n");
#endif
        }
        verify_node_list_equality(nodelist, v, 10);
    }

    // check requests having the same order will keep FIFO.
    {
        TestNode2 v2[10]; unused(v2);
        for (size_t i = 0; i < 10; i++) {
            v[i].order = 0;
            v[i].value = i;
        }
        std::vector<size_t> slist = gen_shuffled_list(10);
        NodeListT<TestNode2> nodelist;
        for (size_t i : slist) {
            insert_sort<TestNode2>(nodelist, &v[i]);
        }
        CYBOZU_TEST_ASSERT(nodelist.size_debug() == 10);
        size_t i = 0;
        for (TestNode2& node : nodelist) {
            unused(node);
            // node.print();
            CYBOZU_TEST_ASSERT(node.value == slist[i]);
            i++;
        }
    }
}


CYBOZU_TEST_AUTO(test_insert_sort2)
{
    TestNode2 v[10];
    for (size_t i = 0; i < 10; i++) v[i].order = i;

    {
        NodeListT<TestNode2> nl1, nl2;
        for (size_t i : {0, 1, 2, 3, 4}) nl1.push_back(&v[i]);
        for (size_t i : {5, 6, 7, 8, 9}) nl2.push_back(&v[i]);
        insert_sort<TestNode2>(nl1, std::move(nl2));
        // print(nl1);
        verify_node_list_equality(nl1, v, 10);
    }

    {
        NodeListT<TestNode2> nl1, nl2;
        for (size_t i : {0, 2, 4, 6, 8}) nl1.push_back(&v[i]);
        for (size_t i : {1, 3, 5, 7, 9}) nl2.push_back(&v[i]);
        insert_sort<TestNode2>(nl1, std::move(nl2));
        // print(nl1);
        verify_node_list_equality(nl1, v, 10);
    }

    {
        NodeListT<TestNode2> nl1, nl2;
        for (size_t i : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) nl1.push_back(&v[i]);
        insert_sort<TestNode2>(nl1, std::move(nl2));
        // print(nl1);
        verify_node_list_equality(nl1, v, 10);
    }

    {
        NodeListT<TestNode2> nl1, nl2;
        for (size_t i : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) nl2.push_back(&v[i]);
        insert_sort<TestNode2>(nl1, std::move(nl2));
        // print(nl1);
        verify_node_list_equality(nl1, v, 10);
    }

    for (size_t i = 0; i < 10; i++) {
        NodeListT<TestNode2> nl1, nl2;
        cybozu::util::Random<size_t> rand(0, 1);
        for (size_t i = 0; i < 10; i++) {
            if (rand() == 0) nl1.push_back(&v[i]);
            else nl2.push_back(&v[i]);
        }
        insert_sort<TestNode2>(nl1, std::move(nl2));
        // print(nl1);
        verify_node_list_equality(nl1, v, 10);
    }
}
