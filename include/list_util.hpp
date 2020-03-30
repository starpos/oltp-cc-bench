#pragma once
#include <cinttypes>
#include <cstddef>
#include <cstdio>
#include <cassert>
#include <functional>
#include "util.hpp"

/**
 * For test.
 */
#include "random.hpp"


/**
 * Insert a node between prev and curr.
 * If prev is null, the node will be head.
 * If curr is null, the node will be tail.
 * Both prev and curr are null, node->next will be null.
 */
template <typename Node>
void insert_node(Node*& head, Node*& tail, Node* prev, Node* curr, Node* node)
{
    assert(node != nullptr);
    if (prev == nullptr) {
        head = node;
    } else {
        prev->next = node;
    }
    node->next = curr; // curr may be null.
    if (curr == nullptr) {
        tail = node;
    }
}


template <typename Node>
class NodeListT;


/**
 * Forward iterator of NodeListT.
 */
template <typename Node>
class NodeListIteratorT
{
private:
    NodeListT<Node>* list_;
    Node* node_;
public:
    using value_type = Node;

    NodeListIteratorT() : list_(nullptr), node_(nullptr) {
    }
    NodeListIteratorT(const NodeListIteratorT&) = default;
    NodeListIteratorT& operator=(const NodeListIteratorT&) = default;

    void set_begin(NodeListT<Node>& list) {
        list_ = &list;
        node_ = list_->front();
    }
    void set_end(NodeListT<Node>& list) {
        list_ = &list;
        node_ = nullptr;
    }

    Node& operator*() { return *node_; }
    Node* operator->() { return node_; }

    bool operator==(const NodeListIteratorT& rhs) const {
        return list_ == rhs.list_ && node_ == rhs.node_;
    }
    bool operator!=(const NodeListIteratorT& rhs) const {
        return !(*this == rhs);
    }

    NodeListIteratorT& operator++() {
        node_ = node_->next;
        return *this;
    }
    NodeListIteratorT operator++(int) {
        NodeListIteratorT ret = *this;
        ++ret;
        return ret;
    }
};


/**
 * Node list.
 * Node instance must have (Node* next) member field.
 *
 * This is not thread-safe code so you need approprietely memory barriers.
 */
template <typename Node>
class NodeListT {
private:
    Node* head_;
    Node* tail_;
    size_t size_;
public:
    NodeListT() : head_(nullptr), tail_(nullptr), size_(0) {
    }

    NodeListT(const NodeListT&) = delete;
    NodeListT& operator=(const NodeListT&) = delete;
    NodeListT(NodeListT&& rhs) : NodeListT() { swap(rhs); }
    NodeListT& operator=(NodeListT&& rhs) noexcept { swap(rhs); return *this; }

    void push_back(Node* node) {
        assert(node != nullptr);
        size_++;
        if (head_ == nullptr) {
            set_first_node(node);
            return;
        }
        tail_->next = node;
        node->next = nullptr;
        tail_ = node;
    }
    void push_front(Node* node) {
        assert(node != nullptr);
        size_++;
        if (head_ == nullptr) {
            assert(tail_ == nullptr);
            set_first_node(node);
            return;
        }
        node->next = head_;
        head_ = node;
    }
    Node* front() const {
        return head_;
    }
    /**
     * debug
     */
    Node* tail() const { return tail_; }
    void pop_front() {
        assert(size_ > 0);
        size_--;
        assert(head_ != nullptr);
        Node* newhead = head_->next;
        head_ = newhead;
        if (head_ == nullptr) tail_ = nullptr;
    }
    void push_back_list(NodeListT&& node_list) {
        if (node_list.empty()) return;
        if (tail_ == nullptr) {
            assert(head_ == nullptr);
            head_ = node_list.head_;
            tail_ = node_list.tail_;
            size_ = node_list.size_;
        } else {
            tail_->next = node_list.head_;
            tail_ = node_list.tail_;
            size_ += node_list.size_;
        }
        node_list.init();
    }
    bool empty() const { return size_ == 0; }
    size_t size() const { return size_; }

    /**
     * From the head to tail, nodes must be connected by next pointer.
     */
    void set(Node* head, Node* tail, size_t size) {
        assert((head == nullptr) == (tail == nullptr));
        assert((head == nullptr) == (size == 0));
        head_ = head;
        tail_ = tail;
        size_ = size;
    }
    void init() { set(nullptr, nullptr, 0); }

    /**
     * debug
     */
    void print() const {
        ::printf("size: %zu\n", size_);
        Node* node = head_;
        for (size_t i = 0; i < size_; i++) {
            ::printf("node %zu %p\n", i, node);
            if (node != nullptr) node = node->next;
        }
        ::printf("tail: %p\n", tail_);
    }
    /**
     * debug
     */
    void put_log() const {
        log("size", size_);
        Node* node = head_;
        for (size_t i = 0; i < size_; i++) {
            log("node", i, node);
            if (node != nullptr) node = node->next;
        }
        log("tail", tail_);
    }
    /**
     * debug
     */
    void verify() const {
#ifndef NDEBUG
        if (head_ == nullptr || tail_ == nullptr || size_ == 0) {
            assert(head_ == nullptr);
            assert(tail_ == nullptr);
            assert(size_ == 0);
        }
        if (head_ != nullptr || tail_ != nullptr || size_ > 0) {
            assert(head_ != nullptr);
            assert(tail_ != nullptr);
            assert(size_ > 0);
        }

        size_t c = 0;
        Node* node = head_;
        while (node != nullptr) {
            c++;
            node = node->next;
        }
        if (c != size_) {
            log("NodeListT::verify failed", c, size_);
            assert(false);
        }
#endif
    }

    using iterator = NodeListIteratorT<Node>;
    iterator begin() { iterator it; it.set_begin(*this); return it; }
    iterator end() { iterator it; it.set_end(*this); return it; }


    template <typename Less>
    void insert_sort(Node* node) {
        assert(node != nullptr);
        Node* head = front();
        if (head == nullptr) {
            push_back(node);
            return;
        }
        Less less;
        Node* prev = nullptr;
        while (head != nullptr && !less(*node, *head)) {  // head <= node
            prev = head;
            head = head->next;
        }
        // insert the node at the previous of head.
        insert_node(head_, tail_, prev, head, node);
        size_++;
    }

    template <typename Less>
    void insert_sort(NodeListT<Node>&& src) {
        NodeListT<Node>& dst = *this;
        if (src.empty()) return;
        if (dst.empty()) {
            dst = std::move(src);
            return;
        }

        Node* src_node = src.front();
        assert(src_node != nullptr);
        Node* src_next = src_node->next;

        Node* dst_prev = nullptr;
        Node* dst_node = dst.front();
        assert(dst_node != nullptr);

        dst.size_ += src.size_;
        Less less;

        while (src_node != nullptr) {
            while (dst_node != nullptr && !less(*src_node, *dst_node)) { // dst_node <= src_node
                dst_prev = dst_node;
                dst_node = dst_node->next;
            }

            insert_node(dst.head_, dst.tail_, dst_prev, dst_node, src_node);
            dst_prev = src_node;

            src_node = src_next;
            if (src_next != nullptr) src_next = src_next->next;
        }
        src.init();
    }

private:
    void swap(NodeListT& rhs) noexcept {
        std::swap(head_, rhs.head_);
        std::swap(tail_, rhs.tail_);
        std::swap(size_, rhs.size_);
    }
    void set_first_node(Node* node) {
        head_ = node;
        tail_ = node;
        node->next = nullptr;
    }

    template <typename... Args>
    void log(Args&&... args) {
        unused(args...);
    }
};


struct TestNode
{
    TestNode* next;
    TestNode() : next(nullptr) {
    }
};


void test_nodelist()
{
    NodeListT<TestNode> nodelist;
    TestNode v[10];

    nodelist.print();
    assert(nodelist.size() == 0);

    nodelist.push_back(&v[0]);
    assert(nodelist.size() == 1);
    assert(nodelist.front() == &v[0]);
    nodelist.print();
    nodelist.push_back(&v[1]);
    nodelist.print();
    nodelist.pop_front();
    nodelist.print();
    nodelist.pop_front();
    assert(nodelist.size() == 0);
    nodelist.print();

    NodeListT<TestNode> nodelist2;
    nodelist.push_back_list(std::move(nodelist2));
    assert(nodelist.size() == 0);

    nodelist.push_back(&v[0]);
    nodelist.push_back(&v[1]);
    nodelist2.push_back(&v[2]);
    nodelist2.push_back(&v[3]);
    nodelist.push_back_list(std::move(nodelist2));
    assert(nodelist.size() == 4);
    assert(nodelist2.size() == 0);

    assert(nodelist.front() == &v[0]);
    nodelist.pop_front();
    assert(nodelist.front() == &v[1]);
    nodelist.pop_front();
    assert(nodelist.front() == &v[2]);
    nodelist.pop_front();
    assert(nodelist.front() == &v[3]);
    nodelist.pop_front();
    assert(nodelist.front() == nullptr);

    nodelist.print();
    nodelist2.print();

    nodelist2.push_back(&v[2]);
    nodelist2.push_back(&v[3]);
    nodelist.push_back_list(std::move(nodelist2));
    assert(nodelist.size() == 2);
    assert(nodelist2.size() == 0);
    assert(nodelist.front() == &v[2]);
    nodelist.pop_front();
    assert(nodelist.front() == &v[3]);
    nodelist.pop_front();
    assert(nodelist.front() == nullptr);


    for (size_t s0 = 0; s0 < 5; s0++) {
        for (size_t s1 = 0; s1 < 5; s1++) {
            NodeListT<TestNode> n0, n1;
            for (size_t i = 0; i < s0; i++) n0.push_back(&v[i]);
            for (size_t i = s0; i < s0 + s1; i++) n1.push_back(&v[i]);
            n0.push_back_list(std::move(n1));
            assert(n0.size() == s0 + s1);
            assert(n1.size() == 0);

            TestNode* node = n0.front();
            for (size_t i = 0; i < n0.size(); i++) {
                assert(node == &v[i]);
                node = node->next;
            }
        }
    }

}


/**
 * Insert sort.
 */
template <typename Node, typename Less = std::less<Node> >
void insert_sort(NodeListT<Node>& list, Node* node)
{
    list.template insert_sort<Less>(node);
}


/**
 * Insert all nodes in src to dst.
 * Assume dst and src are sorted already.
 * The result items in dst are sorted.
 * src will be empty.
 */
template <typename Node, typename Less = std::less<Node> >
void insert_sort(NodeListT<Node>& dst, NodeListT<Node>&& src)
{
    dst.template insert_sort<Less>(std::move(src));
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
    assert(list.size() == nr);
    size_t i = 0;
    Node* node = list.front();
    while (node != nullptr) {
        assert(node == &array[i]);
        i++;
        node = node->next;
    }
    assert(i == nr);
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


void test_insert_sort()
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
        assert(nodelist.size() == 10);
        size_t i = 0;
        for (TestNode2& node : nodelist) {
            unused(node);
            // node.print();
            assert(node.value == slist[i]);
            i++;
        }
    }
}


void test_insert_sort2()
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
