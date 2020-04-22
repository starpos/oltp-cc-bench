#pragma once
#include <cinttypes>
#include <cstddef>
#include <cstdio>
#include <cassert>
#include <functional>
#include "util.hpp"


#if 1
#undef USE_NODE_LIST_SIZE_FIELD
#else
#define USE_NODE_LIST_SIZE_FIELD
#endif


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
#ifdef USE_NODE_LIST_SIZE_FIELD
    size_t size_;
#endif
public:
#ifdef USE_NODE_LIST_SIZE_FIELD
    NodeListT() : head_(nullptr), tail_(nullptr), size_(0) {}
#else
    NodeListT() : head_(nullptr), tail_(nullptr) {}
#endif

    NodeListT(const NodeListT&) = delete;
    NodeListT& operator=(const NodeListT&) = delete;
    NodeListT(NodeListT&& rhs) : NodeListT() { swap(rhs); }
    NodeListT& operator=(NodeListT&& rhs) noexcept { swap(rhs); return *this; }

    void push_back(Node* node) {
        assert(node != nullptr);
#ifdef USE_NODE_LIST_SIZE_FIELD
        size_++;
#endif
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
#ifdef USE_NODE_LIST_SIZE_FIELD
        size_++;
#endif
        if (head_ == nullptr) {
            assert(tail_ == nullptr);
            set_first_node(node);
            return;
        }
        node->next = head_;
        head_ = node;
    }
    Node* front() const { return head_; }
    Node* back() const { return tail_; }
    void pop_front() {
#ifdef USE_NODE_LIST_SIZE_FIELD
        assert(size_ > 0);
        size_--;
#endif
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
#ifdef USE_NODE_LIST_SIZE_FIELD
            size_ = node_list.size_;
#endif
        } else {
            tail_->next = node_list.head_;
            tail_ = node_list.tail_;
#ifdef USE_NODE_LIST_SIZE_FIELD
            size_ += node_list.size_;
#endif
        }
        node_list.init();
    }
    bool empty() const {
#if 0
        return size_ == 0;
#else
        return head_ == nullptr;
#endif
    }
    bool size_is_one() const {
#if 0
        return size_ == 1;
#else
        return !empty() && head_ == tail_;
#endif
    }
#ifdef USE_NODE_LIST_SIZE_FIELD
    size_t size() const { return size_; }
    size_t size_debug() const { return size_; }
#else
    /** This is for debug code only. */
    size_t size_debug() const {
        size_t c = 0;
        Node* node = head_;
        while (node != nullptr) {
            c++;
            node = node->next;
        }
        return c;
    }
#endif

    /**
     * From the head to tail, nodes must be connected by next pointer.
     */
#ifdef USE_NODE_LIST_SIZE_FIELD
    void set(Node* head, Node* tail, size_t size) {
        assert((head == nullptr) == (tail == nullptr));
        assert((head == nullptr) == (size == 0));
        head_ = head;
        tail_ = tail;
        size_ = size;
    }
    void init() { set(nullptr, nullptr, 0); }
#else
    void set(Node* head, Node* tail) {
        assert((head == nullptr) == (tail == nullptr));
        head_ = head;
        tail_ = tail;
    }
    void init() { set(nullptr, nullptr); }
#endif

    /**
     * debug
     */
    void print() const {
#ifdef USE_NODE_LIST_SIZE_FIELD
        ::printf("size: %zu\n", size_);
        Node* node = head_;
        for (size_t i = 0; i < size_; i++) {
            ::printf("node %zu %p\n", i, node);
            if (node != nullptr) node = node->next;
        }
#else
        Node* node = head_;
        while (node != nullptr) {
            ::printf("node %p\n", node);
            node = node->next;
        }
#endif
        ::printf("tail: %p\n", tail_);
    }
    /**
     * debug
     */
    template <typename Ostream>
    void out(Ostream& os) const {
        // TODO
        os << " ";
    }
    /**
     * debug
     */
    template <typename Ostream>
    friend Ostream& operator<<(Ostream& os, const NodeListT& list) {
        list.out(os);
        return os;
    }
    /**
     * debug
     */
    void put_log() const {
#ifdef USE_NODE_LIST_SIZE_FIELD
        log("size", size_);
        Node* node = head_;
        for (size_t i = 0; i < size_; i++) {
            log("node", i, node);
            if (node != nullptr) node = node->next;
        }
#else
        Node* node = head_;
        while (node != nullptr) {
            log("node", node);
            node = node->next;
        }
#endif
        log("tail", tail_);
    }
    /**
     * debug
     */
    void verify() const {
#ifndef NDEBUG
#ifdef USE_NODE_LIST_SIZE_FIELD
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
#else
        if (head_ == nullptr || tail_ == nullptr) {
            assert(head_ == nullptr);
            assert(tail_ == nullptr);
        }
        if (head_ != nullptr || tail_ != nullptr) {
            assert(head_ != nullptr);
            assert(tail_ != nullptr);

            Node* node = head_;
            Node* prev = nullptr;
            while (node != nullptr) {
                prev = node;
                node = node->next;
            }
            assert(prev != nullptr);
            assert(prev == tail_);
        }
#endif
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
#ifdef USE_NODE_LIST_SIZE_FIELD
        size_++;
#endif
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

#ifdef USE_NODE_LIST_SIZE_FIELD
        dst.size_ += src.size_;
#endif
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
#ifdef USE_NODE_LIST_SIZE_FIELD
        std::swap(size_, rhs.size_);
#endif
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
