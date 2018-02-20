#pragma once
/**
 * Priority-queuing locks.
 */
#include "lock.hpp"
#include "atomic_wrapper.hpp"


namespace cybozu {
namespace lock {


class PQNoneLock
{
public:
    struct Mutex {};
    PQNoneLock() {}
    PQNoneLock(Mutex *, uint32_t) {}
    ~PQNoneLock() noexcept {}
    uint32_t getTopPriorityInWaitQueue() const { return UINT32_MAX; }
};


/**
 * This is priority-queuing lock as an extended MCS lock.
 * This is a naive implementation.
 */
class PQMcsLock
{
public:
    struct Node {
        Node *next;
        uint32_t pri; // smaller is prior.
        bool wait;
        Node()
            : next(nullptr)
            , pri(UINT32_MAX)
            , wait(false) {}
        void init() {
            next = nullptr;
            pri = UINT32_MAX;
            wait = false;
        }
    };
    struct Mutex {
        Node *tail;
        std::vector<Node*> buf; // sort buffer;
        Mutex() : tail(nullptr), buf() {}
    };
private:
    Mutex *mutex_; /* shared pointer to the tail of list. */
    Node node_; /* list node. */

    Node *readd(Node *node) {
        // node must not be the tail item.
        Node *next = load(node->next);
        store(node->next, nullptr);
        Node *prev = exchange(mutex_->tail, node, __ATOMIC_RELAXED);
        assert(prev != nullptr);
        store(prev->next, node);
        return next;
    }
    Node *remove(Node *node) {
        Node *next = load(node->next);
        store(node->next, (Node *)nullptr);
        return next;
    }
    void readd2(Node *first, Node *last) {
        Node *prev = exchange(mutex_->tail, last, __ATOMIC_ACQ_REL);
        assert(prev != nullptr);
        store(prev->next, first);
    }
    void reorder() {
        assert(load(node_.next) != nullptr);
        Node *minP, *p;
        // search item with minimum priority.
        Node *tail = load(mutex_->tail);
        p = load(node_.next);
        minP = p;
        size_t c1 = 0;
        size_t c2 = 0;
        while (p != tail) {
            // We must avoid ABA problem:
            // If the pointer is not installed, we must wait for the operation.
            while (load(p->next) == nullptr) _mm_pause();
            p = load(p->next);
            c2++;
            if (p->pri < minP->pri) {
                minP = p;
                c1 = c2;
            }
        }
        // move privious items to the tail of the list.
        // Example:
        //                                                tail
        // (locked)->(pri=5)->(pri=3)->(pri=1)->(pri=6)->(pri=4)-->null
        //              0        1        2       3        4
        //                               minP
        // Here c1 = 2, c2 = 4. Two items before minP will be moved to the tail.
        // After the operation, the list will be the following:
        // (locked)->(pri=1)->(pri=6)->(pri=4)->...->(pri=5)->(pri=3)-->...
        p = load(node_.next);
#if 0
        while (p != minP) {
            p = readd(p);
        }
#else
        if (c1 > 0) {
            std::vector<Node*> &v = mutex_->buf;
            v.clear();
            v.reserve(c1);
            while (p != minP) {
                v.push_back(p);
                p = remove(p);
            }
            //if (c1 != q.size()) { ::printf("c1 %zu q.size %zu\n", c1, q.size()); } // QQQ
#if 1
            std::sort(v.begin(), v.end(), [](const Node* a, const Node* b) { return a->pri < b->pri; } );
#endif
            if (!v.empty()) {
                std::vector<Node*>::iterator i, j;
                i = v.begin();
                j = i; ++j;
                while (j != v.end()) {
                    store((*i)->next, *j);
                    ++i; ++j;
                }
                readd2(v.front(), v.back());
            }
        }
#endif
        store(node_.next, minP);
    }
public:
    PQMcsLock() : mutex_(nullptr), node_() {
    }
    PQMcsLock(Mutex *mutex, uint32_t pri) : PQMcsLock() {
        lock(mutex, pri);
    }
    ~PQMcsLock() noexcept {
        unlock();
    }
    void lock(Mutex *mutex, uint32_t pri) {
        assert(!mutex_);
        node_.init();
        mutex_ = mutex;
        node_.pri = pri;

        Node *prev = exchange(mutex_->tail, &node_, __ATOMIC_ACQ_REL);
        if (prev) {
            store(node_.wait, true);
            storeRelease(prev->next, &node_);
            while (loadAcquire(node_.wait)) _mm_pause();
        }
    }
    void unlock() noexcept {
        if (!mutex_) return;

        if (!load(node_.next)) {
            Node *node = &node_;
            if (compareExchange(mutex_->tail, node, nullptr, __ATOMIC_RELEASE)) {
                mutex_ = nullptr;
                return;
            }
            while (!loadAcquire(node_.next)) _mm_pause();
        }
        //mutex_.next is not null.
        reorder();
        storeRelease(node_.next->wait, false);

        mutex_ = nullptr;
    }
    PQMcsLock(const PQMcsLock&) = delete;
    PQMcsLock(PQMcsLock&& rhs) = delete;
    PQMcsLock& operator=(const PQMcsLock&) = delete;
    PQMcsLock& operator=(PQMcsLock&& rhs) = delete;

    uint32_t getTopPriorityInWaitQueue() const {
        assert(!node_.wait); // you must hold the lock.
        uint32_t pri = UINT32_MAX;
        Node *p = node_.next;
        while (p) {
            if (p->pri < pri) pri = p->pri;
            p = p->next;
        }
        return pri;
    }
};

/**
 * Simple priority-queuing lock.
 * This uses a TTAS spinlock for critical sections.
 */
class PQSpinLock
{
private:
    struct Node {
        uint32_t pri;
        bool wait;
        Node() : pri(UINT32_MAX), wait(false) {}
    };
    struct Compare {
        bool operator()(const Node *a, const Node *b) const {
            return a->pri > b->pri;
        }
    };
    using PriQueue = std::priority_queue<Node*, std::vector<Node*>, Compare>;
public:
    struct Mutex {
        TtasSpinlockT<0>::Mutex ttasMutex;
        PriQueue priQ;
        bool locked;
        Mutex() : ttasMutex(), priQ(), locked(false) {}
    };
private:
    Mutex *mutex_;
    Node node_;
public:
    PQSpinLock() : mutex_(), node_() {}
    PQSpinLock(Mutex *mutex, uint32_t pri) : PQSpinLock() {
        lock(mutex, pri);
    }
    ~PQSpinLock() noexcept {
        unlock();
    }
    PQSpinLock(const PQSpinLock&) = delete;
    PQSpinLock(PQSpinLock&& rhs) : PQSpinLock() { swap(rhs); }
    PQSpinLock& operator=(const PQSpinLock&) = delete;
    PQSpinLock& operator=(PQSpinLock&& rhs) { swap(rhs); return *this; }
    void lock(Mutex *mutex, uint32_t pri) {
        if (mutex_) throw std::runtime_error("PQSpinLock: already locked");
        mutex_ = mutex;
        node_.pri = pri;
        {
            TtasSpinlockT<0> lk(&mutex_->ttasMutex);
            if (mutex_->locked) {
                node_.wait = true;
                mutex_->priQ.push(&node_);
            } else {
                mutex_->locked = true;
            }
        }
        while (node_.wait) _mm_pause();
    }
    void unlock() {
        if (!mutex_) return;

        TtasSpinlockT<0> lk(&mutex_->ttasMutex);
        assert(mutex_->locked);
        if (mutex_->priQ.empty()) {
            mutex_->locked = false;
        } else {
            Node *p = mutex_->priQ.top();
            mutex_->priQ.pop();
            p->wait = false;
        }
        mutex_ = nullptr;
    }
    uint32_t getTopPriorityInWaitQueue() const {
        assert(mutex_);
        TtasSpinlockT<0> lk(&mutex_->ttasMutex);
        assert(mutex_->locked);
        assert(!node_.wait);
        if (mutex_->priQ.empty()) return UINT32_MAX;
        return mutex_->priQ.top()->pri;
    }
private:
    void swap(PQSpinLock& rhs) {
        std::swap(mutex_, rhs.mutex_);
        std::swap(node_, rhs.node_);
    }
};


/**
 * This is a priority-queuing spinlock as an extended MCS lock.
 * This is more efficient than PQMcsLock.
 */
class PQMcsLock2
{
private:
    struct Node {
        Node *next;
        uint32_t pri; // smaller is prior.
        bool wait;
        Node() : next(nullptr), pri(UINT32_MAX), wait(true) {}
        void init() {
            next = nullptr;
            pri = UINT32_MAX;
            wait = true;
        }
    };
    struct Compare {
        bool operator()(const Node *a, const Node *b) const {
            return a->pri > b->pri;
        }
    };
    using PriQueue = std::priority_queue<Node*, std::vector<Node*>, Compare>;

    //static constexpr Node *HAZARD_PTR = reinterpret_cast<Node *>(-1);

public:
    struct Mutex {
        Node *tail; // This must be changed by CAS.
        size_t nr; // Number of nodes. atomic access is required.

        // Only the thread holding lock can change this.
        //Node dummy[2];
        //uint8_t dummyId;
        std::deque<std::unique_ptr<Node> > dummy;
        PriQueue priQ;
#ifndef NDEBUG
        size_t dummyAlloc; // debug
        size_t dummyFree; // debug
#endif

        Mutex() : tail(nullptr), nr(0), dummy(), priQ()
#ifndef NDEBUG
                , dummyAlloc(0), dummyFree(0)
#endif
        {
        }
    };
private:
    Mutex *mutex_; /* shared by all threads. */
    Node node_; /* list node. */

public:
    PQMcsLock2() : mutex_(nullptr), node_() {
    }
    PQMcsLock2(Mutex *mutex, uint32_t pri) : mutex_(), node_() {
        lock(mutex, pri);
    }
    ~PQMcsLock2() noexcept {
        unlock();
    }
    PQMcsLock2(const PQMcsLock2& rhs) = delete;
    PQMcsLock2& operator=(const PQMcsLock2& rhs) = delete;
    /**
     * Move can be supported because locked object's node_ is
     * not included the shared structure.
     */
    PQMcsLock2(PQMcsLock2&& rhs) : PQMcsLock2() {
        swap(rhs);
    }
    PQMcsLock2& operator=(PQMcsLock2&& rhs) {
        swap(rhs);
        return *this;
    }
    void lock(Mutex *mutex, uint32_t pri) {
        if (mutex_) throw std::runtime_error("PQMcsLock2: already locked.");

        mutex_ = mutex;
        node_.pri = pri;
        fetchAdd(mutex_->nr, 1);
        Node *tail = addToTail(&node_, __ATOMIC_RELEASE);
        if (!tail) {
            // Now lock is held.
            store(node_.wait, false);
            Node *dummy = getNewDummy();
            addToTail(dummy, __ATOMIC_RELEASE);
            while (!loadAcquire(node_.next)) _mm_pause();
            assert(mutex_->priQ.empty());
            moveListToPriQ(node_.next);
        } else {
            while (loadAcquire(node_.wait)) _mm_pause();
            // Now lock is held.
        }
    }
    void unlock() {
        if (!mutex_) return;
        assert(!node_.wait);

        Node *dummy = getDummy();
        if (mutex_->priQ.empty() && load(mutex_->tail) == dummy) {
            if (compareExchange(mutex_->tail, dummy, nullptr, __ATOMIC_ACQ_REL)) {
                /*
                 * There is no thread having lock now.
                 *
                 * Do not check assert(mutex_->dummy.next == nullptr);
                 * Because the dummy node may be used by another thread that have new lock.
                 */
                fetchSub(mutex_->nr, 1);
                mutex_ = nullptr;
                node_.init();
                return;
            }
        }

        Node *p = moveDummyToTail();
        if (p) moveListToPriQ(p);

        assert(!mutex_->priQ.empty());
        p = mutex_->priQ.top();
        mutex_->priQ.pop();
        //::printf("givelock\n"); // QQQ
        gcDummy();

        // Release lock and the thread on p will hold lock.
        storeRelease(p->wait, false);

        fetchSub(mutex_->nr, 1);
        mutex_ = nullptr;
        node_.init();
    }
    uint32_t getSelfPriority() const { return node_.pri; }
    /**
     * You must call this thread with lock held.
     */
    uint32_t getTopPriorityInWaitQueue() {
        assert(!load(node_.wait));
        if (load(getDummy()->next)) {
            Node *p = moveDummyToTail();
            if (p) moveListToPriQ(p);
        }
        if (mutex_->priQ.empty()) {
            return UINT32_MAX;
        } else {
            return mutex_->priQ.top()->pri;
        }
    }

private:
    void swap(PQMcsLock2& rhs) {
        std::swap(mutex_, rhs.mutex_);
        std::swap(node_, rhs.node_);
    }
    Node *moveDummyToTail() {
        /*
         * before [dummy, head, ..., tail] (mutex_->tail points tail)
         * after [head, ..., tail, dummy]  (mutex_->tail points dummy)
         *
         * [head, ..., tail] will be moved to the priority queue
         * by calling moveListToPriQ() after this.
         */
        Node *oldDummy = getDummy();
        Node *newDummy = getNewDummy();
        addToTail(newDummy, __ATOMIC_RELEASE);
#if 1
        while (!load(oldDummy->next)) _mm_pause();
#else
        waitForReachable(oldDummy, newDummy);
#endif
        Node *head = load(oldDummy->next);
        storeRelease(oldDummy->next, nullptr);
        return head;
    }
    Node *addToTail(Node *node, int mode) {
        Node *prev = exchange(mutex_->tail, node, mode);
        if (prev) {
            store(prev->next, node);
        }
        return prev;
    }
    bool isDummy(const Node *node) const {
        return node == getDummy();
    }
    Node *getDummy() {
        assert(!mutex_->dummy.empty());
        return mutex_->dummy.back().get();
    }
    const Node *getDummy() const {
        assert(!mutex_->dummy.empty());
        return mutex_->dummy.back().get();
    }
    static constexpr const size_t MAX_NODE_POOL_SIZE = 128;
    static std::deque<std::unique_ptr<Node> >& getNodePool() {
        static thread_local std::deque<std::unique_ptr<Node> > pool;
        return pool;
    }
    Node *getNewDummy() {
        std::deque<std::unique_ptr<Node> >& pool = getNodePool();
        size_t nr = load(mutex_->nr);
        std::deque<std::unique_ptr<Node> >& d = mutex_->dummy;
        if (d.size() < nr) {
            if (pool.empty()) {
                d.push_back(std::make_unique<Node>());
#ifndef NDEBUG
                fetchAdd(mutex_->dummyAlloc, 1);
#endif
            } else {
                d.push_back(std::move(pool.front()));
                pool.pop_front();
            }
        } else {
            std::unique_ptr<Node> node = std::move(d.front());
            d.pop_front();
            node->init();
            d.push_back(std::move(node));
        }
        return d.back().get();
    }
    void gcDummy() {
        std::deque<std::unique_ptr<Node> >& pool = getNodePool();
        size_t nr = load(mutex_->nr);
        std::deque<std::unique_ptr<Node> >& d = mutex_->dummy;
        if (d.size() > nr) {
            pool.push_front(std::move(d.front()));
            d.pop_front();
        }
        if (pool.size() > MAX_NODE_POOL_SIZE) {
#ifndef NDEBUG
            fetchAdd(mutex_->dummyFree, pool.size() - MAX_NODE_POOL_SIZE);
#endif
            pool.resize(MAX_NODE_POOL_SIZE);
        }
    }
    size_t moveListToPriQ(Node *p) {
        size_t c = 0;
        while (!isDummy(p)) {
            mutex_->priQ.push(p);
            c++;
            while (!load(p->next)) _mm_pause();
            p = load(p->next);
        }
        return c;
    }
    size_t waitForReachable(const Node *first, const Node *last) const {
        const Node *p = first;
        size_t c = 0;
        while (p != last) {
            while (!loadAcquire(p->next)) _mm_pause();
            p = load(p->next);
            c++;
        }
        return c;
    }
};


/**
 * An improvement version of PQMcsLock2.
 * Dummy nodes are not required anymore.
 */
class PQMcsLock3
{
private:
    struct Node {
        alignas(8)
        Node *next;
        uint32_t order; // smaller is prior.
        bool wait;
        Node() : next(nullptr), order(UINT32_MAX), wait(false) {
        }
        void init() {
            next = nullptr;
            order = UINT32_MAX;
            wait = false;
        }
    };
    struct Compare {
        bool operator()(const Node *a, const Node *b) const {
            return a->order > b->order;
        }
    };
    using PriQueue = std::priority_queue<Node*, std::vector<Node*>, Compare>;

public:
    struct Mutex {
        // This must be changed by CAS or XCHG.
        // LSB is used to the manager existance.
        // The manager is almost the lock holder except for the initial procedure.
        // Initial state: (null, 1).
        // Lock requester node0 exchanges tailWithBit with (node0, 0).
        // Lock requester got (null, 1). LSB is 1 so it will be the manager.
        // The manager has responsibility to maintain priQ and notify the requester with top priority.
        // Notified node will hold the lock and become the next manager.
        // The manager get the queue by exchanging tailWithBit with (null, 0) after getting head and making it null.
        // If a node who got (null, 0), there is another manager but it must install head for the manager.
        // If there is no requester, tailWithBit must be set from (null, 0) to (null, 1) with CAS.
        uintptr_t tailWithBit;

        // Only the manager can access the head.
        // If this is not null, tail must not be null.
        // Just three (head, tail) patterns are allowed: (null, null), (null, not null), (not null, not null).
        Node *head;

        // Only the manager can access this priority queue.
        PriQueue priQ;

        Mutex() : tailWithBit(1), head(nullptr), priQ() {
        }
    };
private:
    Mutex *mutex_; /* shared by all threads. */
    Node node_; /* list node. */

public:
    PQMcsLock3() : mutex_(nullptr), node_() {
    }
    PQMcsLock3(Mutex *mutex, uint32_t order) : PQMcsLock3() {
        lock(mutex, order);
    }
    ~PQMcsLock3() noexcept {
        unlock();
    }
    PQMcsLock3(const PQMcsLock3& rhs) = delete;
    PQMcsLock3& operator=(const PQMcsLock3& rhs) = delete;
    /**
     * Move can be supported because locked object's node_ is
     * not included the shared structure.
     */
    PQMcsLock3(PQMcsLock3&& rhs) : PQMcsLock3() { swap(rhs); }
    PQMcsLock3& operator=(PQMcsLock3&& rhs) { swap(rhs); return *this; }

    void lock(Mutex *mutex, uint32_t order) {
        assert(mutex);
        mutex_ = mutex;
        store(node_.order, order);

        uintptr_t prevWithBit = exchange(mutex_->tailWithBit, uintptr_t(&node_), __ATOMIC_ACQ_REL);
        const bool isManager = prevWithBit == 1; // prevWithBit & 0x1
        Node *prev = (Node *)(prevWithBit & ~0x1);

        if (prev != nullptr) {
            store(node_.wait, true);
            storeRelease(prev->next, &node_);
            while (loadAcquire(node_.wait)) _mm_pause(); // local spin wait.
            // Now I hold the lock.
            return;
        }
        if (!isManager) {
            store(node_.wait, true);
            assert(load(mutex_->head) == nullptr);
            storeRelease(mutex_->head, &node_);
            while (loadAcquire(node_.wait)) _mm_pause(); // local spin wait.
            // Now I hold the lock.
            return;
        }
        // I'm manager in the initial procedure.
        assert(mutex_->head == nullptr);
        assert(mutex_->priQ.empty());
        uintptr_t tailWithBit = exchange(mutex_->tailWithBit, 0, __ATOMIC_ACQ_REL);
        assert((tailWithBit & ~0x1) != 0);
        Node *tail = (Node *)tailWithBit;
        Node *node = &node_; // head.
        moveQtoQ(node, tail);
        node = mutex_->priQ.top();
        mutex_->priQ.pop();
        if (node == &node_) {
            // Now I hold the lock.
            return;
        }
        store(node_.wait, true);
        storeRelease(node->wait, false); // notify
        while (loadAcquire(node_.wait)) _mm_pause(); // local spin wait.
        // Now I hold the lock.
    }

    void unlock() {
        assert(mutex_);
        uintptr_t tailWithBit = load(mutex_->tailWithBit);
        while (tailWithBit == 0 && mutex_->priQ.empty()) {
            // There is no requester.
            if (compareExchange(mutex_->tailWithBit, tailWithBit, 1, __ATOMIC_ACQ_REL)) {
                // The manager can leave.
                mutex_ = nullptr;
                node_.init();
                return;
            }
            _mm_pause();
        }
        if (tailWithBit != 0) {
            Node *node, *tail;
            extractFromQ(&node, &tail);
            moveQtoQ(node, tail);
        }
        assert(!mutex_->priQ.empty());
        Node *node = mutex_->priQ.top();
        mutex_->priQ.pop();
        storeRelease(node->wait, false); // notify.
        mutex_ = nullptr;
        node_.init();
    }

    void extractFromQ(Node **nodeP, Node **tailP) {
        while (load(mutex_->head) == nullptr) _mm_pause();
        *nodeP = load(mutex_->head);
        store(mutex_->head, nullptr);
        uintptr_t tailWithBit = exchange(mutex_->tailWithBit, 0, __ATOMIC_ACQ_REL);
        assert(tailWithBit > 1);
        *tailP = (Node *)tailWithBit;
    }

    /**
     * Only the thread that hold the lock can call this method.
     */
    uint32_t getTopPriorityInWaitQueue() {
        assert(mutex_);
        uintptr_t tailWithBit = load(mutex_->tailWithBit);
        assert(tailWithBit != 1);
        if (tailWithBit != 0) {
            Node *node, *tail;
            extractFromQ(&node, &tail);
            moveQtoQ(node, tail);
        }
        if (mutex_->priQ.empty()) return UINT32_MAX;
        return load(mutex_->priQ.top()->order);
    }

private:
    /**
     * Do not call this in lock() function.
     * Executing lock() function, node_ will be shared by multiple threads.
     */
    void swap(PQMcsLock3& rhs) {
        std::swap(mutex_, rhs.mutex_);
        std::swap(node_, rhs.node_);
    }
    /**
     * Only the manager can call this.
     */
    void moveQtoQ(Node *node, Node *tail) {
        assert(node);
        assert(tail);

        for (;;) {
            mutex_->priQ.push(node);
            if (node == tail) {
                assert(load(tail->next) == nullptr);
                return;
            }
            while (load(node->next) == nullptr) _mm_pause();
            node = load(node->next);
        }
    }
};



/**
 * Priority-queuing lock using posix mutex lock.
 * Waiting threads will sleep while ones with a spinlock will execute busy loop.
 */
class PQPosixLock
{
private:
    struct Node {
        std::unique_ptr<std::condition_variable> cvp;
        uint32_t pri;
        Node() : cvp(std::make_unique<std::condition_variable>()), pri(UINT32_MAX) {}
    };
    struct Compare {
        bool operator()(const Node *a, const Node *b) const {
            return a->pri > b->pri;
        }
    };
    using Lock = std::unique_lock<std::mutex>;
    using PriQueue = std::priority_queue<Node*, std::vector<Node*>, Compare>;
public:
    struct Mutex {
        std::mutex posixMutex;
        PriQueue priQ;
        bool locked;
        Mutex() : posixMutex(), priQ(), locked(false) {}
    };
private:
    Mutex *mutex_;
    Node node_;
public:
    PQPosixLock() : mutex_(), node_() {}
    PQPosixLock(Mutex *mutex, uint32_t pri) : PQPosixLock() {
        lock(mutex, pri);
    }
    ~PQPosixLock() noexcept {
        unlock();
    }
    PQPosixLock(const PQPosixLock&) = delete;
    PQPosixLock(PQPosixLock&& rhs) : PQPosixLock() { swap(rhs); }
    PQPosixLock& operator=(const PQPosixLock&) = delete;
    PQPosixLock& operator=(PQPosixLock&& rhs) { swap(rhs); return *this; }
    void lock(Mutex *mutex, uint32_t pri) {
        if (mutex_) throw std::runtime_error("PQPosixLock: already locked.");
        mutex_ = mutex;
        node_.pri = pri;

        Lock lk(mutex_->posixMutex);
        if (mutex_->locked) {
            mutex_->priQ.push(&node_);
            node_.cvp->wait(lk);
        } else {
            mutex_->locked = true;
        }
    }
    void unlock() {
        if (!mutex_) return;

        Lock lk(mutex_->posixMutex);
        if (mutex_->priQ.empty()) {
            mutex_->locked = false;
        } else {
            Node *node = mutex_->priQ.top();
            mutex_->priQ.pop();
            node->cvp->notify_one();
        }
        mutex_ = nullptr;
    }
    uint32_t getTopPriorityInWaitQueue() const {

        Lock lk(mutex_->posixMutex);
        if (mutex_->priQ.empty()) {
            return UINT32_MAX;
        } else {
            return mutex_->priQ.top()->pri;
        }
    }
private:
    void swap(PQPosixLock& rhs) {
        std::swap(mutex_, rhs.mutex_);
        std::swap(node_, rhs.node_);
    }
};


namespace lock1993 {

struct Proc;
struct Req;


struct Proc
{
    Req *watch;
    Req *myreq;
    uint32_t pri;
    Proc() : watch(), myreq(), pri(UINT32_MAX) {}
};


struct Req
{
    Proc *watcher;
    Proc *myproc;
    bool isGranted; // false: pending, true: granted.
    Req() : watcher(), myproc(), isGranted(false) {}
    void init() {
        watcher = nullptr;
        myproc = nullptr;
        isGranted = false;
    }
};


std::deque<std::unique_ptr<Req> >& getPool()
{
    static thread_local std::deque<std::unique_ptr<Req> > q;
    return q;
}


std::unique_ptr<Req> allocReq()
{
#if 1
    std::deque<std::unique_ptr<Req> >& pool = getPool();
    //::printf("pool size %zu\n", pool.size()); // QQQ
    if (pool.empty()) {
        auto p = std::make_unique<Req>();
        //::printf("alloc %p\n", p.get()); // QQQ
        return p;
    }
    std::unique_ptr<Req> ret = std::move(pool.front());
    pool.pop_front();
    ret->init();
    return ret;
#else
    return std::make_unique<Req>();
#endif
}


void freeReq(std::unique_ptr<Req>&& req)
{
    assert(req);
    constexpr size_t MAX_NR = 128;

#if 1
    //::printf("free  %p\n", req.get()); // QQQ
    std::deque<std::unique_ptr<Req> >& pool = getPool();
    pool.push_front(std::move(req));
    if (pool.size() > MAX_NR) pool.resize(MAX_NR);
#else
    req.reset();
#endif
}


struct Lock
{
    Req *head;
    Req *tail;
#if 0 // debug
    size_t counter;
#endif
    Lock() : head(), tail()
#if 0
           , counter(0)
#else
    {
#endif
        // The memory for Req will be freed
        // by another thread at being unlocked.
        Req *p = allocReq().release();

        p->myproc = nullptr;
        p->watcher = nullptr;
        p->isGranted = true;
        tail = p;
        head = p;
    }
    ~Lock() noexcept {
        // There must exist neither lock requestors nor holders.
        assert(head == tail);
        assert(head != nullptr);
        freeReq(std::unique_ptr<Req>(head));
    }
};


void lock1993(Lock& lk, Proc& proc)
{
    // This allocated memory will be freeed by another thread.
    store(proc.myreq, allocReq().release());
    store(proc.myreq->myproc, &proc);
#if 0
    ::printf("%5u  req %p\n", proc.pri, proc.myreq);
#endif
    Req *tail = exchange(lk.tail, proc.myreq, __ATOMIC_ACQ_REL);
    store(proc.watch, tail);
    assert(proc.watch);
    assert(!proc.watch->watcher);
    storeRelease(proc.watch->watcher, &proc);
#if 0
    ::printf("pri %u watch %p\n", proc.pri, proc.watch);
#endif
    while (!loadAcquire(proc.watch->isGranted)) _mm_pause();
    // locked.

#if 0
    __attribute__((unused)) size_t c
        = __atomic_fetch_add(&lk.counter, 1, __ATOMIC_RELAXED);
#if 0
    ::printf("%5u  locked %zu\n", proc.pri, c);
    ::fflush(::stdout);
#endif
    if (c != 0) {
        assert(false);
    };
#endif
}


void unlock1993(Lock& lk, Proc& proc)
{
    // Remove my Process and the Request I watched from the list.
    //while (proc.myreq->myproc != &proc) _mm_pause();
    assert(proc.myreq->myproc == &proc);
    assert(proc.watch->watcher == &proc);

    store(proc.myreq->myproc, proc.watch->myproc);
    if (proc.myreq->myproc != nullptr) {
        storeRelease(proc.myreq->myproc->myreq, proc.myreq);
    } else {
        storeRelease(lk.head, proc.myreq);
    }

    // Search the list for the highest-priority waiter.
    uint32_t highpri = UINT32_MAX;
    Req *req = loadAcquire(lk.head);
    Req *highreq = req;
#if 0 // try to reach tail but slower.
    Req *tail = __atomic_load_n(&lk.tail, __ATOMIC_RELAXED);
    while (req != tail) {
        while (!req->watcher) _mm_pause();
        Proc *currproc = req->watcher;
        if (currproc->pri < highpri) {
            highpri = currproc->pri;
            highreq = req;
        }
        req = currproc->myreq;
    }
#else
    Proc *currproc = loadAcquire(req->watcher);
    while (currproc != nullptr) {
        if (currproc->pri < highpri) {
            highpri = currproc->pri;
            highreq = load(currproc->watch);
        }
        currproc = load(currproc->myreq->watcher);
    }
#endif

    // Pass the lock to the highest-priority watcher.
#if 0
    __attribute__((unused)) size_t c
        = __atomic_sub_fetch(&lk.counter, 1, __ATOMIC_RELAXED);
#endif
#if 0
    ::printf("%5u  unlocked %zu  highpri %u  highreq %p\n"
             , proc.pri, c, highpri, highreq);
    ::fflush(::stdout);
#endif
    storeRelease(highreq->isGranted, true);
    // The lock has been moved to the highreq watcher.

    freeReq(std::unique_ptr<Req>(proc.watch));
}

} //namespace lock1993


class PQ1993Lock
{
public:
    using Mutex = lock1993::Lock;
private:
    Mutex *mutex_;
    lock1993::Proc proc_;
public:
    PQ1993Lock() : mutex_(nullptr), proc_() {}
    PQ1993Lock(Mutex *mutex, uint32_t pri) : PQ1993Lock() {
        lock(mutex, pri);
    }
    ~PQ1993Lock() noexcept { unlock(); }
    PQ1993Lock(const PQ1993Lock&) = delete;
    PQ1993Lock(PQ1993Lock&&) = delete;
    PQ1993Lock operator=(const PQ1993Lock&) = delete;
    PQ1993Lock operator=(PQ1993Lock&&) = delete;
    void lock(Mutex *mutex, uint32_t pri) {
        assert(!mutex_);
        assert(mutex);
        mutex_ = mutex;
        proc_.pri = pri;
        lock1993::lock1993(*mutex_, proc_);
    }
    void unlock() noexcept {
        if (!mutex_) return;
        lock1993::unlock1993(*mutex_, proc_);
        mutex_ = nullptr;
    }
    uint32_t getTopPriorityInWaitQueue() const {
        assert(mutex_);
        assert(proc_.watch->isGranted);
        uint32_t highpri = UINT32_MAX;
        lock1993::Proc *currproc = load(mutex_->head->watcher);
        while (currproc != nullptr) {
            if (currproc->pri < highpri && currproc != &proc_) {
                highpri = currproc->pri;
            }
            //while (!currproc->myreq) _mm_pause();
            currproc = load(currproc->myreq->watcher);
        }
        return highpri;
    }
};


/**
 * CAUSION:
 * Currently PQLock1997 does not work correctly.
 * It has memory reuse problem.
 * In addition, this code is tested only x86_64, not on ARMv8.
 */
namespace lock1997 {


struct Node;


struct PCtr
{
    /**
     * 0-62  : Counter 63bit
     * 63    : Dq flag 1bit
     * 64-127: Next ptr 64bit
     */
    union {
        uint128_t obj;
        struct {
            // We assume little endian.
            uint64_t ctr:63;
            bool dq:1;
            Node *ptr; // 64bit.
        };
    };

    PCtr() : ctr(0), dq(true), ptr(nullptr) {
    }
    PCtr(uint128_t v) {
        obj = v;
    }
    operator uint128_t() const {
        return obj;
    }

    void init(Node *node = nullptr) {
        PCtr v0 = load();
        for (;;) {
            PCtr v1 = v0;
            v1.ctr++;
            v1.dq = true;
            v1.ptr = node;
            if (compareExchange(obj, v0.obj, v1.obj, __ATOMIC_RELAXED)) {
                return;
            }
        }
    }

    void setDq(bool dq0) {
        PCtr v0 = load();
        for (;;) {
            PCtr v1 = v0;
            v1.ctr++;
            v1.dq = dq0;
            if (compareExchange(obj, v0.obj, v1.obj, __ATOMIC_RELAXED)) {
                return;
            }
        }
    }

    PCtr load() const {
        return ::load(obj);
    }
};

static_assert(sizeof(PCtr) <= sizeof(uint128_t), "PCtr size proceeds uint128_t.");



struct Node
{
    PCtr next;
    uint32_t pri; // smaller is prior.
    bool isLocked; // You will get lock when this becomes false.

    void init(uint32_t pri0) {
        next.init();
        pri = pri0;
        isLocked = false;
    }
};



struct Mutex
{
    PCtr next;

    Mutex() : next() { next.init(); }

#if 0
    // Do not reuse cached nodes (with counter) for another mutex.
    // because the algorithm does not work correctly.
    using NodePool = std::map<Mutex *, std::unique_ptr<Node> >;

    static NodePool& getNodePool() {
        static thread_local NodePool np;
        return np;
    }
    std::unique_ptr<Node> allocNode() {
        NodePool& np = getNodePool();
        std::unique_ptr<Node> p;
        auto it = np.find(this);
        if (it == np.end()) {
            p = std::make_unique<Node>();
        } else {
            p = std::move(it->second);
            np.erase(it);
        }
        return p;
    }
    void freeNode(std::unique_ptr<Node>&& p) {
        NodePool& np = getNodePool();
        np.emplace(this, std::move(p));
    }
    static void gc() {
        getNodePool().clear();
    }
#else
    using NodePool = std::deque<std::unique_ptr<Node> >;

    static NodePool& getNodePool() {
        static thread_local NodePool np;
        return np;
    }
    std::unique_ptr<Node> allocNode() {
        NodePool& np = getNodePool();
        std::unique_ptr<Node> p;
#if 1
        if (np.empty()) {
            p = std::make_unique<Node>();
        } else {
            p = std::move(np.front());
            np.pop_front();
        }
#else
        p = std::make_unique<Node>();
#endif
        return p;
    }
    void freeNode(std::unique_ptr<Node>&& p) {
        NodePool& np = getNodePool();
        np.push_back(std::move(p));
    }
    static void gc() {
        getNodePool().clear();
    }
#endif
};


void lock(Mutex& mutex, PCtr& self, uint32_t pri)
{
    Node *node = mutex.allocNode().release();
    node->init(pri);
    self.init(node);
    __atomic_thread_fence(__ATOMIC_RELEASE);

    bool succeeded = false;
    bool failed = false;

    do {
        PCtr prev;
        PCtr next = load(mutex.next.obj);
        while (next.ptr == nullptr) {
            self.ctr = next.ctr + 1;
            self.dq = false;
            if (compareExchange(mutex.next.obj, next.obj, self.obj, __ATOMIC_RELAXED)) {
                self.ptr->isLocked = false;
                self.ptr->pri = 0; // max priority.
                __atomic_thread_fence(__ATOMIC_RELEASE);
                self.ptr->next.setDq(false);
                succeeded = true;
                break;
            }
        }
        if (succeeded) break;
        failed = false;
        self.ptr->isLocked = true;
        assert(self.ptr->next.dq);
        __atomic_thread_fence(__ATOMIC_RELEASE);
        size_t c = 0;
        do {
            prev = next;
            next = load(prev.ptr->next.obj);
            if (next.dq || prev.ptr->pri > self.ptr->pri) {
                failed = true;
                c++;
                continue;
            }
            if (next.ptr == nullptr || (next.ptr != nullptr && next.ptr->pri > self.ptr->pri)) {
                self.ptr->next.ptr = next.ptr;
                __atomic_thread_fence(__ATOMIC_RELEASE);
                self.ctr = next.ctr + 1;
                assert(!next.dq);
                self.dq = false;
                if (compareExchange(prev.ptr->next.obj, next.obj, self.obj, __ATOMIC_RELAXED)) {
                    self.ptr->next.setDq(false);
                    __atomic_thread_fence(__ATOMIC_ACQ_REL);
                    while (self.ptr->isLocked) _mm_pause();
                    succeeded = true;
                    break;
                }
#if 0
                if (next.dq || prev.ptr->pri > self.ptr->pri) {
                    failed = true;
                    c++;
                    continue;
                }
                next = prev;
#else
                failed = true;
                c++;
                continue;
#endif
            }
            c++;
        } while (!succeeded && !failed);
    } while (!succeeded);

    assert(!self.ptr->next.dq);
}


#if 0
static thread_local PCtr pctrV_[10];
#endif


void unlock(Mutex& mutex, PCtr& self)
{
    self.ptr->next.setDq(true);
    assert(self.ptr == mutex.next.ptr);
    __atomic_thread_fence(__ATOMIC_RELEASE);


#if 0 // check
    {
        PCtr pctr = atomicRead(mutex.next);
        size_t i = 0;
        while (pctr.ptr) {
            pctrV_[i] = pctr;
            pctr = atomicRead(pctr.ptr->next);
            i++;
        }
        pctrV_[i] = pctr;
    }
#endif


#if 0
    mutex.next = self.ptr->next;
#else
#if 1
    PCtr p0 = load(mutex.next.obj);
#else
    PCtr p0 = mutex.next;
#endif
    PCtr p1;
    for (;;) {
        p1 = load(self.ptr->next.obj);
        p1.ctr = p0.ctr + 1;
        p1.dq = p0.dq;
        if (compareExchange(mutex.next.obj, p0.obj, p1.obj, __ATOMIC_RELAXED)) break;
    }
#endif

    if (p1.ptr != nullptr) {
        Node *node = p1.ptr;

#if 1
        while (node->next.load().dq) _mm_pause();
        //while (node->next.dq) _mm_pause();
#endif
        assert(mutex.next.load().ptr == node); // QQQ

        node->pri = 0; // max priority.
        __atomic_thread_fence(__ATOMIC_RELEASE);

        assert(node->isLocked);
        node->isLocked = false;
    }

    //atomicInit(self.ptr->next); // QQQ
    mutex.freeNode(std::unique_ptr<Node>(self.ptr));
    self.ptr = nullptr;
}


} // namespace lock1997


class PQ1997Lock
{
public:
    using Mutex = lock1997::Mutex;
private:
    using Node = lock1997::Node;
    using PCtr = lock1997::PCtr;

    Mutex *mutex_;
    PCtr self_;

public:
    PQ1997Lock() : mutex_(nullptr), self_() {}
    PQ1997Lock(Mutex* mutex, uint32_t pri) : PQ1997Lock() {
        lock(mutex, pri);
    }
    ~PQ1997Lock() noexcept {
        unlock();
    }
    void lock(Mutex *mutex, uint32_t pri) {
        assert(!mutex_);
        assert(mutex);
        mutex_ = mutex;
        lock1997::lock(*mutex_, self_, pri);
    }
    void unlock() {
        if (!mutex_) return;
        lock1997::unlock(*mutex_, self_);
        mutex_ = nullptr;
    }
    uint32_t getTopPriorityInWaitQueue() const {
        assert(mutex_);
        assert(self_.ptr);
        assert(mutex_->next.ptr == self_.ptr);
        assert(!self_.ptr->isLocked);
        PCtr next = self_.ptr->next.load();
        if (next.ptr == nullptr) {
            return UINT32_MAX;
        } else {
            return next.ptr->pri;
        }
    }
};


}} // namespace cybozu::lock.
