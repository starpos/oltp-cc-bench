#pragma once
/**
 * Priority-queuing locks.
 */
#include "lock.hpp"

namespace cybozu {
namespace lock {

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
#ifdef MUTEX_ON_CACHELINE
        alignas(CACHE_LINE_SIZE)
#endif
        Node *tail;
        std::vector<Node*> buf; // sort buffer;
        Mutex() : tail(nullptr), buf() {}
    };
private:
    Mutex *mutex_; /* shared pointer to the tail of list. */
    Node node_; /* list node. */

    Node *readd(Node *node) {
        // node must not be the tail item.
        Node *next = node->next;
        node->next = nullptr;
        Node *prev = __atomic_exchange_n(&mutex_->tail, node, __ATOMIC_RELAXED);
        assert(prev != nullptr);
        prev->next = node;
        __atomic_thread_fence(__ATOMIC_RELEASE);
        return next;
    }
    Node *remove(Node *node) {
        Node *next = node->next;
        node->next = nullptr;
        return next;
    }
    void readd2(Node *first, Node *last) {
        Node *prev = __atomic_exchange_n(&mutex_->tail, last, __ATOMIC_RELAXED);
        assert(prev != nullptr);
        prev->next = first;
        __atomic_thread_fence(__ATOMIC_RELEASE);
    }
    void reorder() {
        assert(node_.next != nullptr);
        Node *minP, *p;
        // search item with minimum priority.
        p = node_.next;
        minP = p;
        size_t c1 = 0;
        size_t c2 = 0;
        while (p->next) {
            p = p->next;
            c2++;
            if (p->pri < minP->pri) {
                minP = p;
                c1 = c2;
            }
        }
        // move privious items to the tail of the list.
        p = node_.next;
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
	            (*i)->next = *j;
	            ++i;
		    ++j;
	        }
	        readd2(v.front(), v.back());
	    }
	}
#endif
        node_.next = minP;
    }
public:
    PQMcsLock() : mutex_(nullptr), node_() {}
    PQMcsLock(Mutex *mutex, uint32_t pri) : PQMcsLock() { lock(mutex, pri); }
    ~PQMcsLock() noexcept { unlock(); }
    void lock(Mutex *mutex, uint32_t pri) {
        assert(!mutex_);
        node_.init();
        mutex_ = mutex;
        node_.pri = pri;

        Node *prev = __atomic_exchange_n(&mutex_->tail, &node_, __ATOMIC_RELAXED);
        if (prev) {
            node_.wait = true;
            prev->next = &node_;
            __atomic_thread_fence(__ATOMIC_RELEASE);
            while (node_.wait) _mm_pause();
        }
    }
    void unlock() noexcept {
        if (!mutex_) return;

        if (!node_.next) {
            Node *node = &node_;
            if (__atomic_compare_exchange_n(
                    &mutex_->tail, &node, nullptr, false,
                    __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
                return;
            }
            while (!node_.next) _mm_pause();
        }
        //mutex_.next is not null.
        reorder();
        node_.next->wait = false;

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
#ifdef MUTEX_ON_CACHELINE
        alignas(CACHE_LINE_SIZE)
#endif
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
#ifdef MUTEX_ON_CACHELINE
        alignas(CACHE_LINE_SIZE)
#endif
        Node *tail; // This must be changed by CAS.
        size_t nr; // Number of nodes. atomic access is required.

        // Only the thread holding lock can change this.
        //Node dummy[2];
        //uint8_t dummyId;
        std::deque<std::unique_ptr<Node> > dummy;
        PriQueue priQ;
        size_t dummyAlloc; // debug
        size_t dummyFree; // debug

        Mutex() : tail(nullptr), nr(0), dummy(), priQ()
                , dummyAlloc(0), dummyFree(0) {}
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
        atomicFetchAdd(&mutex_->nr, 1);
        Node *tail = addToTail(&node_);
        if (!tail) {
            // Now lock is held.
            __atomic_thread_fence(__ATOMIC_ACQUIRE);
            node_.wait = false;
            Node *dummy = getNewDummy();
            //while (dummy->next) _mm_pause();
            //dummy->next = HAZARD_PTR;
            addToTail(dummy);
            //__atomic_thread_fence(__ATOMIC_ACQUIRE);
            while (!node_.next) _mm_pause();
            //dummy->next = nullptr;
            assert(mutex_->priQ.empty());
            moveListToPriQ(node_.next);
        } else {
            //node_.wait = true;
            //while (tail->next == HAZARD_PTR) _mm_pause();
            while (node_.wait) _mm_pause();
            // Now lock is held.
            __atomic_thread_fence(__ATOMIC_ACQUIRE);
        }
        node_.next = nullptr;
    }
    void unlock() {
        if (!mutex_) return;
        assert(!node_.wait);

        Node *dummy = getDummy();
        if (mutex_->priQ.empty() && !dummy->next) {
            if (compareAndSwap(&mutex_->tail, &dummy, nullptr)) {
                /*
                 * There is no thread having lock now.
                 *
                 * Do not check assert(mutex_->dummy.next == nullptr);
                 * Because the dummy node may be used by another thread that have new lock.
                 */
                __atomic_thread_fence(__ATOMIC_RELEASE);
                atomicFetchSub(&mutex_->nr, 1);
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
        __atomic_thread_fence(__ATOMIC_RELEASE);
        p->wait = false; // Release lock and the thread on p will hold lock.

        atomicFetchSub(&mutex_->nr, 1);
        mutex_ = nullptr;
    }
    uint32_t getSelfPriority() const { return node_.pri; }
    uint32_t getTopPriorityInWaitQueue() {
        assert(!node_.wait);
        if (getDummy()->next) {
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
#if 0
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
        while (newDummy->next) _mm_pause(); // QQQ
        //newDummy->next = HAZARD_PTR;
        newDummy->next = nullptr;
#endif
        //__atomic_thread_fence(__ATOMIC_RELEASE);
        addToTail(newDummy);
        //__atomic_thread_fence(__ATOMIC_ACQUIRE);
#if 1
        while (!oldDummy->next) _mm_pause();
#else
        waitForReachable(oldDummy, newDummy);
#endif
        Node *head = oldDummy->next;
        oldDummy->next = nullptr;
        //__atomic_thread_fence(__ATOMIC_RELEASE);
        return head;
    }
    Node *addToTail(Node *node) {
        Node *prev = atomicSwap(&mutex_->tail, node);
        if (prev) {
            //while (prev->next == HAZARD_PTR) _mm_pause();
            prev->next = node;
            __atomic_thread_fence(__ATOMIC_RELEASE);
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
        size_t nr = atomicLoad(&mutex_->nr);
        std::deque<std::unique_ptr<Node> >& d = mutex_->dummy;
        if (d.size() < nr) {
            if (pool.empty()) {
                d.push_back(std::make_unique<Node>());
                atomicFetchAdd(&mutex_->dummyAlloc, 1);
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
        size_t nr = atomicLoad(&mutex_->nr);
        std::deque<std::unique_ptr<Node> >& d = mutex_->dummy;
        if (d.size() > nr) {
            pool.push_front(std::move(d.front()));
            d.pop_front();
        }
        if (pool.size() > MAX_NODE_POOL_SIZE) {
            atomicFetchAdd(&mutex_->dummyFree, pool.size() - MAX_NODE_POOL_SIZE);
            pool.resize(MAX_NODE_POOL_SIZE);
        }
    }
    size_t moveListToPriQ(Node *p) {
        size_t c = 0;
        while (!isDummy(p)) {
            mutex_->priQ.push(p);
            c++;
            __atomic_thread_fence(__ATOMIC_ACQUIRE);
            while (!p->next) _mm_pause();
            p = p->next;
        }
        return c;
    }
    size_t waitForReachable(const Node *first, const Node *last) const {
        const Node *p = first;
        size_t c = 0;
        while (p != last) {
            __atomic_thread_fence(__ATOMIC_ACQUIRE);
            while (!p->next) _mm_pause();
            p = p->next;
            c++;
        }
        return c;
    }
    //static constexpr int AtomicMode = __ATOMIC_SEQ_CST;
    static constexpr int AtomicMode = __ATOMIC_RELAXED;
    template <typename T> static T atomicLoad(T* t) {
        return __atomic_load_n(t, AtomicMode);
    }
    template <typename T, typename U> static void atomicStore(T* t, U v) {
        __atomic_store_n(t, static_cast<T>(v), AtomicMode);
    }
    template <typename T, typename U> static bool compareAndSwap(T* t, T* before, U after) {
        return __atomic_compare_exchange_n(t, before, static_cast<T>(after), false, AtomicMode, __ATOMIC_RELAXED);
    }
    template <typename T, typename U> static T atomicSwap(T* t, U v) {
        return __atomic_exchange_n(t, v, AtomicMode);
    }
    template <typename T, typename U> static T atomicFetchAdd(T* t, U v) {
        return __atomic_fetch_add(t, static_cast<T>(v), AtomicMode);
    }
    template <typename T, typename U> static T atomicFetchSub(T* t, U v) {
        return __atomic_fetch_sub(t, static_cast<T>(v), AtomicMode);
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
#ifdef MUTEX_ON_CACHELINE
        alignas(CACHE_LINE_SIZE)
#endif
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
    //__atomic_thread_fence(__ATOMIC_RELEASE);
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
#ifdef MUTEX_ON_CACHELINE
    alignas(CACHE_LINE_SIZE)
#endif
    Req *head;
    Req *tail;
    size_t counter; // QQQ
    Lock() : head(), tail(), counter(0) {
        Req *p = allocReq().release();
        p->myproc = nullptr;
        p->watcher = nullptr;
        p->isGranted = true;
        tail = p;
        //__atomic_thread_fence(__ATOMIC_RELEASE);
        head = p;
    }
};


void lock1993(Lock& lk, Proc& proc)
{
    proc.myreq = allocReq().release();
    proc.myreq->myproc = &proc;
#if 0
    ::printf("%5u  req %p\n", proc.pri, proc.myreq); // QQQ
#endif
    proc.watch = __atomic_exchange_n(&lk.tail, proc.myreq, __ATOMIC_RELEASE);
    assert(proc.watch);
    assert(!proc.watch->watcher);
    proc.watch->watcher = &proc;
#if 0
    ::printf("pri %u watch %p\n", proc.pri, proc.watch); // QQQ
#endif
    while (!proc.watch->isGranted) _mm_pause();

    // QQQ
#if 0
    __attribute__((unused)) size_t c
        = __atomic_fetch_add(&lk.counter, 1, __ATOMIC_RELAXED);
#if 0
    ::printf("%5u  locked %zu\n", proc.pri, c); // QQQ
    ::fflush(::stdout);
#endif
    if (c != 0) {
        assert(false);
    }; // QQQ
#endif
}


void unlock1993(Lock& lk, Proc& proc)
{
    // Remove my Process and the Request I watched from the list.
    //while (proc.myreq->myproc != &proc) _mm_pause();
    assert(proc.myreq->myproc == &proc);
    assert(proc.watch->watcher == &proc);

    //__atomic_thread_fence(__ATOMIC_ACQUIRE);
    proc.myreq->myproc = proc.watch->myproc;
    //__atomic_thread_fence(__ATOMIC_RELEASE);
    if (proc.myreq->myproc != nullptr) {
        proc.myreq->myproc->myreq = proc.myreq;
    } else {
        lk.head = proc.myreq;
    }
    //__atomic_thread_fence(__ATOMIC_ACQ_REL);

    // Search the list for the highest-priority waiter.
    uint32_t highpri = UINT32_MAX;
    Req *req = lk.head;
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
    Proc *currproc = req->watcher;
    while (currproc != nullptr) {
        if (currproc->pri < highpri) {
            highpri = currproc->pri;
            highreq = currproc->watch;
        }
        currproc = currproc->myreq->watcher;
    }
#endif

    // Pass the lock to the highest-priority watcher.
    //__atomic_thread_fence(__ATOMIC_RELEASE);
#if 0
    __attribute__((unused)) size_t c
        = __atomic_sub_fetch(&lk.counter, 1, __ATOMIC_RELAXED);
#endif
#if 0
    ::printf("%5u  unlocked %zu  highpri %u  highreq %p\n"
             , proc.pri, c, highpri, highreq); // QQQ
    ::fflush(::stdout);
#endif
    highreq->isGranted = true;

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
        assert(mutex_);
        lock1993::unlock1993(*mutex_, proc_);
        mutex_ = nullptr;
    }
    uint32_t getTopPriorityInWaitQueue() const {
        assert(mutex_);
        assert(proc_.watch->isGranted);
        uint32_t highpri = UINT32_MAX;
        lock1993::Proc *currproc = mutex_->head->watcher;
        while (currproc != nullptr) {
            if (currproc->pri < highpri && currproc != &proc_) {
                highpri = currproc->pri;
            }
            //while (!currproc->myreq) _mm_pause();
            currproc = currproc->myreq->watcher;
        }
        return highpri;
    }
};


namespace lock1997 {


struct Node;


struct PCtr
{
    /**
     * 127-64: Next ptr 64bit
     * 63    : Dq flag 1bit
     * 62-0  : Counter 63bit
     */
    union {
        uint128_t obj;
        struct {
            Node *ptr; // 8 bytes;
            bool dq:1;
            uint64_t ctr:63;
        };
    };
#if 0
    alignas(sizeof(uint128_t))
    Node *ptr; // 8 bytes;
    bool dq:1;
    uint64_t ctr:63;
#endif

    PCtr() : ptr(nullptr), dq(true), ctr(0) {}
    PCtr(uint128_t v) {
        //::memcpy(this, &v, sizeof(*this));
        obj = v;
    }
    operator uint128_t() const {
#if 0
        uint128_t v;
        ::memcpy(&v, this, sizeof(v));
        return v;
#else
        return obj;
#endif
    }
};


static_assert(sizeof(PCtr) <= sizeof(uint128_t), "PCtr size proceeds uint128_t.");


PCtr atomicRead(const PCtr& pctr)
{
    return __atomic_load_n(&pctr.obj, __ATOMIC_ACQUIRE);
}


bool compareAndSwap(PCtr& pctr, PCtr& expected, PCtr desired)
{
    return __atomic_compare_exchange_n(
        &pctr.obj, &expected.obj, desired.obj,
        false, __ATOMIC_RELEASE, __ATOMIC_RELAXED);
}

void atomicInit(PCtr& pctr)
{
    bool failed = true;
    PCtr pctr0 = pctr;
    while (failed) {
        PCtr pctr1 = pctr0;
        pctr1.ptr = nullptr;
        pctr1.dq = true;
        pctr1.ctr++; // DO NOT RESET ctr.
        failed = !__atomic_compare_exchange_n(
            &pctr.obj, &pctr0.obj, pctr1.obj,
            false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
    }
}

void init(PCtr& pctr)
{
    pctr.ptr = nullptr;
    pctr.dq = true;
    pctr.ctr++;
}


void atomicSetDq(PCtr& pctr, bool v)
{
    bool failed = true;
    PCtr pctr0 = pctr;
    while (failed) {
        PCtr pctr1 = pctr0;
        pctr1.dq = v;
        pctr1.ctr++; // QQQ
        failed = !__atomic_compare_exchange_n(
            &pctr.obj, &pctr0.obj, pctr1.obj,
            false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
    }
}


struct Node
{
#ifdef MUTEX_ON_CACHELINE
    alignas(CACHE_LINE_SIZE)
#endif
    PCtr next;
    uint32_t pri; // smaller is prior.
    bool isLocked; // You will get lock when this becomes false.

    void init(uint32_t pri0) {
        atomicInit(next);
        pri = pri0;
        isLocked = false;
    }
};



struct Mutex
{
#ifdef MUTEX_ON_CACHELINE
    alignas(CACHE_LINE_SIZE)
#endif
    PCtr next;

    Mutex() : next() { atomicInit(next); }

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
    atomicInit(self);
    self.ptr = node;
    __atomic_thread_fence(__ATOMIC_RELEASE);

    bool succeeded = false;
    bool failed = false;

    do {
        PCtr prev;
        PCtr next = atomicRead(mutex.next);
        while (next.ptr == nullptr) {
            self.ctr = next.ctr + 1;
            self.dq = false;
            if (compareAndSwap(mutex.next, next, self)) {
                self.ptr->isLocked = false;
                self.ptr->pri = 0; // max priority.
                __atomic_thread_fence(__ATOMIC_RELEASE);
#if 0
                self.ptr->next.dq = false;
#else
                atomicSetDq(self.ptr->next, false);
#endif
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
            next = atomicRead(prev.ptr->next);
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
                if (compareAndSwap(prev.ptr->next, next, self)) {
#if 0
                    self.ptr->next.dq = false;
#else
                    atomicSetDq(self.ptr->next, false);
#endif
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

    assert(!atomicRead(self.ptr->next).dq);
}


#if 0
static thread_local PCtr pctrV_[10];
#endif


void unlock(Mutex& mutex, PCtr& self)
{
#if 0
    self.ptr->next.dq = true;
#else
    atomicSetDq(self.ptr->next, true);
#endif
    assert(self.ptr == atomicRead(mutex.next).ptr);
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
    PCtr p0 = atomicRead(mutex.next);
#else
    PCtr p0 = mutex.next;
#endif
    PCtr p1;
    for (;;) {
        p1 = atomicRead(self.ptr->next);
        p1.ctr = p0.ctr + 1;
        p1.dq = p0.dq;
        if (compareAndSwap(mutex.next, p0, p1)) break;
    }
#endif

    if (p1.ptr != nullptr) {
        Node *node = p1.ptr;

#if 1
        while (atomicRead(node->next).dq) _mm_pause();
        //while (node->next.dq) _mm_pause();
#endif
        assert(atomicRead(mutex.next).ptr == node); // QQQ

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
        PCtr next = atomicRead(self_.ptr->next);
        if (next.ptr == nullptr) {
            return UINT32_MAX;
        } else {
            return next.ptr->pri;
        }
    }
};


}} // namespace cybozu::lock.
