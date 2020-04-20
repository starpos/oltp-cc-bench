#pragma once
#include <cinttypes>
#include "inline.hpp"
#include "atomic_wrapper.hpp"
#include "arch.hpp"


/**
 * Helper functions to implement locking protocols with MCS-like lock template.
 *
 * Request::Message type must exists.
 * Request must have set_next() member function of void(Request*) type.
 * Request must have delegate_ownership() member function of void() type.
 * Request must have wait_for_ownership() member function of void() type.
 * Request must have local_spin_wait() member function of Message() type.
 * Func must be void(Request&) type.
 */
namespace mcslike {


constexpr uintptr_t UNOWNED = 0;
constexpr uintptr_t OWNED = 1;


template <typename Request>
INLINE Request* to_req_ptr(uintptr_t req)
{
    return reinterpret_cast<Request*>(req);
}

template <typename Request>
INLINE uintptr_t from_req_ptr(Request* req)
{
    return uintptr_t(req);
}


template <typename Request>
INLINE void release_owner(uintptr_t& tail_, Request*& head_)
{
    uintptr_t tail = load_acquire(tail_);
    if (tail == OWNED && compare_exchange(tail_, tail, UNOWNED)) return;
    Request* head;
    while ((head = load(head_)) == nullptr) _mm_pause();
    store(head_, nullptr);
    head->delegate_ownership();
}


template <typename Request, typename Func>
INLINE void do_owner_task(uintptr_t& tail_, Request*& head_, Func&& owner_task)
{
    Request* tail = to_req_ptr<Request>(exchange(tail_, OWNED));
    owner_task(*tail);
    release_owner<Request>(tail_, head_);
}


/**
 * owner_task will receive the tail pointer.
 * The owner request must deal with the request list starting from the owner request itself
 * to the tail request. The list size is at least 1. If so, the tail is the owner request.
 */
template <typename Request, typename Func>
INLINE void do_request_async(Request& req, uintptr_t& tail_, Request*& head_, Func&& owner_task)
{
    uintptr_t prev = exchange(tail_, from_req_ptr(&req));
    if (prev == UNOWNED) {
        do_owner_task<Request, Func>(tail_, head_, std::move(owner_task));
        return;
    }
    if (prev == OWNED) {
        store_release(head_, &req);
        req.wait_for_ownership();
        do_owner_task<Request, Func>(tail_, head_, std::move(owner_task));
        return;
    }
    Request& prev_req = *to_req_ptr<Request>(prev);
    prev_req.set_next(&req);
}


template <typename Request, typename Func>
INLINE typename Request::Message do_request_sync(
    Request& req, uintptr_t& tail_, Request*& head_, Func&& owner_task)
{
    do_request_async<Request, Func>(req, tail_, head_, std::move(owner_task));
    return req.local_spin_wait();
}


} // namespace mcslike
