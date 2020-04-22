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
INLINE void release_owner(uintptr_t& tail, Request*& head)
{
    uintptr_t tail0 = load_acquire(tail);
    if (tail0 == OWNED && compare_exchange(tail, tail0, UNOWNED)) return;
    Request* head0;
    while ((head0 = load(head)) == nullptr) _mm_pause();
    store(head, nullptr);
    head0->delegate_ownership();
}


template <typename Request, typename Func>
INLINE void do_owner_task(uintptr_t& tail, Request*& head, Func&& owner_task)
{
    Request* tail0 = to_req_ptr<Request>(exchange(tail, OWNED));
    owner_task(*tail0);
    release_owner<Request>(tail, head);
}


/**
 * owner_task will receive the tail pointer.
 * The owner request must deal with the request list starting from the owner request itself
 * to the tail request. The list size is at least 1. If so, the tail is the owner request.
 */
template <typename Request, typename Func>
INLINE void do_request_async(Request& req, uintptr_t& tail, Request*& head, Func&& owner_task)
{
    uintptr_t prev = exchange(tail, from_req_ptr(&req));
    if (prev == UNOWNED) {
        do_owner_task<Request, Func>(tail, head, std::move(owner_task));
        return;
    }
    if (prev == OWNED) {
        store_release(head, &req);
        req.wait_for_ownership();
        do_owner_task<Request, Func>(tail, head, std::move(owner_task));
        return;
    }
    Request& prev_req = *to_req_ptr<Request>(prev);
    prev_req.set_next(&req);
}


template <typename Request, typename Func>
INLINE typename Request::Message do_request_sync(
    Request& req, uintptr_t& tail, Request*& head, Func&& owner_task)
{
    do_request_async<Request, Func>(req, tail, head, std::move(owner_task));
    return req.local_spin_wait();
}


} // namespace mcslike
