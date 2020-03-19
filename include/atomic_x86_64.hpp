#pragma once
/*
 * This header works only x86_64 architecture.
 */
#include <cstddef>
#include <cinttypes>


#if 0
__uint128_t my_atomic_load_n(__uint128_t &t)
{
    return __atomic_load_n(&t, __ATOMIC_SEQ_CST);
}


void my_atomic_store_n(__uint128_t &m, const __uint128_t &a)
{
    __atomic_store_n(&m, a, __ATOMIC_SEQ_CST);
}


bool my_atomic_compare_exchange(__uint128_t &m, __uint128_t &b, __uint128_t &a)
{
    return __atomic_compare_exchange(&m, &b, &a, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}
#endif


void my_atomic_load_8(const uint64_t &m, uint64_t &a)
{
#if 1
    __asm__ volatile (
        "movq %1, %%rax \n\t"
        "movq %%rax, %0 \n\t"
        : "=m" (a)
        : "m" (m)
        : "rax");
#else
    a = __atomic_load_n(&m,  __ATOMIC_SEQ_CST);
#endif
}


void my_atomic_store_8(uint64_t &m, const uint64_t &a)
{
#if 1
    __asm__ volatile (
        "movq %1, %%rax \n\t"
        "movq %%rax, %1 \n\t"
        : "=m" (m)
        : "rm" (a)
        : "rax", "memory");
#else
    __atomic_store_n(&m, a,  __ATOMIC_SEQ_CST);
#endif
}


template <size_t atomic_kind>
bool my_atomic_compare_exchange_8(uint64_t &m, uint64_t &b, const uint64_t &a)
{
    if (atomic_kind == 0) {
        return __atomic_compare_exchange(
            &m, &b, (uint64_t *)&a,
            false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    } else if (atomic_kind == 1) {
        uint64_t b2;
        b2 = __sync_val_compare_and_swap(&m, b, a);
        if (b2 == b) {
            return true;
        } else {
            b = b2;
            return false;
        }
    } else {
#if 0
        bool ret;
        __asm__ volatile (
            "movl  (%2), %%eax \n\t"
            "movl 4(%2), %%edx \n\t"
            "movl  (%3), %%ebx \n\t"
            "movl 4(%3), %%ecx \n\t"
            "lock cmpxchg8b (%1) \n\t"
            "sete %0 \n\t"
            "movl %%eax,  (%2) \n\t"
            "movl %%edx, 4(%2) \n\t"
            : "=r" (ret)
            : "r" (&m), "r" (&b), "r" (&a)
            : "eax", "ebx", "ecx", "edx", "memory");

        return ret;
#else
        // cmpxchg is faster than cmpxchg8b.
        bool ret;
        __asm__ volatile (
            "lock cmpxchgq %3, %2 \n\t"
            "sete %0 \n\t"
            : "=r" (ret), "+a" (b)
            : "m" (m), "r" (a)
            : "memory");
        return ret;
#endif
    }
}


void my_atomic_load_16(const __uint128_t &m, __uint128_t &a)
{
#if 1
    __asm__ volatile (
        "movl $0, %%eax \n\t"
        "movl $0, %%edx \n\t"
        "movl $0, %%ebx \n\t"
        "movl $0, %%ecx \n\t"
        "lock cmpxchg16b (%0) \n\t"
        "movq %%rax, (%1) \n\t"
        "movq %%rdx, 8(%1) \n\t"
        :: "r" (&m), "r" (&a)
        : "rax", "rbx", "rcx", "rdx");
#else
    a = __atomic_load_n(&m, __ATOMIC_SEQ_CST);
#endif
}


void my_atomic_store_16(__uint128_t &m, const __uint128_t &v)
{
#if 1
    __asm__ volatile (
        "movq  (%0), %%rax \n\t"
        "movq 8(%0), %%rdx\n\t"
        "L0_%=: \n\t"
        "movq  (%1), %%rbx \n\t"
        "movq 8(%1), %%rcx \n\t"
        "lock cmpxchg16b (%0) \n\t"
        "jne L0_%= \n\t"
        :: "r" (&m), "r" (&v)
        : "rax", "rbx", "rcx", "rdx", "memory");
#elif 0
    __atomic_store_n(&m, v, __ATOMIC_SEQ_CST);
#else
    __uint128_t b = m;
    while (!my_atomic_compare_exchange_16(m, b, v)) {}
#endif
}


template <size_t atomic_kind>
bool my_atomic_compare_exchange_16(__uint128_t &m, __uint128_t &b, const __uint128_t &a)
{
    if (atomic_kind == 0) {
        return __atomic_compare_exchange(
            &m, &b, (__uint128_t *)&a,
            false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    } else if (atomic_kind == 1) {
        __uint128_t b2;
        b2 = __sync_val_compare_and_swap(&m, b, a);
        if (b2 == b) {
            return true;
        } else {
            b = b2;
            return false;
        }
    } else {
#if 0
        bool ret;
        __asm__ volatile (
            "movq  (%2), %%rax \n\t"
            "movq 8(%2), %%rdx \n\t"
            "movq  (%3), %%rbx \n\t"
            "movq 8(%3), %%rcx \n\t"
            "lock cmpxchg16b %1 \n\t"
            "sete %0 \n\t"
            "testb %0, %0 \n\t"
            "jne L0_%=\n\t"
            "movq %%rax,  (%2) \n\t"
            "movq %%rdx, 8(%2) \n\t"
            "L0_%=: \n\t"
            : "=r" (ret)
            : "m" (m), "r" (&b), "r" (&a)
            : "rax", "rbx", "rcx", "rdx", "memory");
        return ret;
#else
        bool ret;
        // This version avoid jump instead two movq.
        __asm__ volatile (
            "movq  (%2), %%rax \n\t"
            "movq 8(%2), %%rdx \n\t"
            "movq  (%3), %%rbx \n\t"
            "movq 8(%3), %%rcx \n\t"
            "lock cmpxchg16b %1 \n\t"
            "sete %0 \n\t"
            "movq %%rax,  (%2) \n\t"
            "movq %%rdx, 8(%2) \n\t"
            : "=r" (ret)
            : "m" (m), "r" (&b), "r" (&a)
            : "rax", "rbx", "rcx", "rdx", "memory");
        return ret;
#endif
    }
}


void my_atomic_load(const __uint128_t &m, __uint128_t &v)
{
    my_atomic_load_16(m, v);
}


void my_atomic_load(const uint64_t &m, uint64_t &v)
{
    my_atomic_load_8(m, v);
}


template <size_t atomic_kind>
bool my_atomic_compare_exchange(__uint128_t &m, __uint128_t &b, const __uint128_t &a)
{
    return my_atomic_compare_exchange_16<atomic_kind>(m, b, a);
}


template <size_t atomic_kind>
bool my_atomic_compare_exchange(uint64_t &m, uint64_t &b, const uint64_t &a)
{
    return my_atomic_compare_exchange_8<atomic_kind>(m, b, a);
}
