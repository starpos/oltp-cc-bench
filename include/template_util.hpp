#pragma once


template <typename T1, typename T2, bool flag = (sizeof(T1) > sizeof(T2))>
struct get_large_type
{
    using type = T1;
};


template <typename T1, typename T2>
struct get_large_type<T1, T2, false>
{
    using type = T2;
};
