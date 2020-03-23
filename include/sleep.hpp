#pragma once
#include <chrono>
#include <thread>

void sleep_ms(size_t ms)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}


void sleep_us(size_t us)
{
    std::this_thread::sleep_for(std::chrono::microseconds(us));
}
