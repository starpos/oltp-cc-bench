#pragma once

#define _mm_pause() __asm__ volatile ("" ::: "memory")
