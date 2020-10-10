#pragma once

#include <string.h>
#include <emmintrin.h>
#include <xmmintrin.h>
#include "storage.h"

// #define CLWB_MODE

#define clwb(addr) ({ \
    __asm__ __volatile__ ("clwb %0" : : "m"(*addr)); \
})

#define clflush(addr) ({ \
    __asm__ __volatile__ ("clflush %0" : : "m"(*addr)); \
})

#define fence() ({ \
    __asm__ __volatile__ ("sfence":::"memory"); \
})


inline void flush_with_fence(void *addr) {
#ifdef CLWB_MODE
    clwb((char *)addr);
#else
    clflush((char *)addr);
#endif
    fence();
}

inline void ntstore(char *addr, char *wbuffer, uint32_t size) {
    int *s = (int *)addr;
    int *t = (int *)wbuffer;

    int round = (size-1) / 4 + 1;
    for (int i = 0;i < round;i++) {
        _mm_stream_si32(s, *t);
        s++;
        t++;
    }
}

