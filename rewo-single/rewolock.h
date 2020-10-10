#pragma once

#include "storage.h"

/* in-GCC atomic primitive */
// type __sync_val_compare_and_swap (type *ptr, type oldval, type newval, ...);
// P: data address
// O: old value
// N: new value
#define cmpxchg(P, O, N) __sync_val_compare_and_swap((P), (O), (N))


inline bool lock(uint8_t *lockmap, int i) {
    int tmp = *lockmap;
    if (*lockmap & (BIT_FLAG >> i)) {
        return false;
    }
    else{
        int ret = cmpxchg(lockmap, tmp, *lockmap | (BIT_FLAG >> i));
        return ret == tmp;
    }
}

inline void unlock(uint8_t *lockmap, int i) {
    *lockmap &= (~BIT_FLAG >> i);
}

inline void unlock(uint8_t *lockmap, int i, int j) {
    *lockmap &= (~BIT_FLAG >> i);
    *lockmap &= (~BIT_FLAG >> j);
}

inline void commit(uint8_t *bitmap, int i) {
    *bitmap |= (BIT_FLAG >> i);
}

inline void commit(uint8_t *bitmap, int off_old, int off_new) {
    *bitmap &= (~BIT_FLAG >> off_old);
    *bitmap |= (BIT_FLAG >> off_new);
}

inline void invalidate(uint8_t *bitmap, int i) {
    *bitmap &= (~BIT_FLAG >> i);
}

inline void kv_atomic_add(uint32_t &total_kv_num) {
    cmpxchg(&total_kv_num, total_kv_num, total_kv_num+1);
}

inline int resize_prepare(uint32_t &resize_flag) {
    int ret = cmpxchg(&resize_flag, 0, 1);
    if (ret == 0)
        return 0;
    return -1;
}

// order -> slot no.
// 4 slots has an order mapping: 1 -> 2, 2 - > 0, 3 -> 3, 4 -> 1
// if slot 0 is updated, then the mapping becomes: 1 -> 0, 2 -> 2, 3 -> 3, 4 -> 1
//
inline void lru_update(uint32_t *lru, int off) {
    int order = 0;
    bool find_flag = false;
    for (int i = 0;i < SLOT_PER_BUCKET;i++) {
        auto slot = *lru & (FP_MASK << (FP_BIT_NUM * (SLOT_PER_BUCKET - i - 1)));
        if (slot == off) {
            find_flag = true;
            order = i;
            break;
        }
    }

    if (find_flag) {
        if (order == 0)
            return;
        else {
            for (int i = order-1; i >= 0;i--) {
                auto slot = *lru & (FP_MASK << (FP_BIT_NUM * (SLOT_PER_BUCKET - i - 1)));
                *lru &= ~(FP_MASK << (FP_BIT_NUM * (SLOT_PER_BUCKET - order - 1)));
                *lru |= (slot << (FP_BIT_NUM * (SLOT_PER_BUCKET - order - 1)));
            }
            *lru &= ~(FP_MASK << (FP_BIT_NUM * (SLOT_PER_BUCKET -  1)));
            *lru |= (off << (FP_BIT_NUM * (SLOT_PER_BUCKET - 1)));
        }
    }
    else {
        *lru = *lru >> (FP_BIT_NUM);
        *lru |= (off << (FP_BIT_NUM * (SLOT_PER_BUCKET - 1)));
    }
}

inline void lru_update(uint32_t *lru) {
    auto off = *lru & FP_MASK;
    *lru = *lru >> (FP_BIT_NUM);
    *lru |= (off << (FP_BIT_NUM * (SLOT_PER_BUCKET - 1)));
}

inline int lru_evict(uint32_t *lru) {
    return *lru & FP_MASK;
}

