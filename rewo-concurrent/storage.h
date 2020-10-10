#pragma once

#include <time.h>
#include <cstdint>
#include "trxn.h"
#include "config.h"

#if (NVM_BLOCK_SIZE >= 128)
#define BUCKET_SIZE (NVM_BLOCK_SIZE)
#else
#define BUCKET_SIZE (128)
#endif

/* key size: smaller than 16B, including 16B */
#define KEY_SIZE (16)
/* bucket metadata: 8B, supporting atomic modification */
#define BUCKET_META_SIZE (8)
/* bucket data size: 248B by default */
#define BUCKET_KV_SIZE (BUCKET_SIZE - BUCKET_META_SIZE)
/* bucket slot number: 8 by default */
#define SLOT_PER_BUCKET (BUCKET_SIZE / (KEY_SIZE * 2))
/* value size: smaller than 15B, including 15B, by default */
#define VALUE_SIZE (BUCKET_KV_SIZE / SLOT_PER_BUCKET - KEY_SIZE)
#define KV_SIZE (KEY_SIZE + VALUE_SIZE)

#define BIT_FLAG (1 << (SLOT_PER_BUCKET - 1))

/* fingerprints use: 4-bit per slot by default */
#define FP_BIT_NUM (32 / SLOT_PER_BUCKET)
/* fingerprints hashing mod value: 16 by default */
#define FP_MAX_NUM (1 << FP_BIT_NUM)
/* fingerprints mask: 15 by default */
#define FP_MASK (FP_MAX_NUM - 1)

#define SUPER_SIZE (sizeof(Super))
#define PBUCKET_NUM NVM_BUCKET_NUM
#define CBUCKET_NUM (PBUCKET_NUM / NVM_TO_DRAM_RATIO)
#define PKV_SIZE (BUCKET_SIZE * PBUCKET_NUM)
#define DKV_SIZE (BUCKET_SIZE * CBUCKET_NUM)
#define TOTAL_SIZE (SUPER_SIZE + PKV_SIZE) //allocated size

#define BUF_TUPLE_SIZE (sizeof(buffer_tuple))

#define ZONE_NUM 8

#define MAGIC 12345

/*
 * Rewo-Hash can support different forms of keys and values (integer and string),
 * and variable-size keys and values (via strlen for string type).
 */
typedef struct {
    union {
        uint64_t key;
        char ckey[KEY_SIZE];
    } __attribute__((packed));
    union {
        uint64_t val;
        char value[VALUE_SIZE];
    } __attribute__((packed));
} __attribute__((packed)) KeyValue;

typedef struct {
    // 8-byte bucket metadata
    struct {
        uint8_t bitmap;
        uint8_t lockmap;
        uint16_t padding;
        union {
            uint32_t fingerprints;
            uint32_t lru_sorteds;
        };
    } __attribute__((packed)) meta;

    KeyValue kv[SLOT_PER_BUCKET];
} __attribute__((packed)) Bucket;

typedef struct{
    uint64_t used_nvm_size;
    uint32_t ptable_num;
//    uint32_t ctable_num; // only one cached table for simplicity
    uint32_t max_load_num;
    uint32_t kv_num;
    float load_factor;

    // 0. normal
    // 1. rehashing
    int resize_flag;
    uint32_t resize_table_seq;
    uint32_t resize_bucket_seq;
    // 0. normal
    // 1. overflowing
    int cache_overflow_flag;
    int magic;

    // zone pointers
    // in fact it is offsets in the physical PM file
    uint64_t table_addr[ZONE_NUM];
    uint64_t table_offset[ZONE_NUM];
    uint32_t bucket_num[ZONE_NUM];
    uint32_t cbucket_num;
} __attribute__((aligned(4096))) Super;

/* frontend-backend coordination buffer  */
typedef struct {
    /*
     * flag = 0: NON
     * flag = 1: INSERT
     * flag = 2: UPDATE
     * flag = 3: DELETE
     * flag = 4: COMPLETE
     */
    int sync_flag;
    union {
        uint64_t key;
        char *ckey;
    };

    union {
        char *v_addr;
        struct {
            // uint8_t *lockmap;
            uint64_t lockmap_addr;
            int off;
        } lock_info;
    };
} buffer_tuple;

#define MAX_BACKEND_THREAD_NUM 4
extern int FRONTEND_THREAD_NUM;
extern int BACKEND_THREAD_NUM;
extern buffer_tuple *TupleSet;

extern char *dram_base_addr;
extern void *nvm_force_addr;
extern char *nvm_base_addr;

extern Bucket *CBucket;
extern Super *SP;
extern Super *SP_DRAM;


void pm_init(int ntype);
void cache_init();
void ht_exit();
void buffer_init();
