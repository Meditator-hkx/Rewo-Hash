#include "hash.h"
#include "storage.h"
#include <algorithm>
#include <iostream>

std::hash<unsigned long long> int_hasher;
std::hash<char *> str_hasher;

uint32_t consistent_hash(uint64_t key) {
    // obtain the binary form of key
    if (SP->ptable_num == 1) {
        return 0;
    }
    else {
        uint64_t bit_flag = 1ULL;
        for (uint32_t i = SP_DRAM->ptable_num - 2;i >= 0;i--) {
            if ((key & (bit_flag << i)) > 0) {
                return (i+1);
            }
        }
        return 0;
    }
}

uint32_t consistent_hash(char *key) {
    // obtain the binary form of key
    if (SP->ptable_num == 1) {
        return 0;
    }
    else {
        uint64_t bit_flag = 1ULL;
        for (uint32_t i = SP_DRAM->ptable_num - 2;i >= 0;i--) {
            if ((*(uint64_t *)key & (bit_flag << i)) > 0) {
                return (i+1);
            }
        }
        return 0;
    }
}

uint32_t persistent_hash(uint64_t key, uint32_t &table_index, int hash_method) {
    table_index = consistent_hash(key);
    size_t res;
    if (hash_method == 0) {
        res = int_hasher(key);
    }
    else {
        res = int_hasher(~key);
    }
    auto ret = (uint32_t)(res % SP_DRAM->bucket_num[table_index]);
    return ret;
}

uint32_t persistent_hash(char *key, uint32_t &table_index, int hash_method) {
    table_index = consistent_hash(key);
    size_t res;
    if (hash_method == 0) {
        res = str_hasher(key);
    }
    else {
        key[0]='$';
        res = str_hasher(key);
    }
    auto ret = (uint32_t)(res % SP_DRAM->bucket_num[table_index]);
    return ret;
}

uint32_t cache_hash(uint64_t key, int hash_method) {
    size_t res;
    if (hash_method == 0) {
        res = int_hasher(key);
    }
    else {
        res = int_hasher(~key);
    }
    auto ret = res % SP_DRAM->cbucket_num;
    return ret;
}

uint32_t cache_hash(char *key, int hash_method) {
    size_t res;
    if (hash_method == 0) {
        res = str_hasher(key);
    }
    else {
        key[0]='$';
        res = str_hasher(key);
    }
    auto ret = res % SP_DRAM->cbucket_num;
    return ret;
}

uint8_t fp_hash(uint64_t key) {
    auto ret = (uint8_t)(int_hasher(key) % FP_MAX_NUM);
    return ret;
}

uint8_t fp_hash(char *key) {
    auto ret = (uint8_t)(str_hasher(key) % FP_MAX_NUM);
    return ret;
}

bool fp_match(uint32_t *fp, int i, uint64_t key) {
    uint8_t fp_key = fp_hash(key);
    auto fp_stored = (*fp >> (FP_BIT_NUM * (SLOT_PER_BUCKET - i - 1))) & FP_MASK;
    return fp_key == (uint8_t)fp_stored;
}

bool fp_match(uint32_t *fp, int i, char *key) {
    uint8_t fp_key = fp_hash(key);
    auto fp_stored = (*fp >> (FP_BIT_NUM * (SLOT_PER_BUCKET - i - 1))) & FP_MASK;
    return fp_key == (uint8_t)fp_stored;
}

void fp_append(uint32_t *fp, int i, uint64_t key) {
    uint8_t fp_key = fp_hash(key);
    *fp &= ~(FP_MASK << (FP_BIT_NUM * (SLOT_PER_BUCKET - i - 1)));
    *fp |= (fp_key << (FP_BIT_NUM * (SLOT_PER_BUCKET - i - 1)));
}

void fp_append(uint32_t *fp, int i, char *key) {
    uint8_t fp_key = fp_hash(key);
    *fp &= ~(FP_MASK << (FP_BIT_NUM * (SLOT_PER_BUCKET - i - 1)));
    *fp |= (fp_key << (FP_BIT_NUM * (SLOT_PER_BUCKET - i - 1)));
}