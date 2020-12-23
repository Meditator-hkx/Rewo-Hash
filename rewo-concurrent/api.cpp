#include <iostream>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <thread>
#include <string.h>
#include <iostream>
#include <fstream>
#include "api.h"
#include "storage.h"
#include "trxn.h"
#include "rewolock.h"
#include "factory.h"

void rewo_init(int ntype) {
    //nvm init
    pm_init(ntype);
    rewo_recover();
#if DRAM_CACHE_ENABLE == 1
    cache_init();
    buffer_init();
    back_execute();
#endif
}

void rewo_exit() {
    ht_exit();
}

void rewo_clear() {
    // initiate SP from 0
    memset(SP, 0, TOTAL_SIZE);
    SP->load_factor = 0;
    SP->kv_num = 0;
    SP->ptable_num = 1;
    SP->bucket_num[0] = PBUCKET_NUM;
    SP->max_load_num = PBUCKET_NUM * SLOT_PER_BUCKET;
    SP->cbucket_num = CBUCKET_NUM;
    SP->resize_flag = 0;
    SP->resize_table_seq = 0;
    SP->resize_bucket_seq = 0;
    SP->cache_overflow_flag = 0;
    SP->magic = MAGIC;
    SP->table_offset[0] = SUPER_SIZE;
    SP->table_addr[0] = (uint64_t)(nvm_base_addr + SUPER_SIZE);
    memset((char *)SP->table_addr[0], 0, KV_SIZE);  // danger?

    flush_with_fence(SP);
    SP->magic = MAGIC - 1;
    flush_with_fence(&SP->magic);
    memcpy(SP_DRAM, SP, SUPER_SIZE);
}

void rewo_recover() {
    // check SP status
    // case 1: valid and normal start
    if (SP->magic == MAGIC) {
        // basically, should recover cache state if cache_overflow is 0
        // but there is no need actually
        // since data will be cached upon PM read hit
        SP->magic--;
        flush_with_fence(&SP->magic);
        memcpy(SP_DRAM, SP, SUPER_SIZE);
    }
    // case 2: valid but start from crash
    else if (SP->magic == MAGIC - 1) {
        // continue from resizing if necessary
        if (SP->resize_flag) {
            // redo_resize();
        }

        // loop in each valid table (zone)
        // clear all the locks and count how many key-value items exist
        SP->magic--;
        flush_with_fence(&SP->magic);
        memcpy(SP_DRAM, SP, SUPER_SIZE);
    }
    // case 3: cold start
    else {
        rewo_clear();
    }
}

void rewo_resize() {
    // 1. allocate space and update metadata
    int fd = open(MAP_PMEM, O_RDWR | O_CREAT, 0666);
    char *nvm_addr = (char *)((uint64_t)nvm_force_addr + SP->used_nvm_size);
    char *t_addr = (char *)mmap(nvm_addr, TOTAL_SIZE, PROT_READ | PROT_WRITE, \
            MAP_SHARED, fd, SP->used_nvm_size);
    SP->table_offset[SP->ptable_num] = SP->used_nvm_size;

    /* these two operations should be an atomic update */
    SP->ptable_num++;
    SP->resize_flag = 1;

    flush_with_fence(&SP->resize_flag);

    // 2. start rehashing
    int ret;
    for (int i = 0;i < SP->ptable_num - 1;i++) {
        auto *PBucket = (Bucket *)SP->table_addr[i];
        int bucket_num = SP->bucket_num[i];
        for (int j = 0;j < bucket_num;j++) {
            for (int k = 0;k < SLOT_PER_BUCKET;k++) {
                //
                if (!(PBucket[j].meta.bitmap & (BIT_FLAG >> k)) && consistent_hash(PBucket[j].kv[k].key) != i) {
                    // lock the slot
                    // migrate the key
                    if (lock(&PBucket[j].meta.lockmap, k)) {
                        ret = pm_insert(PBucket[j].kv[k].key, PBucket[j].kv[k].value);
                        if (ret == 0) {
                            // delete the key in current slot
                            invalidate(&PBucket[j].meta.bitmap, k);
                            flush_with_fence(&PBucket[j].meta);
                            unlock(&PBucket[j].meta.lockmap, k);
                        }
                    }
                    else {
                        // unexpected error in inserting a key to the new table
                        exit(-1);
                    }
                }
            }
            SP->resize_bucket_seq++;
            flush_with_fence(&SP->resize_bucket_seq);
        }
        SP->resize_table_seq++;
        flush_with_fence(&SP->resize_table_seq);
    }

    // 3. complete rehashing
    SP->resize_table_seq = 0;
    SP->resize_bucket_seq = 0;
    SP->resize_flag = 0;
}

int rewo_search(int order, uint64_t key, char *value) {
    int ret;
#if DRAM_CACHE_ENABLE == 1
    ret = cache_search(key, value);
    if (ret == 0) {
        return 0;
    }
#endif
    ret = pm_search(key, value);
    // add (key,value) to cache
    // cache_add(key, value);
    // cache replace might be required -> lru_replace(key, value);
    if (ret == 0) {
        // cache replace
#if DRAM_CACHE_ENABLE == 1
        cache_replace(key, value);
#endif
    }
    return ret;
}

int rewo_search(int order, char *key, char *value) {
    int ret;
#if DRAM_CACHE_ENABLE == 1
    ret = cache_search(key, value);
    if (ret == 0) {
        return 0;
    }
#endif
    ret = pm_search(key, value);
    // add (key,value) to cache
    // cache_add(key, value);
    // cache replace might be required -> lru_replace(key, value);
    if (ret == 0) {
        // cache replace
#if DRAM_CACHE_ENABLE == 1
        cache_replace(key, value);
#endif
    }
    return ret;
}

int rewo_insert(int order, uint64_t key, char *value) {
    int ret;
#if DRAM_CACHE_ENABLE == 1
    // sync key-value address to buffer
    // backend dram modification is executed in parallel
    buffer_tuple *sync_signal = sync_buffer(order, key, value, 1);
#endif
    ret = pm_insert(key, value);
#if DRAM_CACHE_ENABLE == 1
    sync_release(sync_signal);
#endif
    return ret;
}

int rewo_insert(int order, char *key, char *value) {
    int ret;
#if DRAM_CACHE_ENABLE == 1
    // sync key-value address to buffer
    // backend dram modification is executed in parallel
    buffer_tuple *sync_signal = sync_buffer(order, key, value, 1);
#endif
    ret = pm_insert(key, value);
#if DRAM_CACHE_ENABLE == 1
    sync_release(sync_signal);
#endif
    return ret;
}

int rewo_update(int order, uint64_t key, char *value) {
    int ret;
#if DRAM_CACHE_ENABLE == 1
    buffer_tuple *sync_signal = sync_buffer(order, key, value, 2);
#endif

    ret = pm_update(key, value);
#if DRAM_CACHE_ENABLE == 1
    sync_release(sync_signal);
#endif
    return ret;

}

int rewo_update(int order, char *key, char *value) {
    int ret;
#if DRAM_CACHE_ENABLE == 1
    buffer_tuple *sync_signal = sync_buffer(order, key, value, 2);
#endif

    ret = pm_update(key, value);
#if DRAM_CACHE_ENABLE == 1
    sync_release(sync_signal);
#endif
    return ret;

}

int rewo_delete(int order, uint64_t key) {
    int ret;
#if DRAM_CACHE_ENABLE == 1
    buffer_tuple *sync_signal = sync_buffer(order, key, nullptr, 3);
#endif

    ret = pm_delete(key);
#if DRAM_CACHE_ENABLE == 1
    sync_release(sync_signal);
#endif
    return ret;
}

int rewo_delete(int order, char *key) {
    int ret;
#if DRAM_CACHE_ENABLE == 1
    buffer_tuple *sync_signal = sync_buffer(order, key, nullptr, 3);
#endif

    ret = pm_delete(key);
#if DRAM_CACHE_ENABLE == 1
    sync_release(sync_signal);
#endif
    return ret;
}

int rewo_export() {
    // open file
    std::ofstream outfile;
    outfile.open(EXPORT_FILE);

    // scan entire persistent hash table
    for (int i = 0;i < ZONE_NUM;i++) {
        // for each data table
        auto *PBucket = (Bucket *)SP_DRAM->table_addr[i];
        for (int j = 0;j < SP_DRAM->bucket_num[i];i++) {
            // for each bucket
            for (int k = 0;k < SLOT_PER_BUCKET;k++) {
                // for each valid slot
                if (PBucket[j].meta.bitmap & (BIT_FLAG >> k)) {
                    // export the item to the file
#if KEY_TYPE == 1
                    outfile << PBucket[j].kv[k].key << ", " << PBucket[j].kv[k].value << "; " << std::endl;
#else
                    outfile << PBucket[j].kv[k].ckey << ", " << PBucket[j].kv[k].value << "; " << std::endl;
#endif
                }
            }
        }
    }

    // complete the exporting and close file
    outfile.close();
}

uint32_t rewo_get_kv_num() {
    return SP_DRAM->kv_num;
}

int pm_search(uint64_t key, char *value) {
    uint32_t bucketOff;
    uint32_t table_index;
    uint64_t bucket_meta_check;

PM_SEARCH:
    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = persistent_hash(key, table_index, h);
        auto *PBucket = (Bucket *)SP->table_addr[table_index];
        bucket_meta_check = *(uint64_t *)&PBucket[bucketOff].meta;
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bit state = 1
            if (PBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key fingerprint match
                if (fp_match(&PBucket[bucketOff].meta.fingerprints, i, key)) {
                    if (PBucket[bucketOff].kv[i].key == key) {
                        // return
                        strcpy(value, PBucket[bucketOff].kv[i].value);
                        if (bucket_meta_check != *(uint64_t *)&PBucket[bucketOff].meta) {
                            goto PM_SEARCH;
                        }
#if DRAM_CACHE_ENABLE == 1
                        // cache the kv item
                        cache_insert(key, value);
#endif
                        return 0;
                    }
                }
            }
        }
    }

    // negative search
    return -1;
}

int pm_search(char *key, char *value) {
    uint32_t bucketOff;
    uint32_t table_index;
    uint64_t bucket_meta_check;

PM_SEARCH:
    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = persistent_hash(key, table_index, h);
        auto *PBucket = (Bucket *)SP->table_addr[table_index];
        bucket_meta_check = *(uint64_t *)&PBucket[bucketOff].meta;
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bit state = 1
            if (PBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key fingerprint match
                if (fp_match(&PBucket[bucketOff].meta.fingerprints, i, key)) {
                    if (strcmp(PBucket[bucketOff].kv[i].ckey, key) == 0) {
                        // return
                        strcpy(value, PBucket[bucketOff].kv[i].value);
                        if (bucket_meta_check != *(uint64_t *)&PBucket[bucketOff].meta) {
                            goto PM_SEARCH;
                        }
#if DRAM_CACHE_ENABLE == 1
                        // cache the kv item
                        cache_insert(key, value);
#endif
                        return 0;
                    }
                }
            }
        }
    }

    // negative search
    return -1;
}

// NOTICE: insert does not need to update the bucket version since it only appends to a free slot
int pm_insert(uint64_t key, char *value) {
    uint32_t bucketOff;
    uint32_t table_index;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = persistent_hash(key, table_index, h);
        auto *PBucket = (Bucket *)SP->table_addr[table_index];
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // 1. find a free slot
            if (PBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                continue;
            }
            // 2. lock the slot and proceed only if it succeeds
            if (lock(&PBucket[bucketOff].meta.lockmap, i)) {
                // 3. write in that slot
                // NOTICE: using ntstore
                ntstore((char *)&PBucket[bucketOff].kv[i].key, (char *)&key, KEY_SIZE);
                ntstore((char *)PBucket[bucketOff].kv[i].value, value, (uint32_t)strlen(value)+1);
                fence();

                // PBucket[bucketOff1].kv[i].key = key;
                // strcpy(PBucket[bucketOff1].kv[i].value, value);
                // flush_with_fence(&PBucket[bucketOff1].kv[i]);

                // 4. commit the write operation
                // 4.1 append the fingerprint
                fp_append(&PBucket[bucketOff].meta.fingerprints, i, key);
                // 4.2 atomic metadata update
                commit(&PBucket[bucketOff].meta.bitmap, i);
                // 4.3 persist the metadata -> durable transaction
                flush_with_fence(&PBucket[bucketOff].meta);


                // 5. unlock the slot
                // NOTICE: it can be volatile and will be reset during crash recovery
                unlock(&PBucket[bucketOff].meta.lockmap, i);

                // 6. update system metadata
                SP_DRAM->kv_num++;
                SP_DRAM->load_factor = (float) SP_DRAM->kv_num / SP_DRAM->max_load_num;
                return 0;
            }
        }
    }


    // if reach here, no free slot exists in both candidates
    // execute evict for one slot in two candidate blocks
    
    /*------------- Evict Procedure -------------*/
    // try to evict in candidate bucket 1
    Bucket *cBucket;
    uint32_t eBucketOff;
    uint64_t cKey;
    char *cValue;
    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = persistent_hash(key, table_index, h);
        auto *PBucket = (Bucket *)SP->table_addr[table_index];
        cBucket = &PBucket[bucketOff];
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            cKey = cBucket->kv[i].key;
            cValue = cBucket->kv[i].value;

            // compute candidate evict bucket for candidate evict key
            eBucketOff = persistent_hash(cKey, table_index, 1-h);

            if (!lock(&cBucket->meta.lockmap, i)) {
                continue;
            }

            // 1. search free slot in candidate evict bucket
            for (int j = 0;j < SLOT_PER_BUCKET;j++) {
                if (PBucket[eBucketOff].meta.bitmap & (BIT_FLAG >> j)) {
                    continue;
                }
                // 2. lock the slot and proceed only if it succeeds
                if (lock(&PBucket[eBucketOff].meta.lockmap, j)) {
                    // persist the lock state for double record check during crash recovery
                    flush_with_fence(&cBucket->meta);

                    // 3. migrate data
                    ntstore((char *)&PBucket[eBucketOff].kv[j].key, (char *)&cKey, KEY_SIZE);
                    ntstore((char *)PBucket[eBucketOff].kv[j].value, cValue, VALUE_SIZE);
                    fence();

                    // 4. commit and persist the INSERT in eBucket
                    fp_append(&PBucket[eBucketOff].meta.fingerprints, j, cKey);
                    commit(&PBucket[eBucketOff].meta.bitmap, j);
                    flush_with_fence(&PBucket[eBucketOff].meta);

                    // 5. unlock the current slot in eBucket
                    unlock(&PBucket[eBucketOff].meta.lockmap, j);

                    // 6. delete the original item in cBucket
                    invalidate(&cBucket->meta.bitmap, i);
                    flush_with_fence(&cBucket->meta);

                    // 7. write new data into the freed slot
                    ntstore((char *)&cBucket->kv[j].key, (char *)&key, KEY_SIZE);
                    ntstore((char *)cBucket->kv[j].value, value, VALUE_SIZE);
                    fence();

                    // 8. commit and persist the INSERT in cBucket
                    fp_append(&cBucket->meta.fingerprints, i, key);
                    commit(&cBucket->meta.bitmap, i);
                    flush_with_fence(&cBucket->meta);

                    // 9. unlock current slot in cBucket
                    unlock(&cBucket->meta.lockmap, i);

                    // 10. update system metadata
                    SP_DRAM->kv_num++;
                    SP_DRAM->load_factor = (float) SP_DRAM->kv_num / SP_DRAM->max_load_num;
                    return 0;
                }
            }
            unlock(&cBucket->meta.lockmap, i);
        }
    }

    // need to resize
    rewo_resize();
    return -1;
}

// NOTICE: insert does not need to update the bucket version since it only appends to a free slot
int pm_insert(char *key, char *value) {
    uint32_t bucketOff1, bucketOff2, bucketOff;
    uint32_t table_index;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = persistent_hash(key, table_index, h);
        auto *PBucket = (Bucket *)SP->table_addr[table_index];
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // 1. find a free slot
            if (PBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                continue;
            }
            // 2. lock the slot and proceed only if it succeeds
            if (lock(&PBucket[bucketOff].meta.lockmap, i)) {
                // 3. write in that slot
                // NOTICE: using ntstore
                ntstore((char *)PBucket[bucketOff].kv[i].ckey, key, KEY_SIZE);
                ntstore((char *)PBucket[bucketOff].kv[i].value, value, (uint32_t)strlen(value)+1);
                fence();

                // PBucket[bucketOff1].kv[i].key = key;
                // strcpy(PBucket[bucketOff1].kv[i].value, value);
                // flush_with_fence(&PBucket[bucketOff1].kv[i]);

                // 4. commit the write operation
                // 4.1 append the fingerprint
                fp_append(&PBucket[bucketOff].meta.fingerprints, i, key);
                // 4.2 atomic metadata update
                commit(&PBucket[bucketOff].meta.bitmap, i);
                // 4.3 persist the metadata -> durable transaction
                flush_with_fence(&PBucket[bucketOff].meta);

                // 5. unlock the slot
                // NOTICE: it can be volatile and will be reset during crash recovery
                unlock(&PBucket[bucketOff].meta.lockmap, i);

                // 6. update system metadata
                SP_DRAM->kv_num++;
                SP_DRAM->load_factor = (float) SP_DRAM->kv_num / SP_DRAM->max_load_num;
                return 0;
            }
        }
    }


    // if reach here, no free slot exists in both candidates
    // execute evict for one slot in two candidate blocks

    /*------------- Evict Procedure -------------*/
    // try to evict in candidate bucket 1
    Bucket *cBucket;
    uint32_t eBucketOff;
    char *cKey;
    char *cValue;
    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = persistent_hash(key, table_index, h);
        auto *PBucket = (Bucket *)SP->table_addr[table_index];
        cBucket = &PBucket[bucketOff];
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            cKey = cBucket->kv[i].ckey;
            cValue = cBucket->kv[i].value;

            // compute candidate evict bucket for candidate evict key
            eBucketOff = persistent_hash(cKey, table_index, 1-h);

            if (!lock(&cBucket->meta.lockmap, i)) {
                continue;
            }

            // 1. search free slot in candidate evict bucket
            for (int j = 0;j < SLOT_PER_BUCKET;j++) {
                if (PBucket[eBucketOff].meta.bitmap & (BIT_FLAG >> j)) {
                    continue;
                }
                // 2. lock the slot and proceed only if it succeeds
                if (lock(&PBucket[eBucketOff].meta.lockmap, j)) {
                    // persist the lock state for double record check during crash recovery
                    flush_with_fence(&cBucket->meta);

                    // 3. migrate data
                    ntstore((char *)PBucket[eBucketOff].kv[j].ckey, cKey, KEY_SIZE);
                    ntstore((char *)PBucket[eBucketOff].kv[j].value, cValue, VALUE_SIZE);
                    fence();

                    // 4. commit and persist the INSERT in eBucket
                    fp_append(&PBucket[eBucketOff].meta.fingerprints, j, cKey);
                    commit(&PBucket[eBucketOff].meta.bitmap, j);
                    flush_with_fence(&PBucket[eBucketOff].meta);

                    // 5. unlock the current slot in eBucket
                    unlock(&PBucket[eBucketOff].meta.lockmap, j);

                    // 6. delete the original item in cBucket
                    invalidate(&cBucket->meta.bitmap, i);
                    flush_with_fence(&cBucket->meta);

                    // 7. write new data into the freed slot
                    ntstore((char *)cBucket->kv[j].ckey, key, KEY_SIZE);
                    ntstore((char *)cBucket->kv[j].value, value, VALUE_SIZE);
                    fence();

                    // 8. commit and persist the INSERT in cBucket
                    fp_append(&cBucket->meta.fingerprints, i, key);
                    commit(&cBucket->meta.bitmap, i);
                    flush_with_fence(&cBucket->meta);

                    // 9. unlock current slot in cBucket
                    unlock(&cBucket->meta.lockmap, i);

                    // 10. update system metadata
                    SP_DRAM->kv_num++;
                    SP_DRAM->load_factor = (float) SP_DRAM->kv_num / SP_DRAM->max_load_num;
                    return 0;
                }
            }
            unlock(&cBucket->meta.lockmap, i);
        }
    }

    // need to resize
    rewo_resize();
    return -1;
}

int pm_update(uint64_t key, char *value) {
    uint32_t bucketOff;
    int oldSlot;
    uint32_t table_index;
    auto find_flag = false;

    //  use hashFunc 1
    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = persistent_hash(key, table_index, h);
        auto *PBucket = (Bucket *)SP->table_addr[table_index];
        int old_off = -1;

        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bit state = 1
            if (PBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key fingerprint match
                if (fp_match(&PBucket[bucketOff].meta.fingerprints, i, key)) {
                    if (PBucket[bucketOff].kv[i].key == key) {
                        if (!lock(&PBucket[bucketOff].meta.lockmap, i)) {
                            return -1;
                        }
                        find_flag = true;
                        old_off = i;
                        for (int j = 0;j < SLOT_PER_BUCKET;j++) {
                            if (PBucket[bucketOff].meta.bitmap & (BIT_FLAG >> j)) {
                                continue;
                            }
                            else if (lock(&PBucket[bucketOff].meta.lockmap, j)) {
                                // write into this slot
                                ntstore((char *)&PBucket[bucketOff].kv[j].key, (char *)&key, KEY_SIZE);
                                ntstore(PBucket[bucketOff].kv[j].value, value, VALUE_SIZE);
                                fence();

                                // commit and persist metadata
                                fp_append(&PBucket[bucketOff].meta.fingerprints, j, key);
                                commit(&PBucket[bucketOff].meta.bitmap, i, j);
                                flush_with_fence(&PBucket[bucketOff].meta);

                                // update bucket version
                                PBucket[bucketOff].meta.version++;

                                // unlock two slots (locks can be volatile)
                                unlock(&PBucket[bucketOff].meta.lockmap, i, j);
                                return 0;
                            }
                        }
                    }
                }
            }
        }

        // no free slot within the same bucket is found
        if (find_flag) {
            // execute evict for one slot in the target bucket
            /*------------- Evict Procedure -------------*/
            uint32_t eBucketOff;
            uint64_t cKey;
            char *cValue;
            for (int i = 0;i < SLOT_PER_BUCKET;i++) {
                if (i == old_off || !lock(&PBucket[bucketOff].meta.lockmap, i)) {
                    continue;
                }
                cKey = PBucket[bucketOff].kv[i].key;
                cValue = PBucket[bucketOff].kv[i].value;

                // compute candidate evict bucket for candidate evict key
                eBucketOff = persistent_hash(cKey, table_index, 1-h);

                // 1. search free slot in candidate evict bucket
                for (int j = 0;j < SLOT_PER_BUCKET;j++) {
                    if (PBucket[eBucketOff].meta.bitmap & (BIT_FLAG >> j)) {
                        continue;
                    }
                    // 2. lock the slot if it is free and holding no lock
                    if (lock(&PBucket[eBucketOff].meta.lockmap, j)) {
                        // 3. persist the lock state of original data for double record check during crash recovery
                        flush_with_fence(&PBucket[bucketOff].meta);

                        // 4. migrate data
                        ntstore((char *)&PBucket[eBucketOff].kv[j].key, (char *)&cKey, KEY_SIZE);
                        ntstore((char *)PBucket[eBucketOff].kv[j].value, cValue, VALUE_SIZE);
                        fence();

                        // 5. commit, persist the INSERT in eBucket and unlock this slot
                        fp_append(&PBucket[eBucketOff].meta.fingerprints, j, cKey);
                        commit(&PBucket[eBucketOff].meta.bitmap, j);
                        flush_with_fence(&PBucket[eBucketOff].meta);
                        unlock(&PBucket[eBucketOff].meta.lockmap, j);

                        // 6. delete the original item in target bucket
                        // update bucket version
                        PBucket[bucketOff].meta.version++;
                        invalidate(&PBucket[bucketOff].meta.bitmap, i);
                        flush_with_fence(&PBucket[bucketOff].meta);

                        // 7. write new data into the freed slot
                        ntstore((char *)&PBucket[bucketOff].kv[i].key, (char *)&key, KEY_SIZE);
                        ntstore((char *)PBucket[bucketOff].kv[i].value, value, VALUE_SIZE);
                        fence();

                        // 8. commit and persist the INSERT in target bucket
                        fp_append(&PBucket[bucketOff].meta.fingerprints, i, key);
                        commit(&PBucket[bucketOff].meta.bitmap, i);
                        flush_with_fence(&PBucket[bucketOff].meta);

                        // 9. unlock the newly-written slot and the evicted slot
                        unlock(&PBucket[bucketOff].meta.lockmap, old_off, i);

                        return 0;
                    }
                }
                unlock(&PBucket[bucketOff].meta.lockmap, i);
            }
            unlock(&PBucket[bucketOff].meta.lockmap, old_off);
            // need to resize and retry
            rewo_resize();
            break;
        }
    }

    // no such key exists and should return pm_insert(key, value);
    if (find_flag) {
        return pm_insert(key, value);
    }

    // currently, we just return false and require the client to post the request again later
    return -1;
}

int pm_update(char *key, char *value) {
    uint32_t bucketOff;
    int oldSlot;
    uint32_t table_index;
    auto find_flag = false;

    //  use hashFunc 1
    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = persistent_hash(key, table_index, h);
        auto *PBucket = (Bucket *)SP->table_addr[table_index];
        int old_off = -1;
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bit state = 1
            if (PBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key fingerprint match
                if (fp_match(&PBucket[bucketOff].meta.fingerprints, i, key)) {
                    if (strcmp(PBucket[bucketOff].kv[i].ckey, key) == 0) {
                        if (!lock(&PBucket[bucketOff].meta.lockmap, i)) {
                            return -1;
                        }
                        find_flag = true;
                        old_off = i;
                        for (int j = 0;j < SLOT_PER_BUCKET;j++) {
                            if (PBucket[bucketOff].meta.bitmap & (BIT_FLAG >> j)) {
                                continue;
                            }
                            else if (lock(&PBucket[bucketOff].meta.lockmap, j)) {
                                // write into this slot
                                ntstore((char *)PBucket[bucketOff].kv[j].ckey, key, KEY_SIZE);
                                ntstore(PBucket[bucketOff].kv[j].value, value, VALUE_SIZE);
                                fence();

                                // commit and persist metadata
                                fp_append(&PBucket[bucketOff].meta.fingerprints, j, key);
                                commit(&PBucket[bucketOff].meta.bitmap, i, j);
                                flush_with_fence(&PBucket[bucketOff].meta);

                                // update bucket version
                                PBucket[bucketOff].meta.version++;

                                // unlock two slots (locks can be volatile)
                                unlock(&PBucket[bucketOff].meta.lockmap, i, j);
                                return 0;
                            }
                        }
                    }
                }
            }
        }

        // no free slot within the same bucket is found
        if (find_flag) {
            // execute evict for one slot in the target bucket
            /*------------- Evict Procedure -------------*/
            uint32_t eBucketOff;
            char *cKey;
            char *cValue;
            for (int i = 0;i < SLOT_PER_BUCKET;i++) {
                if (i == old_off || !lock(&PBucket[bucketOff].meta.lockmap, i)) {
                    continue;
                }
                cKey = PBucket[bucketOff].kv[i].ckey;
                cValue = PBucket[bucketOff].kv[i].value;

                // compute candidate evict bucket for candidate evict key
                eBucketOff = persistent_hash(cKey, table_index, 1-h);

                // 1. search free slot in candidate evict bucket
                for (int j = 0;j < SLOT_PER_BUCKET;j++) {
                    if (PBucket[eBucketOff].meta.bitmap & (BIT_FLAG >> j)) {
                        continue;
                    }
                    // 2. lock the slot if it is free and holding no lock
                    if (lock(&PBucket[eBucketOff].meta.lockmap, j)) {
                        // 3. persist the lock state of original data for double record check during crash recovery
                        flush_with_fence(&PBucket[bucketOff].meta);

                        // 4. migrate data
                        ntstore((char *)PBucket[eBucketOff].kv[j].ckey, cKey, KEY_SIZE);
                        ntstore((char *)PBucket[eBucketOff].kv[j].value, cValue, VALUE_SIZE);
                        fence();

                        // 5. commit, persist the INSERT in eBucket and unlock this slot
                        fp_append(&PBucket[eBucketOff].meta.fingerprints, j, cKey);
                        commit(&PBucket[eBucketOff].meta.bitmap, j);
                        flush_with_fence(&PBucket[eBucketOff].meta);
                        unlock(&PBucket[eBucketOff].meta.lockmap, j);

                        // 6. delete the original item in target bucket
                        // update bucket version
                        PBucket[bucketOff].meta.version++;
                        invalidate(&PBucket[bucketOff].meta.bitmap, i);
                        flush_with_fence(&PBucket[bucketOff].meta);

                        // 7. write new data into the freed slot
                        ntstore((char *)PBucket[bucketOff].kv[i].ckey, key, KEY_SIZE);
                        ntstore((char *)PBucket[bucketOff].kv[i].value, value, VALUE_SIZE);
                        fence();

                        // 8. commit and persist the INSERT in target bucket
                        fp_append(&PBucket[bucketOff].meta.fingerprints, i, key);
                        commit(&PBucket[bucketOff].meta.bitmap, i);
                        flush_with_fence(&PBucket[bucketOff].meta);

                        // 9. unlock the newly-written slot and the evicted slot
                        unlock(&PBucket[bucketOff].meta.lockmap, old_off, i);

                        return 0;
                    }
                }
                unlock(&PBucket[bucketOff].meta.lockmap, i);
            }
            unlock(&PBucket[bucketOff].meta.lockmap, old_off);
            // need to resize and retry
            rewo_resize();
            break;
        }
    }

    // no such key exists and should return pm_insert(key, value);
    if (find_flag) {
        return pm_insert(key, value);
    }

    // currently, we just return false and require the client to post the request again later
    return -1;
}

int pm_delete(uint64_t key) {
    uint32_t bucketOff;
    uint32_t table_index;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = persistent_hash(key, table_index, h);
        auto *PBucket = (Bucket *)SP->table_addr[table_index];

        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bit state = 1
            if (PBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key fingerprint match
                if (fp_match(&PBucket[bucketOff].meta.fingerprints, i, key)) {
                    if (PBucket[bucketOff].kv[i].key == key) {
                        // the lock is used for safety
                        // can be cancelled for some use
                        if (lock(&PBucket[bucketOff].meta.lockmap, i)) {
                            invalidate(&PBucket[bucketOff].meta.bitmap, i);
                            flush_with_fence(&PBucket[bucketOff].meta);
                            // update bucket version
                            PBucket[bucketOff].meta.version++;
                            unlock(&PBucket[bucketOff].meta.lockmap, i);

                            // update system metadata
                            SP_DRAM->kv_num--;
                            SP_DRAM->load_factor = (float) SP_DRAM->kv_num / SP_DRAM->max_load_num;
                            return 0;
                        }
                        else {
                            return -1;
                        }
                    }
                }
            }
        }
    }

    // data not found
    return -1;
}

int pm_delete(char *key) {
    uint32_t bucketOff;
    uint32_t table_index;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = persistent_hash(key, table_index, h);
        auto *PBucket = (Bucket *)SP->table_addr[table_index];
        Pm_Delete:
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bit state = 1
            if (PBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key fingerprint match
                if (fp_match(&PBucket[bucketOff].meta.fingerprints, i, key)) {
                    if (strcmp(PBucket[bucketOff].kv[i].ckey, key) == 0) {
                        // the lock is used for safety
                        // can be cancelled for some use
                        if (lock(&PBucket[bucketOff].meta.lockmap, i)) {
                            invalidate(&PBucket[bucketOff].meta.bitmap, i);
                            // update bucket version
                            PBucket[bucketOff].meta.version++;
                            flush_with_fence(&PBucket[bucketOff].meta);
                            unlock(&PBucket[bucketOff].meta.lockmap, i);

                            // update system metadata
                            SP_DRAM->kv_num--;
                            SP_DRAM->load_factor = (float) SP_DRAM->kv_num / SP_DRAM->max_load_num;
                            return 0;
                        }
                        else {
                            return -1;
                        }
                    }
                }
            }
        }
    }

    // data not found
    return -1;
}

/*
 * functions handle cached hash table
 */
int cache_search(uint64_t key, char *value) {
    uint32_t bucketOff;
    uint64_t bucket_meta_check;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = cache_hash(key, h);
        bucket_meta_check = *(uint64_t *)&CBucket[bucketOff].meta;
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bitmap state = 1
            if (CBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key match
                if (CBucket[bucketOff].kv[i].key == key) {
                    strcpy(value, CBucket[bucketOff].kv[i].value);
                    if (bucket_meta_check == *(uint64_t *)&CBucket[bucketOff].meta) {
                        return 0;
                    }
                    else {
                        // check from persistent table
                        return -1;
                    }
                }
            }
        }
    }
    
    return -1;
}

int cache_search(char *key, char *value) {
    uint32_t bucketOff;
    uint64_t bucket_meta_check;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = cache_hash(key, h);
        bucket_meta_check = *(uint64_t *)&CBucket[bucketOff].meta;
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bitmap state = 1
            if (CBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key match
                if (strcmp(CBucket[bucketOff].kv[i].ckey, key) == 0) {
                    strcpy(value, CBucket[bucketOff].kv[i].value);
                    if (bucket_meta_check == *(uint64_t *)&CBucket[bucketOff].meta) {
                        return 0;
                    }
                    else {
                        // check from persistent table
                        return -1;
                    }
                }
            }
        }
    }

    return -1;
}

int cache_insert(uint64_t key, char *value) {
    uint32_t bucketOff;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = cache_hash(key, h);
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // 1. find a free slot
            if (CBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                continue;
            }
            else {
                // 2. try to lock this slot
                if (lock(&CBucket[bucketOff].meta.lockmap, i)) {
                    // 3. write in the slot
                    CBucket[bucketOff].kv[i].key = key;
                    strcpy(CBucket[bucketOff].kv[i].value, value);

                    // 4. commit the write operation
                    commit(&CBucket[bucketOff].meta.bitmap, i);

                    // 5. update the bucket lru information
                    lru_update(&CBucket[bucketOff].meta.fingerprints, i);

                    // 6. update the bucket version
                    CBucket[bucketOff].meta.version++;

                    // 7. unlock this slot
                    unlock(&CBucket[bucketOff].meta.lockmap, i);
                    return i;
                }
            }
        }
    }

    // fail to insert, but okay
    // future reads on this key can directly probe in NVM buckets
    return -1;
}

int cache_insert(char *key, char *value) {
    uint32_t bucketOff;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = cache_hash(key, h);
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // 1. find a free slot
            if (CBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                continue;
            }
            else {
                // 2. try to lock this slot
                if (lock(&CBucket[bucketOff].meta.lockmap, i)) {
                    // 3. write in the slot
                    strcpy(CBucket[bucketOff].kv[i].ckey, key);
                    strcpy(CBucket[bucketOff].kv[i].value, value);

                    // 4. commit the write operation
                    commit(&CBucket[bucketOff].meta.bitmap, i);

                    // 5. update the bucket lru information
                    lru_update(&CBucket[bucketOff].meta.fingerprints, i);

                    // 6. update the bucket version
                    CBucket[bucketOff].meta.version++;

                    // 7. unlock this slot
                    unlock(&CBucket[bucketOff].meta.lockmap, i);
                    return i;
                }
            }
        }
    }

    // fail to insert, but okay
    // future reads on this key can directly probe in NVM buckets
    return -1;
}

int cache_insert(uint64_t key, char *value, uint64_t &lockmap_addr) {
    uint32_t bucketOff;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = cache_hash(key, h);
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // 1. find a free slot
            if (CBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                continue;
            }
            else {
                // 2. try to lock this slot
                if (lock(&CBucket[bucketOff].meta.lockmap, i)) {
                    // 3. write in the slot
                    CBucket[bucketOff].kv[i].key = key;
                    strcpy(CBucket[bucketOff].kv[i].value, value);

                    // 4. commit the write operation
                    commit(&CBucket[bucketOff].meta.bitmap, i);

                    // 5. update the bucket lru information
                    lru_update(&CBucket[bucketOff].meta.fingerprints, i);


                    // 6. unlock this slot
                    // NOTICE: may need to wait for the NVM bucket's persistence notification
                    // unlock(&CBucket[bucketOff].meta.lockmap, i);
                    lockmap_addr = (uint64_t)&CBucket[bucketOff].meta.lockmap;
                    return i;
                }
            }
        }
    }

    // fail to insert, but okay
    // future reads on this key can directly probe in NVM buckets
    return -1;
}

int cache_insert(char *key, char *value, uint64_t &lockmap_addr) {
    uint32_t bucketOff;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = cache_hash(key, h);
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // 1. find a free slot
            if (CBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                continue;
            }
            else {
                // 2. try to lock this slot
                if (lock(&CBucket[bucketOff].meta.lockmap, i)) {
                    // 3. write in the slot
                    strcpy(CBucket[bucketOff].kv[i].ckey, key);
                    strcpy(CBucket[bucketOff].kv[i].value, value);

                    // 4. commit the write operation
                    commit(&CBucket[bucketOff].meta.bitmap, i);

                    // 5. update the bucket lru information
                    lru_update(&CBucket[bucketOff].meta.fingerprints, i);


                    // 6. unlock this slot
                    // NOTICE: may need to wait for the NVM bucket's persistence notification
                    // unlock(&CBucket[bucketOff].meta.lockmap, i);
                    lockmap_addr = (uint64_t)&CBucket[bucketOff].meta.lockmap;
                    return i;
                }
            }
        }
    }

    // fail to insert, but okay
    // future reads on this key can directly probe in NVM buckets
    return -1;
}

int cache_update(uint64_t key, char *value, uint64_t &lockmap_addr) {
    uint32_t bucketOff;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = cache_hash(key, h);
Cache_Update_1:
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bitmap state = 1
            if (CBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key match
                if (CBucket[bucketOff].kv[i].key == key) {
                    // try to lock current slot
                    if (lock(&CBucket[bucketOff].meta.lockmap, i)) {
                        // update value (in-place update)
                        strcpy(CBucket[bucketOff].kv[i].value, value);

                        // update the bucket lru information
                        lru_update(&CBucket[bucketOff].meta.fingerprints, i);

                        // update the bucket version
                        CBucket[bucketOff].meta.version++;

                        // unlock current slot
                        // unlock(&CBucket[bucketOff].meta.lockmap, i);
                        lockmap_addr = (uint64_t)&CBucket[bucketOff].meta.lockmap;
                        return i;
                    }
                    else {
                        goto Cache_Update_1;
                    }
                }
            }
        }
    }

    // fail to update, but does not impact correctness
    // should return cache_insert(key, value);
    return -1;
}

int cache_update(char *key, char *value, uint64_t &lockmap_addr) {
    uint32_t bucketOff;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = cache_hash(key, h);
Cache_Update_2:
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bitmap state = 1
            if (CBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key match
                if (strcmp(CBucket[bucketOff].kv[i].ckey, key) == 0) {
                    // try to lock current slot
                    if (lock(&CBucket[bucketOff].meta.lockmap, i)) {
                        // update value (in-place update)
                        strcpy(CBucket[bucketOff].kv[i].value, value);

                        // update the bucket lru information
                        lru_update(&CBucket[bucketOff].meta.fingerprints, i);

                        // update the bucket version
                        CBucket[bucketOff].meta.version++;

                        // unlock current slot
                        // unlock(&CBucket[bucketOff].meta.lockmap, i);
                        lockmap_addr = (uint64_t)&CBucket[bucketOff].meta.lockmap;
                        return i;
                    }
                    else {
                        goto Cache_Update_2;
                    }
                }
            }
        }
    }

    // fail to update, but does not impact correctness
    // should return cache_insert(key, value);
    return -1;
}

int cache_delete(uint64_t key, uint64_t &lockmap_addr) {
    uint32_t bucketOff;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = cache_hash(key, h);

        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bitmap state = 1
            if (CBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key match
                if (CBucket[bucketOff].kv[i].key == key) {
                    // try to lock current slot
                    if (lock(&CBucket[bucketOff].meta.lockmap, i)) {
                        // delete the item
                        invalidate(&CBucket[bucketOff].meta.bitmap, i);
                        flush_with_fence(&CBucket[bucketOff].meta);

                        // update the bucket version
                        CBucket[bucketOff].meta.version++;

                        // unlock current slot
                        // unlock(&CBucket[bucketOff].meta.lockmap, i);
                        lockmap_addr = (uint64_t)&CBucket[bucketOff].meta.lockmap;
                        return i;
                    }
                    else {
                        return -1;
                    }
                }
            }
        }

    }

    // data not found
    return -1;
}

int cache_delete(char *key, uint64_t &lockmap_addr) {
    uint32_t bucketOff;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = cache_hash(key, h);
        Cache_Delete:
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bitmap state = 1
            if (CBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key match
                if (strcmp(CBucket[bucketOff].kv[i].ckey, key) == 0) {
                    // try to lock current slot
                    if (lock(&CBucket[bucketOff].meta.lockmap, i)) {
                        // delete the item
                        invalidate(&CBucket[bucketOff].meta.bitmap, i);
                        flush_with_fence(&CBucket[bucketOff].meta);

                        // update the bucket version
                        CBucket[bucketOff].meta.version++;

                        // unlock current slot
                        // unlock(&CBucket[bucketOff].meta.lockmap, i);
                        lockmap_addr = (uint64_t)&CBucket[bucketOff].meta.lockmap;
                        return i;
                    }
                    else {
                        return -1;
                    }
                }
            }
        }

    }

    // data not found
    return -1;
}

void cache_replace(uint64_t key, char *value) {
    int ret = cache_insert(key, value);
    if (ret == 0)
        return;

    uint32_t bucketOff = cache_hash(key, 0);

    // choose the least-recently-accessed element to be evicted
    auto k = lru_evict(&CBucket[bucketOff].meta.lru_sorteds);
    if (lock(&CBucket[bucketOff].meta.lockmap, k)) {
        CBucket[bucketOff].kv[k].key = key;
        strcpy(CBucket[bucketOff].kv[k].value, value);
        // update bucket lru information
        lru_update(&CBucket[bucketOff].meta.lru_sorteds);
        // update the bucket version
        CBucket[bucketOff].meta.version++;
        unlock(&CBucket[bucketOff].meta.lockmap, k);
    }
    else {
        return;
    }
}

void cache_replace(char *key, char *value) {
    int ret = cache_insert(key, value);
    if (ret == 0)
        return;

    uint32_t bucketOff = cache_hash(key, 0);
    // choose the least-recently-accessed element to be evicted
    auto k = lru_evict(&CBucket[bucketOff].meta.lru_sorteds);
    if (lock(&CBucket[bucketOff].meta.lockmap, k)) {
        strcpy(CBucket[bucketOff].kv[k].ckey, key);
        strcpy(CBucket[bucketOff].kv[k].value, value);
        // update bucket lru information
        lru_update(&CBucket[bucketOff].meta.lru_sorteds);
        // update the bucket version
        CBucket[bucketOff].meta.version++;
        unlock(&CBucket[bucketOff].meta.lockmap, k);
    }
    else {
        return;
    }
}

void back_execute() {
    // start multiple threads
    for (int i = 0;i < BACKEND_THREAD_NUM;i++) {
        std::thread back_thread(thread_execute, i);
        back_thread.detach();
    }
}

void thread_execute(int order) {
    int ret;
    uint64_t lockmap_addr;
    // keep its logic easy to understand as prolegomenon
    if (FRONTEND_THREAD_NUM == BACKEND_THREAD_NUM) {
        while(true) {
            switch(TupleSet[order].sync_flag) {
            case 1:
                ret = cache_insert(TupleSet[order].key, TupleSet[order].v_addr, lockmap_addr);
                TupleSet[order].sync_flag = 4;
                TupleSet[order].lock_info.off = (uint8_t)ret;
                TupleSet[order].lock_info.lockmap_addr = lockmap_addr;
                break;
            case 2:
                cache_update(TupleSet[order].key, TupleSet[order].v_addr, lockmap_addr);
                TupleSet[order].sync_flag = 4;
                TupleSet[order].lock_info.off = (uint8_t)ret;
                TupleSet[order].lock_info.lockmap_addr = lockmap_addr;
                break;
            case 3:
                cache_delete(TupleSet[order].key, lockmap_addr);
                TupleSet[order].sync_flag = 4;
                TupleSet[order].lock_info.off = (uint8_t)ret;
                TupleSet[order].lock_info.lockmap_addr = lockmap_addr;
                break;
            default:
                break;
            }
        }
    }
    else {
        int front_per_back = FRONTEND_THREAD_NUM / BACKEND_THREAD_NUM;
        while (1) {
            for (int i = order * front_per_back; i < FRONTEND_THREAD_NUM && i < order * (front_per_back + 1); i++) {
                switch (TupleSet[i].sync_flag) {
                    case 1:
                        cache_insert(TupleSet[i].key, TupleSet[i].v_addr, lockmap_addr);
                        TupleSet[i].sync_flag = 4;
                        TupleSet[order].lock_info.off = (uint8_t)ret;
                        TupleSet[order].lock_info.lockmap_addr = lockmap_addr;
                        break;
                    case 2:
                        cache_update(TupleSet[i].key, TupleSet[i].v_addr, lockmap_addr);
                        TupleSet[i].sync_flag = 4;
                        TupleSet[order].lock_info.off = (uint8_t)ret;
                        TupleSet[order].lock_info.lockmap_addr = lockmap_addr;
                        break;
                    case 3:
                        cache_delete(TupleSet[i].key, lockmap_addr);
                        TupleSet[i].sync_flag = 4;
                        TupleSet[order].lock_info.off = (uint8_t)ret;
                        TupleSet[order].lock_info.lockmap_addr = lockmap_addr;
                        break;
                    default:
                        break;
                }
            }
        }
    }
}

buffer_tuple *sync_buffer(int order, uint64_t key, char *value, int flag) {
    TupleSet[order].key = key;
    TupleSet[order].v_addr = value;
    TupleSet[order].sync_flag = flag;
    return &TupleSet[order];
}

buffer_tuple *sync_buffer(int order, char *key, char *value, int flag) {
    TupleSet[order].ckey = key;
    TupleSet[order].v_addr = value;
    TupleSet[order].sync_flag = flag;
    return &TupleSet[order];
}

int sync_release(buffer_tuple *sync_tuple) {
    int max_count = 1000;
    uint8_t *lockmap = (uint8_t *)sync_tuple->lock_info.lockmap_addr;
    while (sync_tuple->sync_flag != 4) {
//        max_count--;
//        if (max_count == 0) {
//            std::cout << "sync error: backend dram modification failed!" << std::endl;
//            return -1;
//        }
    }
    // NOTICE: unlock occurs in the main writing thread,
    // hence guarantee the correctness of concurrency:
    // 1) no read-uncommitted will occur
    // 2) no read-obsolete will occur
    if (sync_tuple->lock_info.off >= 0) {
        unlock(lockmap, sync_tuple->lock_info.off);
    }
    sync_tuple->sync_flag = 0;
    return 0;
}
