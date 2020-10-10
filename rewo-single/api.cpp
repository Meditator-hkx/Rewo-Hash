#include "api.h"
#include "storage.h"

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
    // should be MAGIC
    if (SP->magic == MAGIC) {
        // basically, should recover cache state if cache_overflow is 0
        // but there is no need actually
        // since data will be cached upon PM read hit
        SP->magic = MAGIC - 1;
        flush_with_fence(&SP->magic);
        memcpy(SP_DRAM, SP, SUPER_SIZE);
    }
    // case 2: valid but start from crash
    // should be MAGIC-1
    else if (SP->magic == MAGIC - 1) {
        // continue from resizing if necessary
        if (SP->resize_flag) {
            // redo_resize();
        }

        // loop in each valid table (zone)
        // clear all the locks and count how many key-value items exist
        SP->magic = MAGIC - 1;
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
    char *nvm_addr = (char *)((uint64_t)nvm_force_addr + SP->used_nvm_size);
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

int rewo_search(uint64_t key, char *value) {
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
        return 0;
    }
    return -1;
}

int rewo_search(char *key, char *value) {
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
        return 0;
    }
    return -1;
}

int rewo_insert(uint64_t key, char *value) {
    int ret;
#if DRAM_CACHE_ENABLE == 1
    // sync key-value address to buffer
    // backend dram modification is executed in parallel
    int *sync_signal = sync_buffer(key, value, 1);
#endif
    ret = pm_insert(key, value);
#if DRAM_CACHE_ENABLE == 1
    sync_release(sync_signal);
#endif
    return ret;
}

int rewo_insert(char *key, char *value) {
    int ret;
#if DRAM_CACHE_ENABLE == 1
    // sync key-value address to buffer
    // backend dram modification is executed in parallel
    int *sync_signal = sync_buffer(key, value, 1);
#endif
    ret = pm_insert(key, value);
#if DRAM_CACHE_ENABLE == 1
    sync_release(sync_signal);
#endif
    return ret;
}

int rewo_update(uint64_t key, char *value) {
    int ret;
#if DRAM_CACHE_ENABLE == 1
    int *sync_signal = sync_buffer(key, value, 2);
#endif
    ret = pm_update(key, value);
#if DRAM_CACHE_ENABLE == 1
        sync_release(sync_signal);
#endif
    return ret;
}

int rewo_update(char *key, char *value) {
    int ret;
#if DRAM_CACHE_ENABLE == 1
    int *sync_signal = sync_buffer(key, value, 2);
#endif
    ret = pm_update(key, value);
#if DRAM_CACHE_ENABLE == 1
        sync_release(sync_signal);
#endif
    return ret;
}

int rewo_delete(uint64_t key) {
    int ret;
#if DRAM_CACHE_ENABLE == 1
    int *sync_signal = sync_buffer(key, nullptr, 3);
#endif
    ret = pm_delete(key);
#if DRAM_CACHE_ENABLE == 1
        sync_release(sync_signal);
#endif
    return ret;
}

int rewo_delete(char *key) {
    int ret;
#if DRAM_CACHE_ENABLE == 1
    int *sync_signal = sync_buffer(key, nullptr, 3);
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
    outfile.close();
    return 0;
}

uint32_t rewo_get_kv_num() {
    return SP_DRAM->kv_num;
}

int pm_search(uint64_t key, char *value) {
    uint32_t bucketOff;
    uint32_t table_index;

    for (int h = 0; h < HASH_NUM;h++) {
        bucketOff = persistent_hash(key, table_index, h);
        auto *PBucket = (Bucket *)SP->table_addr[table_index];
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bit state = 1
            if (PBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key fingerprint match
                if (fp_match(&PBucket[bucketOff].meta.fingerprints, i, key)) {
                    if (PBucket[bucketOff].kv[i].key == key) {
#if DRAM_CACHE_ENABLE == 1
                        // cache the kv item
                        cache_insert(key, value);
#endif
                        // return
                        strcpy(value, PBucket[bucketOff].kv[i].value);
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

    for (int h = 0; h < HASH_NUM;h++) {
        bucketOff = persistent_hash(key, table_index, h);
        auto *PBucket = (Bucket *)SP->table_addr[table_index];
        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bit state = 1
            if (PBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key fingerprint match
                if (fp_match(&PBucket[bucketOff].meta.fingerprints, i, key)) {
                    if (strcmp(PBucket[bucketOff].kv[i].ckey, key) == 0) {
#if DRAM_CACHE_ENABLE == 1
                        // cache the kv item
                        cache_insert(key, value);
#endif
                        // return
                        strcpy(value, PBucket[bucketOff].kv[i].value);
                        return 0;
                    }
                }
            }
        }
    }

    // negative search
    return -1;
}

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

            // 2. write data into the slot
            // NOTICE: using ntstore
            ntstore((char *)&PBucket[bucketOff].kv[i].key, (char *)&key, KEY_SIZE);
            ntstore((char *)PBucket[bucketOff].kv[i].value, value, VALUE_SIZE);
            fence();

            // 3. commit the write operation
            // 3.1 append the fingerprint
            fp_append(&PBucket[bucketOff].meta.fingerprints, i, key);
            // 3.2 atomic metadata update
            commit(&PBucket[bucketOff].meta.bitmap, i);
            // 3.3 persist the metadata -> durable transaction
            flush_with_fence(&PBucket[bucketOff].meta);

            // 4. update system metadata
            SP_DRAM->kv_num++;
            SP_DRAM->load_factor = (float) SP_DRAM->kv_num / SP_DRAM->max_load_num;
            return 0;
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
            lock(&cBucket->meta.lockmap, i);

            // 1. search free slot in candidate evict bucket
            for (int j = 0;j < SLOT_PER_BUCKET;j++) {
                if (PBucket[eBucketOff].meta.bitmap & (BIT_FLAG >> j)) {
                    continue;
                }

                // 2. persist the lock state for double record check during crash recovery
                flush_with_fence(&cBucket->meta);

                // 3. migrate data
                ntstore((char *)&PBucket[eBucketOff].kv[j].key, (char *)&cKey, KEY_SIZE);
                ntstore((char *)PBucket[eBucketOff].kv[j].value, cValue, VALUE_SIZE);
                fence();

                // 4. commit and persist the INSERT in eBucket
                fp_append(&PBucket[eBucketOff].meta.fingerprints, j, cKey);
                commit(&PBucket[eBucketOff].meta.bitmap, j);
                flush_with_fence(&PBucket[eBucketOff].meta);


                // 5. delete the original item in cBucket
                invalidate(&cBucket->meta.bitmap, i);
                flush_with_fence(&cBucket->meta);

                // 6. write new data into the freed slot
                ntstore((char *)cBucket->kv[i].key, (char *)&key, KEY_SIZE);
                ntstore((char *)cBucket->kv[i].value, value, VALUE_SIZE);
                fence();

                // 7. commit and persist the INSERT in cBucket
                fp_append(&cBucket->meta.fingerprints, i, key);
                commit(&cBucket->meta.bitmap, i);
                flush_with_fence(&cBucket->meta);

                // 8. unlock current slot in cBucket
                unlock(&cBucket->meta.lockmap, i);

                // 9. update system metadata
                SP_DRAM->kv_num++;
                SP_DRAM->load_factor = (float) SP_DRAM->kv_num / SP_DRAM->max_load_num;
                return 0;
            }
            unlock(&cBucket->meta.lockmap, i);
        }
    }

    // need to resize
    rewo_resize();
    return -1;
}

int pm_insert(char *key, char *value) {
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

            // 2. write data into the slot
            // NOTICE: using ntstore
            ntstore((char *)PBucket[bucketOff].kv[i].ckey, key, KEY_SIZE);
            ntstore((char *)PBucket[bucketOff].kv[i].value, value, VALUE_SIZE);
            fence();

            // 3. commit the write operation
            // 3.1 append the fingerprint
            fp_append(&PBucket[bucketOff].meta.fingerprints, i, key);
            // 3.2 atomic metadata update
            commit(&PBucket[bucketOff].meta.bitmap, i);
            // 3.3 persist the metadata -> durable transaction
            flush_with_fence(&PBucket[bucketOff].meta);

            // 4. update system metadata
            SP_DRAM->kv_num++;
            SP_DRAM->load_factor = (float) SP_DRAM->kv_num / SP_DRAM->max_load_num;
            return 0;
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
            lock(&cBucket->meta.lockmap, i);

            // 1. search free slot in candidate evict bucket
            for (int j = 0;j < SLOT_PER_BUCKET;j++) {
                if (PBucket[eBucketOff].meta.bitmap & (BIT_FLAG >> j)) {
                    continue;
                }

                // 2. persist the lock state for double record check during crash recovery
                flush_with_fence(&cBucket->meta);

                // 3. migrate data
                ntstore((char *)&PBucket[eBucketOff].kv[j].ckey, cKey, KEY_SIZE);
                ntstore((char *)PBucket[eBucketOff].kv[j].value, cValue, VALUE_SIZE);
                fence();

                // 4. commit and persist the INSERT in eBucket
                fp_append(&PBucket[eBucketOff].meta.fingerprints, j, cKey);
                commit(&PBucket[eBucketOff].meta.bitmap, j);
                flush_with_fence(&PBucket[eBucketOff].meta);


                // 5. delete the original item in cBucket
                invalidate(&cBucket->meta.bitmap, i);
                flush_with_fence(&cBucket->meta);

                // 6. write new data into the freed slot
                ntstore((char *)cBucket->kv[i].ckey, key, KEY_SIZE);
                ntstore((char *)cBucket->kv[i].value, value, VALUE_SIZE);
                fence();

                // 7. commit and persist the INSERT in cBucket
                fp_append(&cBucket->meta.fingerprints, i, key);
                commit(&cBucket->meta.bitmap, i);
                flush_with_fence(&cBucket->meta);

                // 8. unlock current slot in cBucket
                unlock(&cBucket->meta.lockmap, i);

                // 9. update system metadata
                SP_DRAM->kv_num++;
                SP_DRAM->load_factor = (float) SP_DRAM->kv_num / SP_DRAM->max_load_num;
                return 0;
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
    uint32_t table_index;
    auto find_flag = false;

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
                        find_flag = true;
                        old_off = i;
                        for (int j = 0;j < SLOT_PER_BUCKET;j++) {
                            if (PBucket[bucketOff].meta.bitmap & (BIT_FLAG >> j)) {
                                continue;
                            }
                            // write into this slot
                            ntstore((char *)&PBucket[bucketOff].kv[j].key, (char *)&key, KEY_SIZE);
                            ntstore(PBucket[bucketOff].kv[j].value, value, VALUE_SIZE);
                            flush_with_fence(&PBucket[bucketOff].kv[j]);

                            // commit and persist metadata
                            fp_append(&PBucket[bucketOff].meta.fingerprints, j, key);
                            commit(&PBucket[bucketOff].meta.bitmap, i, j);
                            flush_with_fence(&PBucket[bucketOff].meta);
                            return 0;
                        }
                        break;
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
                if (i == old_off) {
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

                    // 2. persist the lock state for double record check during crash recovery
                    unlock(&PBucket[bucketOff].meta.lockmap, i);
                    flush_with_fence(&PBucket[bucketOff].meta);

                    // 3. migrate data
                    ntstore((char *)&PBucket[eBucketOff].kv[j].key, (char *)&cKey, KEY_SIZE);
                    ntstore((char *)PBucket[eBucketOff].kv[j].value, cValue, VALUE_SIZE);
                    fence();

                    // 4. commit and persist the INSERT in eBucket
                    fp_append(&PBucket[eBucketOff].meta.fingerprints, j, cKey);
                    commit(&PBucket[eBucketOff].meta.bitmap, j);
                    flush_with_fence(&PBucket[eBucketOff].meta);

                    // 5. delete the original item in target bucket
                    invalidate(&PBucket[bucketOff].meta.bitmap, i);
                    flush_with_fence(&PBucket[bucketOff].meta);

                    // 6. write new data into the freed slot
                    ntstore((char *)&PBucket[bucketOff].kv[i].key, (char *)&key, KEY_SIZE);
                    ntstore((char *)PBucket[bucketOff].kv[i].value, value, VALUE_SIZE);
                    fence();

                    // 7. commit and persist the INSERT in target bucket
                    fp_append(&PBucket[bucketOff].meta.fingerprints, i, key);
                    commit(&PBucket[bucketOff].meta.bitmap, i);
                    flush_with_fence(&PBucket[bucketOff].meta);

                    // 8. unlock current slot in target bucket
                    unlock(&PBucket[bucketOff].meta.lockmap, old_off);

                    return 0;
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
    uint32_t table_index;
    auto find_flag = false;

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
                        find_flag = true;
                        old_off = i;
                        for (int j = 0;j < SLOT_PER_BUCKET;j++) {
                            if (PBucket[bucketOff].meta.bitmap & (BIT_FLAG >> j)) {
                                continue;
                            }
                            // write into this slot
                            ntstore((char *)PBucket[bucketOff].kv[j].ckey, key, KEY_SIZE);
                            ntstore(PBucket[bucketOff].kv[j].value, value, VALUE_SIZE);
                            flush_with_fence(&PBucket[bucketOff].kv[j]);

                            // commit and persist metadata
                            fp_append(&PBucket[bucketOff].meta.fingerprints, j, key);
                            commit(&PBucket[bucketOff].meta.bitmap, i, j);
                            flush_with_fence(&PBucket[bucketOff].meta);
                            return 0;
                        }
                        break;
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
                if (i == old_off) {
                    continue;
                }
                cKey = PBucket[bucketOff].kv[i].ckey;
                cValue = PBucket[bucketOff].kv[i].value;

                // compute candidate evict bucket for candidate evict key
                eBucketOff = persistent_hash(cKey, table_index, 1-h);
                lock(&PBucket[bucketOff].meta.lockmap, old_off);

                // 1. search free slot in candidate evict bucket
                for (int j = 0;j < SLOT_PER_BUCKET;j++) {
                    if (PBucket[eBucketOff].meta.bitmap & (BIT_FLAG >> j)) {
                        continue;
                    }

                    // 2. persist the lock state for double record check during crash recovery
                    flush_with_fence(&PBucket[bucketOff].meta);

                    // 3. migrate data
                    ntstore((char *)PBucket[eBucketOff].kv[j].ckey, cKey, KEY_SIZE);
                    ntstore((char *)PBucket[eBucketOff].kv[j].value, cValue, VALUE_SIZE);
                    fence();

                    // 4. commit and persist the INSERT in eBucket
                    fp_append(&PBucket[eBucketOff].meta.fingerprints, j, cKey);
                    commit(&PBucket[eBucketOff].meta.bitmap, j);
                    flush_with_fence(&PBucket[eBucketOff].meta);

                    // 5. delete the original item in target bucket
                    invalidate(&PBucket[bucketOff].meta.bitmap, i);
                    flush_with_fence(&PBucket[bucketOff].meta);

                    // 6. write new data into the freed slot
                    ntstore((char *)PBucket[bucketOff].kv[i].ckey, key, KEY_SIZE);
                    ntstore((char *)PBucket[bucketOff].kv[i].value, value, VALUE_SIZE);
                    fence();

                    // 7. commit and persist the INSERT in target bucket
                    fp_append(&PBucket[bucketOff].meta.fingerprints, i, key);
                    commit(&PBucket[bucketOff].meta.bitmap, i);
                    flush_with_fence(&PBucket[bucketOff].meta);

                    // 8. unlock current slot in target bucket
                    unlock(&PBucket[bucketOff].meta.lockmap, old_off);

                    // 9. update system metadata
                    return 0;
                }
                unlock(&PBucket[bucketOff].meta.lockmap, old_off);
            }
            // need to resize and retry
            rewo_resize();
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
                        invalidate(&PBucket[bucketOff].meta.bitmap, i);
                        flush_with_fence(&PBucket[bucketOff].meta);

                        // update system metadata
                        SP_DRAM->kv_num--;
                        SP_DRAM->load_factor = (float) SP_DRAM->kv_num / SP_DRAM->max_load_num;
                        return 0;
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

        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bit state = 1
            if (PBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key fingerprint match
                if (fp_match(&PBucket[bucketOff].meta.fingerprints, i, key)) {
                    if (strcmp(PBucket[bucketOff].kv[i].ckey, key) == 0) {
                        invalidate(&PBucket[bucketOff].meta.bitmap, i);
                        flush_with_fence(&PBucket[bucketOff].meta);

                        // update system metadata
                        SP_DRAM->kv_num--;
                        SP_DRAM->load_factor = (float) SP_DRAM->kv_num / SP_DRAM->max_load_num;
                        return 0;
                    }
                }
            }
        }
    }

    // data not found
    return -1;
}

int cache_search(uint64_t key, char *value) {
    uint32_t bucketOff;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = cache_hash(key, h);

        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bitmap state = 1
            if (CBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key match
                if (CBucket[bucketOff].kv[i].key == key) {
                    strcpy(value, CBucket[bucketOff].kv[i].value);

                    // update the bucket lru information
                    lru_update(&CBucket[bucketOff].meta.fingerprints, i);

                    return 0;
                }
            }
        }
    }
    
    return -1;
}

int cache_search(char *key, char *value) {
    uint32_t bucketOff;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = cache_hash(key, h);

        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bitmap state = 1
            if (CBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key match
                if (strcmp(CBucket[bucketOff].kv[i].ckey, key) == 0) {
                    strcpy(value, CBucket[bucketOff].kv[i].value);

                    // update the bucket lru information
                    lru_update(&CBucket[bucketOff].meta.fingerprints, i);

                    return 0;
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
                // 2. write in the slot
                CBucket[bucketOff].kv[i].key = key;
                strcpy(CBucket[bucketOff].kv[i].value, value);

                // 3. commit the write operation
                commit(&CBucket[bucketOff].meta.bitmap, i);

                // 4. update the LRU info
                // 4.1 the ith slot should be the latest record
                lru_update(&CBucket[bucketOff].meta.lru_sorteds, i);
                return 0;
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
                // 2. write in the slot
                memcpy(CBucket[bucketOff].kv[i].ckey, key, KEY_SIZE);
                strcpy(CBucket[bucketOff].kv[i].value, value);

                // 3. commit the write operation
                commit(&CBucket[bucketOff].meta.bitmap, i);

                // 4. update the LRU info
                // 4.1 the ith slot should be the latest record
                lru_update(&CBucket[bucketOff].meta.lru_sorteds, i);
                return 0;
            }
        }
    }

    // fail to insert, but okay
    // future reads on this key can directly probe in NVM buckets
    return -1;
}

int cache_update(uint64_t key, char *value) {
    uint32_t bucketOff;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = cache_hash(key, h);

        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bitmap state = 1
            if (CBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key match
                if (CBucket[bucketOff].kv[i].key == key) {
                    // update value (in-place update)
                    strcpy(CBucket[bucketOff].kv[i].value, value);

                    // update the LRU info
                    // the ith slot should be the latest record
                    lru_update(&CBucket[bucketOff].meta.lru_sorteds, i);
                    return 0;
                }
            }
        }
    }

    // fail to update, but does not impact correctness
    // should return cache_insert(key, value);
    return -1;
}

int cache_update(char *key, char *value) {
    uint32_t bucketOff;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = cache_hash(key, h);

        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bitmap state = 1
            if (CBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key match
                if (strcmp(CBucket[bucketOff].kv[i].ckey, key) == 0) {
                    // update value (in-place update)
                    strcpy(CBucket[bucketOff].kv[i].value, value);

                    // update the LRU info
                    // the ith slot should be the latest record
                    lru_update(&CBucket[bucketOff].meta.lru_sorteds, i);
                    return 0;
                }
            }
        }
    }

    // fail to update, but does not impact correctness
    // should return cache_insert(key, value);
    return -1;
}

int cache_delete(uint64_t key) {
    uint32_t bucketOff;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = cache_hash(key, h);

        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bitmap state = 1
            if (CBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key match
                if (CBucket[bucketOff].kv[i].key == key) {
                    // delete the item
                    invalidate(&CBucket[bucketOff].meta.bitmap, i);
                    return 0;
                }
            }
        }
    }

    // data not found
    return -1;
}

int cache_delete(char *key) {
    uint32_t bucketOff;

    for (int h = 0;h < HASH_NUM;h++) {
        bucketOff = cache_hash(key, h);

        for (int i = 0;i < SLOT_PER_BUCKET;i++) {
            // bitmap state = 1
            if (CBucket[bucketOff].meta.bitmap & (BIT_FLAG >> i)) {
                // key match
                if (strcmp(CBucket[bucketOff].kv[i].ckey, key) == 0) {
                    // delete the item
                    invalidate(&CBucket[bucketOff].meta.bitmap, i);
                    return 0;
                }
            }
        }
    }

    // data not found
    return -1;
}

/*
 * Formally, we should use LRU algorithm.
 * Int that case, a slot array field is required in the metadata
 * But it seems that random eviction can still work well
 */
void cache_replace(uint64_t key, char *value) {
    int ret = cache_insert(key, value);
    if (ret == 0)
        return;

    uint32_t bucketOff = cache_hash(key, 1);

    // choose the least-recently-accessed element to be evicted
    auto k = lru_evict(&CBucket[bucketOff].meta.lru_sorteds);

    // write data into it
    CBucket[bucketOff].kv[k].key = key;
    strcpy(CBucket[bucketOff].kv[k].value, value);

    // update bucket lru information
    lru_update(&CBucket[bucketOff].meta.lru_sorteds);
}

void cache_replace(char *key, char *value) {
    int ret = cache_insert(key, value);
    if (ret == 0)
        return;

    uint32_t bucketOff = cache_hash(key, 1);

    // choose the least-recently-accessed element to be evicted
    auto k = lru_evict(&CBucket[bucketOff].meta.lru_sorteds);

    // write data into it
    memcpy(CBucket[bucketOff].kv[k].ckey, key, KEY_SIZE);
    strcpy(CBucket[bucketOff].kv[k].value, value);

    // update bucket lru information
    lru_update(&CBucket[bucketOff].meta.lru_sorteds);
}

// may be interesting to test the overall delay of building these backgroud threads 
// and compute the average context switch overhead for them 
void back_execute() {
    // start backend thread
    std::thread back_thread(thread_execute);
    back_thread.detach();
}

void thread_execute() {
    // keep its logic easy to understand as prolegomenon
    while(1) {
        switch(TupleSet[0].sync_flag) {
        case 1:
            cache_insert(TupleSet[0].key, TupleSet[0].v_addr);
            TupleSet[0].sync_flag = 4;
            break;
        case 2:
            cache_update(TupleSet[0].key, TupleSet[0].v_addr);
            TupleSet[0].sync_flag = 4;
            break;
        case 3:
            cache_delete(TupleSet[0].key);
            TupleSet[0].sync_flag = 4;
            break;
        default:
            break;
        }
    }
}

int  *sync_buffer(uint64_t key, char *value, int flag) {
    TupleSet[0].key = key;
    TupleSet[0].v_addr = value;
    TupleSet[0].sync_flag = flag;
    return &TupleSet[0].sync_flag;
}

int  *sync_buffer(char *key, char *value, int flag) {
    TupleSet[0].ckey = key;
    TupleSet[0].v_addr = value;
    TupleSet[0].sync_flag = flag;
    return &TupleSet[0].sync_flag;
}

int sync_release(int *sync) {
    int max_count = 1000;
    while (*sync != 4) {
//        max_count--;
//        if (max_count == 0) {
//            std::cout << "sync error: backend dram modification failed!" << std::endl;
//            return -1;
//        }
    }
    *sync = 0;
    return 0;
}
