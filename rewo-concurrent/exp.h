#pragma once

#include "storage.h"
#include "api.h"
#include "factory.h"

#define MAX_TEST_NUM (10000000ULL)
#define MAX_TEST_THREAD (32)

// raw DRAM/Optane latency/bandwidth test
void raw_read_single(int total_size, char *addr, int count, int gran, int threads, int order);
void raw_write_single(int total_size, char *addr, int count, int gran, int threads, int order);
void raw_read_lat(int total_size, char *addr, int count, int gran);
void raw_write_lat(int total_size, char *addr, int count, int gran);
void raw_read_thr(int total_size, char *addr, int count, int gran, int thread_num);
void raw_write_thr(int total_size, char *addr, int count, int gran, int thread_num);

void getUniform(uint32_t count);
void getZipf(uint32_t);

void warm_up(uint32_t key_num);
void load_ycsb_c(uint32_t key_num, uint32_t count);
void load_ycsb_b(uint32_t key_num, uint32_t count);
void load_ycsb_a(uint32_t key_num, uint32_t count);
void load_ycsb_write(uint32_t key_num, uint32_t count);
void load_ycsb_readNeg(uint32_t key_num, uint32_t count);
void load_ycsb_insert(uint32_t key_num, uint32_t count);
void load_ycsb_delete(uint32_t key_num, uint32_t count);
void run_single(uint32_t count, int threads, int order);
void run_workload(uint32_t count, int thread_num);

void randomRead(uint32_t count);
void negativeRead(uint32_t count);
void randomPmRead(uint32_t count);
void randomInsert(uint32_t count);
void randomPmInsert(uint32_t count);
void randomUpdate(uint32_t count);
void randomPmUpdate(uint32_t count);
void randomDelete(uint32_t count);
void randomPmDelete(uint32_t count);
void getLoadFactor();
