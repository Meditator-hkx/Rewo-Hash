#pragma once

#include "storage.h"
#include "api.h"
#include "factory.h"
#include <iostream>

using namespace std;

#define MAX_TEST_NUM (10000000ULL)
#define MAX_TEST_THREAD (32)

void getUniform(uint32_t count);
void getZipf(uint32_t count);
void warm_up(uint32_t key_num);
void load_ycsb_c(uint32_t key_num, uint32_t count);
void load_ycsb_b(uint32_t key_num, uint32_t count);
void load_ycsb_a(uint32_t key_num, uint32_t count);
void load_ycsb_write(uint32_t key_num, uint32_t count);
void load_ycsb_readNeg(uint32_t key_num, uint32_t count);
void load_ycsb_insert(uint32_t key_num, uint32_t count);
void load_ycsb_delete(uint32_t key_num, uint32_t count);
void run_single(uint32_t count);
void run_workload(uint32_t count);

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