#include <iostream>
#include <memory.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "storage.h"


int FRONTEND_THREAD_NUM;
int BACKEND_THREAD_NUM;

char *dram_base_addr;
void *nvm_force_addr = (void *)0x160000000;
char *nvm_base_addr;

buffer_tuple *TupleSet;
Bucket *CBucket;
Super *SP;
Super *SP_DRAM;

void pm_init(int ntype) {
    if (ntype == 0) {
        nvm_base_addr = (char *)mmap(nvm_force_addr, TOTAL_SIZE, PROT_READ | PROT_WRITE, \
            MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    }
    else {
        int fd = open(MAP_PMEM, O_RDWR | O_CREAT, 0666);
        nvm_base_addr = (char *)mmap(nvm_force_addr, TOTAL_SIZE, PROT_READ | PROT_WRITE, \
            MAP_SHARED, fd, 0);
    }
    SP = (Super *)nvm_base_addr;
    // cached super metadata in DRAM for fast access
    SP_DRAM = (Super *)malloc(SUPER_SIZE);
}

void cache_init() {
    //cached table init
    dram_base_addr = (char *)malloc(DKV_SIZE);
    CBucket = (Bucket *)dram_base_addr;
    memset(CBucket, 0, DKV_SIZE);
}

void buffer_init() {
    // dram buffer init
    TupleSet = (buffer_tuple *)malloc(BUF_TUPLE_SIZE * FRONTEND_THREAD_NUM);
    memset(TupleSet, 0, BUF_TUPLE_SIZE * FRONTEND_THREAD_NUM);

    if (FRONTEND_THREAD_NUM <= MAX_BACKEND_THREAD_NUM) {
        BACKEND_THREAD_NUM = FRONTEND_THREAD_NUM;
    }
    else {
        BACKEND_THREAD_NUM = MAX_BACKEND_THREAD_NUM;
    }
}

void ht_exit() {
    munmap(nvm_base_addr, PKV_SIZE);
    memcpy(SP, SP_DRAM, SUPER_SIZE);
    flush_with_fence(SP);
    SP->magic = MAGIC;
    flush_with_fence(&SP->magic);
#if DRAM_CACHE_ENABLE == 1
    free(dram_base_addr);
    free(TupleSet);
#endif
}
