#include "exp.h"
#include <math.h>
#include <iostream>

uint64_t key_set[MAX_TEST_NUM];
uint32_t option_set[MAX_TEST_NUM];

void getLoadFactor() {
    std::cout << "hash table load factor = " << SP_DRAM->load_factor << "\n";
}

void raw_read_lat(int total_size, char *addr, int count, int gran) {
    char *buf = (char *)malloc(gran);
    int num = total_size / gran;
    int order;
    for (int i = 0;i < count;i++) {
        order = random() % num;
        startTimer(start);
        memcpy(buf, addr + order * gran, gran);
        endTimer(endt);
        fence();
        addToTotalLatency();
    }
    printAverageLatency(printTotalLatency(), count);
    resetTotalLatency();
}

void raw_write_lat(int total_size, char *addr, int count, int gran) {
    char *buf = (char *)malloc(gran);
    memset(buf, 1, gran);
    int num = total_size / gran;
    int order;
    for (int i = 0;i < count;i++) {
        order = random() % num;
        startTimer(start);
        memcpy(addr + order * gran, buf, gran);
        flush_with_fence(addr+order*gran);
        endTimer(endt);
        addToTotalLatency();
    }
    printAverageLatency(printTotalLatency(), count);
    resetTotalLatency();
}

void raw_read_thr(int total_size, char *addr, int count, int gran, int thread_num) {
    std::thread threads[thread_num];
    startTimer(start);
    for (int i = 0;i < thread_num;i++) {
        threads[i] = std::thread(raw_read_single, total_size, addr, count, gran, thread_num, i);
    }
    for (int i = 0;i < thread_num;i++) {
        threads[i].join();
    }
    endTimer(endt);
    addToTotalLatency();
    printThroughput(total_size/gran, printTotalLatency());
    resetTotalLatency();
}

void raw_write_thr(int total_size, char *addr, int count, int gran, int thread_num) {
    std::thread threads[thread_num];
    startTimer(start);
    for (int i = 0;i < thread_num;i++) {
        threads[i] = std::thread(raw_write_single, total_size, addr, count, gran, thread_num, i);
    }
    for (int i = 0;i < thread_num;i++) {
        threads[i].join();
    }
    endTimer(endt);
    addToTotalLatency();
    printThroughput(total_size/gran, printTotalLatency());
    resetTotalLatency();
}


void raw_read_single(int total_size, char *addr, int count, int gran, int threads, int order) {
    char *buf = (char *)malloc(gran);
    int num = total_size / gran;
    for (int i = order;i < total_size / gran;i = i + threads) {
        memcpy(buf, addr + i * gran, gran);
    }
}

void raw_write_single(int total_size, char *addr, int count, int gran, int threads, int order) {
    char *buf = (char *)malloc(gran);
    memset(buf, 1, gran);
    int num = total_size / gran;
    for (int i = order;i < total_size / gran;i = i + threads) {
        memcpy(addr + i * gran, buf, gran);
        // asm_clflush((addr+order *gran)); asm_mfence();
    }
}

void warm_up(uint32_t key_num) {
    std::cout << key_num << " keys should be inserted into hash table." << "\n";
    char *value = (char *)"random_value";
    int inserted = 0;
    for (uint32_t i = 1;i <= key_num;i++) {
        if (rewo_insert(0, i, value) == 0) {
            inserted++;
        }
    }
    std::cout << inserted << " keys are inserted into hash table." << "\n";
}

void load_ycsb_c(uint32_t key_num, uint32_t count) {
    for (uint32_t j = 0;j < count;j++) {
        key_set[j] = (uint64_t)random() % key_num;
        option_set[j] = 0;
    }

    // getZipf(count);
}

void load_ycsb_readNeg(uint32_t key_num, uint32_t count) {
    for (uint32_t j = 0;j < count;j++) {
        key_set[j] = SP_DRAM->kv_num + j + 1;
        option_set[j] = 0;
    }
}

void load_ycsb_b(uint32_t key_num, uint32_t count) {
    uint64_t rand;
    for (uint32_t j = 0;j < count;j++) {
        rand = (uint64_t)random();
        key_set[j] = rand % key_num;

        if (rand % 100 < 95) {
            option_set[j] = 0;
        }
        else {
            option_set[j] = 2;
        }
    }
}

void load_ycsb_a(uint32_t key_num, uint32_t count) {
    uint64_t rand;
    for (uint32_t j = 0;j < count;j++) {
        rand = (uint64_t)random();
        key_set[j] = rand % key_num;

        if (rand % 100 < 50) {
            option_set[j] = 0;
        }
        else {
            option_set[j] = 2;
        }
    }
}

void load_ycsb_write(uint32_t key_num, uint32_t count) {
    for (uint32_t j = 0;j < count;j++) {
        key_set[j] = (uint64_t)random() % key_num;
        option_set[j] = 2;
    }
}

void load_ycsb_delete(uint32_t key_num, uint32_t count) {
    for (uint32_t j = 0;j < count;j++) {
        key_set[j] = (uint64_t)random() % key_num;
        option_set[j] = 3;
    }
}

void load_ycsb_insert(uint32_t key_num, uint32_t count) {
    for (uint32_t j = 0;j < count;j++) {
        key_set[j] = SP_DRAM->kv_num + j + 1;
        option_set[j] = 1;
    }
}

void run_workload(uint32_t count, int thread_num) {
    std::thread threads[thread_num];
    startTimer(start);
    // run_single(count, 0);
    for (int i = 0;i < thread_num;i++) {
        threads[i] = std::thread(run_single, count, thread_num, i);
    }
    for (int i = 0;i < thread_num;i++) {
        threads[i].join();
    }
    endTimer(endt);
    addToTotalLatency();
    printThroughput(count,printTotalLatency());
    resetTotalLatency();
}

void run_single(uint32_t count, int threads, int order) {
    char *value_get = (char *)malloc(VALUE_SIZE);
    char *value_update = (char *)"random_value";
    int executed = 0;
    for (int i = order;i < count;i = i + threads) {
        if (option_set[i] == 1) {
            if (rewo_insert(order, key_set[i], value_update) == 0) {
                executed++;
            }
        }
        else if (option_set[i] == 2) {
            if (rewo_update(order, key_set[i], value_update) == 0) {
                executed++;
            }
        }
        else if (option_set[i]== 3) {
            if (rewo_delete(order, key_set[i]) == 0) {
                executed++;
            }
        }
        else {
            if (rewo_search(order, key_set[i], value_get) == 0) {
                executed++;
            }
        }
    }
}

void randomRead(uint32_t count) {
    int ret;
    uint32_t valid_count = 0;
    char *value = (char *)malloc(VALUE_SIZE);
    for (uint32_t i = 0;i < count;i++) {
        int key = random() % SP_DRAM->kv_num + 1;
        startTimer(start);
        ret = rewo_search(0, key, value);
        if (ret == 0) {
            valid_count++;
        }
        // asm_mfence();
        endTimer(endt);
        addToTotalLatency();
    }
    printAverageLatency(printTotalLatency(), count);
    std::cout << valid_count << " items are found!\n";
    resetTotalLatency();
}

void negativeRead(uint32_t count) {
    int ret;
    uint32_t valid_count = 0;
    char *value = (char *)malloc(VALUE_SIZE);
    for (uint32_t i = 0;i < count;i++) {
        int key = SP_DRAM->kv_num + i + 1;
        startTimer(start);
        ret = rewo_search(0, key, value);
        // ret = pm_search(key, value);
        if (ret == 0) {
            valid_count++;
        }
        // asm_mfence();
        endTimer(endt);
        fence();
        addToTotalLatency();
    }
    printAverageLatency(printTotalLatency(), count);
    std::cout << valid_count << " items are found!\n";
    resetTotalLatency();

}

void randomPmRead(uint32_t count) {
    int ret;
    uint32_t valid_count = 0;
    char *value = (char *)malloc(VALUE_SIZE);
    for (uint32_t i = 0;i < count;i++) {
        int key = random() % SP_DRAM->kv_num;
        startTimer(start);
        ret = pm_search(key, value);
        if (ret == 0) {
            valid_count++;
        }
        endTimer(endt);
        fence();
        addToTotalLatency();
    }
    printAverageLatency(printTotalLatency(), count);
    resetTotalLatency();
}

void randomInsert(uint32_t count) {
    int ret;
    uint32_t valid_count = 0;
    char *value = (char *)"random value";
    for (uint32_t i = 0;i < count;i++) {
        int key = SP_DRAM->kv_num + i + 1;
        startTimer(start);
        ret = rewo_insert(0, key, value);
        if (ret == 0) {
            valid_count++;
        }
        endTimer(endt);
        fence();
        addToTotalLatency();
    }
    printAverageLatency(printTotalLatency(), count);
    resetTotalLatency();
}

void randomPmInsert(uint32_t count) {
    int ret;
    uint32_t valid_count = 0;
    char *value = (char *)"random value";
    for (uint32_t i = 0;i < count;i++) {
        int key = SP_DRAM->kv_num + i + 1;
        startTimer(start);
        ret = rewo_update(0, key, value);
        if (ret == 0) {
            valid_count++;
        }
        endTimer(endt);
        addToTotalLatency();
        fence();
    }
    printAverageLatency(printTotalLatency(), count);
    resetTotalLatency();
}

void randomUpdate(uint32_t count) {
    int ret;
    uint32_t valid_count = 0;
    char *value = (char *)"random value";
    for (uint32_t i = 0;i < count;i++) {
        int key = random() % SP_DRAM->kv_num;
        startTimer(start);
        ret = rewo_update(0, key, value);
        if (ret == 0) {
            valid_count++;
        }
        endTimer(endt);
        addToTotalLatency();
        fence();
    }
    printAverageLatency(printTotalLatency(), count);
    resetTotalLatency();
}

void randomPmUpdate(uint32_t count) {
    int ret;
    uint32_t valid_count = 0;
    char *value = (char *)"random value";
    for (uint32_t i = 0;i < count;i++) {
        int key = random() % SP_DRAM->kv_num;
        startTimer(start);
        ret = pm_update(key, value);
        if (ret == 0) {
            valid_count++;
        }
        endTimer(endt);
        addToTotalLatency();
        fence();
    }
    printAverageLatency(printTotalLatency(), count);
    resetTotalLatency();
}

void randomDelete(uint32_t count) {
    int ret;
    uint32_t valid_count = 0;
    for (uint32_t i = 0;i < count;i++) {
        startTimer(start);
        ret = rewo_delete(0, i);
        if (ret == 0) {
            valid_count++;
        }
        endTimer(endt);
        addToTotalLatency();
        fence();
    }
    printAverageLatency(printTotalLatency(), count);
    resetTotalLatency();
}

void randomPmDelete(uint32_t count) {
    int ret;
    uint32_t valid_count = 0;
    for (uint32_t i = 0;i < count;i++) {
        startTimer(start);
        ret = pm_delete(i);
        if (ret == 0) {
            valid_count++;
        }
        endTimer(endt);
        addToTotalLatency();
        fence();
    }
    printAverageLatency(printTotalLatency(), count);
    resetTotalLatency();
}

void getUniform(uint32_t count) {
    for (int i = 0;i < count;i++) {
        key_set[i] = random() % SP_DRAM->kv_num + 1;
    }
}

void getZipf(uint32_t count) {
    double s = 0.99;
    uint32_t k = SP_DRAM->kv_num;
    double *freqs = (double *)malloc(k*sizeof(double));
    double sum = 0;

    // write zipf function
    for (int i = 1;i <= k;i++) {
        sum += (1 / pow(i, s));
    }
    for (int i = 1;i <= k;i++) {
        if (i == 1) {
            freqs[i-1] = (1 / pow(i, s)) / sum;
        }
        else {
            freqs[i-1] = (1 / pow(i, s)) / sum + freqs[i-2];
        }
    }

    // write rid_set according to zipf distribution
    srand(0);
    uint32_t r;
    double ratio;
    for (int i = 0; i < count;i++) {
        r = (uint32_t)random();
        ratio = (double)r / RAND_MAX;
        if (ratio <= freqs[0]) {
            key_set[i] = 1;
        }
        else {
            for (uint32_t j = 0;j < k;j++) {
                if (ratio > freqs[j] && ratio <= freqs[j+1]) {
                    key_set[i] = j + 1;
                    break;
                }
            }
        }
    }
}
