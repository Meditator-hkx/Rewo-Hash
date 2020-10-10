#include "exp.h"
#include <math.h>
#include <iostream>

uint64_t key_set[MAX_TEST_NUM];
uint32_t option_set[MAX_TEST_NUM];

void getLoadFactor() {
    cout << "hash table load factor = " << SP_DRAM->load_factor << "\n";
}

void warm_up(uint32_t key_num) {
    cout << key_num << " keys should be inserted into hash table." << "\n";
    char *value = (char *)"random_value";
    int inserted = 0;
    for (uint32_t i = 1;i <= key_num;i++) {
        if (rewo_insert(i, value) == 0) {
            inserted++;
        }
    }
    cout << inserted << " keys are inserted into hash table." << "\n";
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

void load_ycsb_c(uint32_t key_num, uint32_t count) {
    for (uint32_t j = 0;j < count;j++) {
        key_set[j] = (uint64_t)random() % key_num;
        option_set[j] = 0;
    }

//    getZipf(count);
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

void run_workload(uint32_t count) {
    startTimer(start);
    run_single(count);
    endTimer(endt);
    addToTotalLatency();
    printThroughput(count,printTotalLatency());
    resetTotalLatency();
}

void run_single(uint32_t count) {
    // std::cout << count << " requests should be executed." << std::endl;
    char *value_get = (char *)malloc(VALUE_SIZE);
    char *value_update = (char *)"random_value";
    int executed = 0;
    for (uint32_t i = 0;i < count;i++) {
        if (option_set[i] == 1) {
            if (rewo_insert(key_set[i], value_update) == 0) {
                executed++;
            }
        }
        else if (option_set[i] == 2) {
            if (rewo_update(key_set[i], value_update) == 0) {
                executed++;
            }
        }
        else if (option_set[i]== 3) {
            if (rewo_delete(key_set[i]) == 0) {
                executed++;
            }
        }
        else {
            if (rewo_search(key_set[i], value_get) == 0) {
                executed++;
            }
        }
    }
    // std::cout << executed << " requests are successfully executed." << std::endl;
}

void randomRead(uint32_t count) {
    int ret;
    uint32_t valid_count = 0;
    char *value = (char *)malloc(VALUE_SIZE);
    for (uint32_t i = 0;i < count;i++) {
        int key = random() % SP_DRAM->kv_num + 1;
        startTimer(start);
        ret = rewo_search(key, value);
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
        ret = rewo_search(key, value);
        // ret = pm_search(key, value);
        if (ret == 0) {
            valid_count++;
        }
        fence();
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
        ret = rewo_insert(key, value);
        if (ret == 0) {
            valid_count++;
        }
        endTimer(endt);
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
        ret = rewo_update(key, value);
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
        ret = rewo_update(key, value);
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
        ret = rewo_delete(i);
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
