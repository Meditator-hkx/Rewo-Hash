#include <iostream>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "stdio.h"
#include "time.h"
#include "hash.h"
#include "storage.h"
#include "api.h"
#include "cli.h"
#include "exp.h"

int main(int argc, char *argv[]) {
    int ttype, ntype, otype, nsize, count;
    float occupancy;

    if (argc == 7) {
        ttype= atoi(argv[1]);
        ntype = atoi(argv[2]);
        otype = atoi(argv[3]);
        nsize = atoi(argv[4]) * 1024 * 1024;
        count = atoi(argv[5]);
        occupancy = atof(argv[6]);
    }
    else {
        std::cout << "input error.\n";
        std::cout << "./rewo-single test_type nvm_type operation_type nvm_size test_count test_granularity thread_num.\n";
        std::cout << "test_type: 0 = enter interactive mode; 1 = simple case test; 2 = run latency test; 3 = run throughput test. \n";
        std::cout << "nvm_type: 0 = DRAM; 1 = Optane.\n";
        std::cout << "operation_type: 0 = search; 1 = insert; 2 = update; 3 = delete. \n";
        std::cout << "nvm_size: xx MBytes.\n";
        std::cout << "test_count: xx times.\n";
        std::cout << "test_num: 1, 2, 4, ...\n";
        std::cout << "load factor: 1, 2, 4, ...\n";
        std::cout << "entering default mode...\n";

        ttype = 0;
        ntype = 0;
        otype = 0;
        nsize = 100 * 1024 * 1024;
        count = 10000;
        occupancy = 0.2;
//        return -1;
    }


    rewo_init(ntype);
//    warm_up(PBUCKET_NUM * SLOT_PER_BUCKET * occupancy);

    if (ttype == 0) {
        show_cli();
        return 0;
    }

    if (ttype == 1) {
        int ret = 0;
        uint64_t key1 = 1; char *value1 = (char *)"v1";
        uint64_t key2 = 2; char *value2 = (char *)"v2";
        char *value_get = (char *)malloc(VALUE_SIZE);

        startTimer(start);
        ret = rewo_insert(key1, value1);
        endTimer(endt);
        printLatency();

        startTimer(start);
        ret = rewo_search(key1, value_get);
        endTimer(endt);
        printLatency();

        startTimer(start);
        ret = rewo_update(key1, value2);
        endTimer(endt);
        printLatency();

        startTimer(start);
        ret = rewo_search(key1, value_get);
        endTimer(endt);
        printLatency();

        startTimer(start);
        ret = rewo_insert(key2, value2);
        ret = rewo_search(key2, value_get);
        endTimer(endt);
        printLatency();

        startTimer(start);
        ret = rewo_delete(key1);
        ret = rewo_search(key1, value_get);
        ret = rewo_search(key1, value_get);
        ret = rewo_search(key2, value_get);
        endTimer(endt);
        printLatency();

        ret = rewo_insert(key1, value1);
        ret = rewo_search(key1, value_get);
        free(value_get);

        return ret;
    }
    else if (ttype == 2) {
        /* build the system */
        rewo_init(ntype);
        if (otype == 0) {
            // vary the load factor;
            // vary positive and negative
            warm_up(PBUCKET_NUM * SLOT_PER_BUCKET * occupancy);
            getLoadFactor();
            if (nsize == 0) {
                std::cout << "negative read: \n";
                negativeRead(count);
            }
            else {
                std::cout << "positive read: \n";
                randomRead(count);
            }
        }
        else if (otype == 1) {
            warm_up(PBUCKET_NUM * SLOT_PER_BUCKET * occupancy);
            getLoadFactor();
            std::cout << "random insert: \n";
            randomInsert(count);
        }
        else if (otype == 2) {
            warm_up(PBUCKET_NUM * SLOT_PER_BUCKET * occupancy);
            getLoadFactor();
            std::cout << "random update: \n";
            randomUpdate(count);
        }
        else {
            warm_up(PBUCKET_NUM * SLOT_PER_BUCKET * occupancy);
            getLoadFactor();
            std::cout << "random delete: \n";
            randomDelete(count);
        }
    }
    else if (ttype == 3) {
        /* build the system */
        rewo_init(ntype);
        int real_num = PBUCKET_NUM * SLOT_PER_BUCKET * occupancy;
        // int real_num = 4000;
        warm_up(real_num);
        getLoadFactor();
        if (otype == 0) {
            load_ycsb_c(real_num, count);
            run_workload(count);
        } 
        else if (otype == 1) {
            load_ycsb_b(real_num,count);
            run_workload(count);
        }
        else if (otype == 2) {
            load_ycsb_a(real_num,count);
            run_workload(count);
        }
        else if (otype == 3) {
            load_ycsb_write(real_num,count);
            run_workload(count);
        }
        else if (otype == 4) {
            load_ycsb_readNeg(real_num,count);
            run_workload(count);
        }
        else if (otype == 5) {
            load_ycsb_insert(real_num,count);
            run_workload(count);
        }
        else {
            load_ycsb_delete(real_num,count);
            run_workload(count);
        }
    }

    rewo_exit();
    return 0;
}
