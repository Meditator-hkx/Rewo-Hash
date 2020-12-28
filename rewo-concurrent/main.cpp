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
    int ttype, ntype, otype, nsize, count, threads;
    float occupancy;

    if (argc == 8) {
        ttype= atoi(argv[1]);
        ntype = atoi(argv[2]);
        otype = atoi(argv[3]);
        nsize = atoi(argv[4]) * 1024 * 1024;
        count = atoi(argv[5]);
        threads = atoi(argv[6]);
        occupancy = atof(argv[7]);
    }
    else {
        std::cout << "input error.\n";
        std::cout << "./rewo-concurrent test_type nvm_type operation_type nvm_size test_count test_granularity thread_num.\n";
        std::cout << "test_type: 0 = raw test; 1 = rewo-concurrent test.\n";
        std::cout << "nvm_type: 0 = DRAM; 1 = Optane.\n";
        std::cout << "operation_type: 0 = search; 1 = insert; 2 = update; 3 = delete.\n";
        std::cout << "nvm_size: xx MBytes.\n";
        std::cout << "test_count: xx times.\n";
        std::cout << "thread_num: xx threads.\n";

        ttype = 1;
        ntype = 0;
        otype = 0;
        nsize = 100 * 1024 * 1024;
        count = 10000;
        threads = 1;
        occupancy = 0.2;
//        return -1;
    }



    // temporal test
//    cout << "size of bucket: " << sizeof(Bucket) << endl;
//    cout << "size of key-value: " << sizeof(KeyValue) << endl;
//    cout << "size of bucket meta: " << sizeof(meta) << endl;
//    return 0;

    FRONTEND_THREAD_NUM = threads;

    if (ttype == 0) {
        rewo_init(ntype);
        show_cli();
    }
    else if (ttype == 1) {
        rewo_init(ntype);
        int ret;
        int key1 = 1;
        char *value1 = (char *)"value1";
        char *value2 = (char *)"value2";
        char *value_get = (char *)malloc(20);
        int key2 = 2;
        char *value3 = (char *)"value3";

        startTimer(start);
        ret = rewo_insert(0, key1, value1);
        endTimer(endt);
        printLatency();

        startTimer(start);
        ret = rewo_search(0, key1, value_get);
        endTimer(endt);
        printLatency();

        startTimer(start);
        ret = rewo_update(0, key1, value2);
        endTimer(endt);
        printLatency();

        startTimer(start);
        ret = rewo_search(0, key1, value_get);
        endTimer(endt);
        printLatency();

        startTimer(start);
        ret = rewo_insert(0, key2, value3);
        ret = rewo_search(0, key2, value_get);
        endTimer(endt);
        printLatency();

        startTimer(start);
        ret = rewo_delete(0, key1);
        ret = rewo_search(0, key1, value_get);
        endTimer(endt);
        printLatency();

        ret = rewo_insert(0, key1, value1);
        ret = rewo_search(0, key1, value_get);
        free(value_get);
    }

    
    else if (ttype == 2) {
        /* build the system */
        rewo_init(ntype);
        if (otype == 0) {
            // vary the load factor;
            // vary positive and negative
            warm_up(PBUCKET_NUM * 8 * threads / 10);
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
            warm_up(PBUCKET_NUM * 8 * threads / 10);
            getLoadFactor();
            std::cout << "random insert: \n";
            randomInsert(count);
        }
        else if (otype == 2) {
            warm_up(PBUCKET_NUM * 8 * threads / 10);
            getLoadFactor();
            std::cout << "random update: \n";
            randomUpdate(count);
        }
        else {
            warm_up(PBUCKET_NUM * 8 * threads / 10);
            getLoadFactor();
            std::cout << "random delete: \n";
            randomDelete(count);
        }
    }
    else if (ttype == 3) {
        /* build the system */
        rewo_init(ntype);
        int real_num = PBUCKET_NUM * 8 * 6 / 10;
        warm_up(real_num);
        getLoadFactor();
        if (otype == 0) {
            load_ycsb_c(real_num,count);
            run_workload(count, threads);
        } 
        else if (otype == 1) {
            load_ycsb_b(real_num,count);
            run_workload(count, threads);
        }
        else if (otype == 2) {
            load_ycsb_a(real_num,count);
            run_workload(count, threads);
        }
        else if (otype == 3) {
            load_ycsb_write(real_num,count);
            run_workload(count, threads);
        }
        else if (otype == 4) {
            load_ycsb_readNeg(real_num,count);
            run_workload(count, threads);
        }
        else if (otype == 5) {
            load_ycsb_insert(real_num,count);
            run_workload(count, threads);
        }
        else {
            load_ycsb_delete(real_num,count);
            run_workload(count, threads);
        }
    }

    rewo_exit();
    return 0;
}
