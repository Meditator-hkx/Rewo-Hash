#include "factory.h"
#include <iostream>

struct timespec start, endt;
double total = 0;

double printLatency() {
    double latency = (endt.tv_sec - start.tv_sec) * NS_RATIO + (double)(endt.tv_nsec - start.tv_nsec) / 1000;
    std::cout << "latency (us): " << latency << std::endl;
    return latency;
}
double singleLatency() {
    double latency = (endt.tv_sec - start.tv_sec) * NS_RATIO + (double)(endt.tv_nsec - start.tv_nsec) / 1000;
    return latency;
}
double addToTotalLatency() {
    total += singleLatency();
    return total;
}
double printTotalLatency() {
//    std::cout << "total latency (us): " << total << std::endl;
    return total;
}
void resetTotalLatency() {
    total = 0;
}

double printAverageLatency(double latency, uint32_t num) {
    double average_lat = latency / num;
    std::cout << "average latency (us): " << average_lat << std::endl;
    return average_lat;
}

double printThroughput(uint32_t num_ops, double latency) {
    double throughput = num_ops / latency;
    std::cout << "throughput (MOps/s): " << throughput << std::endl;
    return throughput;
}
