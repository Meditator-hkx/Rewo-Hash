#pragma once

#include<stdint.h>
#include<stdlib.h>
#include<unistd.h>

extern struct timespec start, endt;
extern double total;

#define startTimer(start) (clock_gettime(CLOCK_REALTIME, &start))
#define endTimer(endt) (clock_gettime(CLOCK_REALTIME, &endt))
#define NS_RATIO (1000UL * 1000)

double printLatency();
double singleLatency();
double addToTotalLatency();
double printTotalLatency();
void resetTotalLatency();
double printAverageLatency(double latency, uint32_t num);
double printThroughput(uint32_t num_ops, double latency);
