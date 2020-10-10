#pragma once

#include <thread>
#include <iostream>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <thread>
#include <string.h>
#include <iostream>
#include <fstream>

#include "storage.h"
#include "hash.h"
#include "trxn.h"
#include "rewolock.h"
#include "factory.h"


int rewo_search(uint64_t key, char *value);
int rewo_insert(uint64_t key, char *value);
int rewo_update(uint64_t key, char *value);
int rewo_delete(uint64_t key);
int rewo_search(char *key, char *value);
int rewo_insert(char *key, char *value);
int rewo_update(char *key, char *value);
int rewo_delete(char *key);
void rewo_init(int ntype);
void rewo_clear();
void rewo_exit();
void rewo_resize();
void rewo_recover();
int rewo_export();
uint32_t rewo_get_kv_num();

int pm_search(uint64_t key, char *value);
int pm_insert(uint64_t key, char *value);
int pm_update(uint64_t key, char *value);
int pm_delete(uint64_t key);
int pm_search(char *key, char *value);
int pm_insert(char *key, char *value);
int pm_update(char *key, char *value);
int pm_delete(char *key);

int cache_search(uint64_t key, char *value);
int cache_insert(uint64_t key, char *value);
int cache_update(uint64_t key, char *value);
int cache_delete(uint64_t key);
void cache_replace(uint64_t key, char *value);
int cache_search(char *key, char *value);
int cache_insert(char *key, char *value);
int cache_update(char *key, char *value);
int cache_delete(char *key);
void cache_replace(char *key, char *value);

void back_execute();
void thread_execute();
int  *sync_buffer(uint64_t key, char *value, int flag);
int  *sync_buffer(char *key, char *value, int flag);
int sync_release(int *sync);
