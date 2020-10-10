#pragma once

#include "storage.h"
#include "hash.h"
#include <thread>

int rewo_search(int order, uint64_t key, char *value);
int rewo_insert(int order, uint64_t key, char *value);
int rewo_update(int order, uint64_t key, char *value);
int rewo_delete(int order, uint64_t key);
int rewo_search(int order, char *key, char *value);
int rewo_insert(int order, char *key, char *value);
int rewo_update(int order, char *key, char *value);
int rewo_delete(int order, char *key);
void rewo_init(int ntype);
void rewo_exit();
void rewo_resize();
void rewo_recover();
void rewo_clear();
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
int cache_insert(uint64_t key, char *value, uint64_t &lockmap_addr);
int cache_update(uint64_t key, char *value, uint64_t &lockmap_addr);
int cache_delete(uint64_t key, uint64_t &lockmap_addr);
void cache_replace(uint64_t key, char *value);
int cache_search(char *key, char *value);
int cache_insert(char *key, char *value);
int cache_insert(char *key, char *value, uint64_t &lockmap_addr);
int cache_update(char *key, char *value, uint64_t &lockmap_addr);
int cache_delete(char *key, uint64_t &lockmap_addr);
void cache_replace(uint64_t key, char *value);
void cache_replace(char *key, char *value);

void back_execute();
void thread_execute(int order);
buffer_tuple *sync_buffer(int order, uint64_t key, char *value, int flag);
buffer_tuple *sync_buffer(int order, char *key, char *value, int flag);
int sync_release(buffer_tuple *sync_tuple);
