#pragma once

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#define HASH_NUM 2

uint32_t consistent_hash(uint64_t key);
uint32_t consistent_hash(char *key);
uint32_t persistent_hash(uint64_t key, uint32_t &table_index, int hash_method);
uint32_t persistent_hash(char *key, uint32_t &table_index, int hash_method);
uint32_t cache_hash(uint64_t key, int hash_method);
uint32_t cache_hash(char *key, int hash_method);
uint8_t fp_hash(uint64_t key);
uint8_t fp_hash(char *key);
bool fp_match(uint32_t *fp, int i, uint64_t key);
bool fp_match(uint32_t *fp, int i, char *key);
void fp_append(uint32_t *fp, int i, uint64_t key);
void fp_append(uint32_t *fp, int i, char *key);

