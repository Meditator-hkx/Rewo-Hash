#pragma once

/* persistent mapping file */
#define MAP_PMEM "/home/kaixin/pmdir/pmfile"

/* key type: 1 = integer; 2 = string */
#define KEY_TYPE (2)

/* value type: 1 = integer; 2 = string */
#define VALUE_TYPE (2)

/* NVM access granularity */
#define NVM_BLOCK_SIZE (256)

/* NVM bucket number for initiation */
#define NVM_BUCKET_NUM (1 << 20)

/* maximum DRAM capacity (MB) for hash table */
#define DRAM_LIMIT_CAPACITY (16 * 1024)

/* maximum DRAM capacity relative to NVM */
#define NVM_TO_DRAM_RATIO (8)

/* a pointed file to export hash table data out to */
#define EXPORT_FILE "kaixin_table.txt"

/* if the DRAM cache should be used (it is enabled by default) */
#define DRAM_CACHE_ENABLE 1











