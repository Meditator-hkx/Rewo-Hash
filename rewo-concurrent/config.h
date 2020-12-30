# pragma once

#pragma once

/* persistent mapping file */
#define MAP_PMEM "/home/kaixin/pmdir/pmfile"

/* key type: 0 = integer; 1 = string */
#define KEY_TYPE (1)

/* value type: 0 = integer; 1 = string */
#define VALUE_TYPE (2)

/* NVM access granularity */
#define NVM_BLOCK_SIZE (256)

/* NVM bucket number for initiation */
#define NVM_BUCKET_NUM (1 << 20)

/* maximum DRAM capacity relative to NVM */
#define NVM_TO_DRAM_RATIO (8)

/* a pointed file to export hash table data out to */
#define EXPORT_FILE "kaixin_table.txt"

/* if the DRAM cache should be used (it is enabled by default) */
#define DRAM_CACHE_ENABLE 0

/* read-write concurrency policy type: 0 = bucket version; 1 = slot version; 2 = HTM */
#define READ_WRITE_CONCURRENCY_POLICY 2





