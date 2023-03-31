/**
 * ================================================
 * = Harvard University | CS265 | Systems Project =
 * ================================================
 * =====     LSM TREE DATA TYPES (Modified)   =====
 * ================================================
 */
#ifndef DATA_TYPES_H
#define DATA_TYPES_H
#include <string>

// KEY
typedef int32_t KEY_t;
#define KEY_MAX 2147483647
#define KEY_MIN -2147483647

// VALUE
typedef int32_t VAL_t;
#define VAL_MAX 2147483647
#define VAL_MIN -2147483647
#define TOMBSTONE -2147483648

#define DEFAULT_FANOUT 10 // 10
#define DEFAULT_NUM_PAGES 512 // 512
#define DEFAULT_ERROR_RATE 0.00001
#define DEFAULT_LEVELING_POLICY Level::LAZY_LEVELED
#define DEFAULT_VERBOSE_LEVEL false

// BLOOM FILTER DEFINITIONS
#define BLOOM_FILTER_UNUSED -1.0f

// LSM TREE DEFINITIONS
#define LSM_TREE_FILE "/tmp/lsm-tree.json"
#define BENCHMARK_REPORT_FREQUENCY 100000

// FIRST LEVEL DEFINITION
#define FIRST_LEVEL_NUM 1

// SSTABLE DEFINITIONS
#define FILE_DESCRIPTOR_UNINITIALIZED -1
#define SSTABLE_FILE_TEMPLATE "/tmp/lsm-XXXXXX"

// CLIENT / SERVER DEFINITIONS
#define BUFFER_SIZE 2048
#define DEFAULT_SERVER_PORT 1234
#define END_OF_MESSAGE "<END_OF_MESSAGE>"
#define NO_VALUE "<NO_VALUE>"
#define OK "<OK>"

// Key Value Pair
struct kv_pair {
    KEY_t key;
    VAL_t value;
};

#endif /* DATA_TYPES_H */