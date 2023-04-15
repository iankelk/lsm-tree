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

// VALUES
typedef int32_t VAL_t;
#define VAL_MAX 2147483647
#define VAL_MIN -2147483647
#define TOMBSTONE -2147483648

// DEFAULT LSM TREE PARAMETERS
#define DEFAULT_FANOUT 10 // 10
#define DEFAULT_NUM_PAGES 512 // 512
#define DEFAULT_ERROR_RATE 0.00001
#define DEFAULT_LEVELING_POLICY Level::LAZY_LEVELED
#define DEFAULT_VERBOSE_LEVEL false
#define DEFAULT_NUM_THREADS 10

// LSM TREE DEFINITIONS
#define STATS_PRINT_EVERYTHING -1
#define NUM_LOGICAL_PAIRS_NOT_CACHED -1

// LSM TREE PARAMETERS
#define BENCHMARK_REPORT_FREQUENCY 100000

// BLOOM FILTER DEFINITIONS
#define BLOOM_FILTER_UNUSED -1.0f

// FILE DEFINITIONS
#define DATA_DIRECTORY "data/"
#define LSM_TREE_JSON_FILE "lsm-tree.json"
#define SSTABLE_FILE_TEMPLATE "lsm-XXXXXX.bin"

// DISK DEFINITIONS
#define NUM_DISK_TYPES 5
#define DISK1_NAME "SSD"
#define DISK1_PENALTY_MULTIPLIER 1
#define DISK2_NAME "HDD1"
#define DISK2_PENALTY_MULTIPLIER 5
#define DISK3_NAME "HDD2"
#define DISK3_PENALTY_MULTIPLIER 15
#define DISK4_NAME "HDD3"
#define DISK4_PENALTY_MULTIPLIER 45
#define DISK5_NAME "HDD4"
#define DISK5_PENALTY_MULTIPLIER 135

// FIRST LEVEL DEFINITION
#define FIRST_LEVEL_NUM 1

// RUN DEFINITIONS
#define FILE_DESCRIPTOR_UNINITIALIZED -1

// COMPACTION DEFINITIONS
#define COMPACTION_PLAN_NOT_SET -1

// CLIENT / SERVER DEFINITIONS
#define BUFFER_SIZE 4096
#define DEFAULT_SERVER_PORT 1234
#define END_OF_MESSAGE "<END_OF_MESSAGE>"
#define NO_VALUE "<NO_VALUE>"
#define OK "<OK>"
const std::string SERVER_SHUTDOWN = "<SERVER_SHUTDOWN>";

// KEY-VALUE PAIR
struct kvPair {
    KEY_t key;
    VAL_t value;
};

#endif /* DATA_TYPES_H */