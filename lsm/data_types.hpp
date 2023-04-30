#pragma once
#include <string>

// KEY
using KEY_t = int32_t;
constexpr int32_t KEY_MAX = 2147483647;
constexpr int32_t KEY_MIN = -2147483647;

// VALUES
using VAL_t = int32_t;
constexpr int32_t VAL_MAX = 2147483647;
constexpr int32_t VAL_MIN = -2147483647;
constexpr int32_t TOMBSTONE = -2147483648;

// DEFAULT LSM TREE PARAMETERS
constexpr int DEFAULT_FANOUT = 10;
constexpr int DEFAULT_NUM_PAGES = 128;
constexpr double DEFAULT_ERROR_RATE = 0.01;
#define DEFAULT_LEVELING_POLICY Level::TIERED
constexpr bool DEFAULT_VERBOSE_LEVEL = false;
constexpr int DEFAULT_NUM_THREADS = 10;
constexpr double DEFAULT_COMPACTION_PERCENTAGE = 0.2;
const std::string DEFAULT_DATA_DIRECTORY = "data";

// LSM TREE DEFINITIONS
constexpr int STATS_PRINT_EVERYTHING = -1;
constexpr int NUM_LOGICAL_PAIRS_NOT_CACHED = -1;

// LSM TREE PARAMETERS
constexpr int BENCHMARK_REPORT_FREQUENCY = 100000;

// BLOOM FILTER DEFINITIONS
constexpr float BLOOM_FILTER_UNUSED = -1.0f;

// FILE DEFINITIONS
const std::string LSM_TREE_JSON_FILE = "lsm-tree.json";
const std::string SSTABLE_FILE_TEMPLATE = "lsm-XXXXXX.bin";

// DISK DEFINITIONS
constexpr int NUM_DISK_TYPES = 5;
#define DISK1_NAME "SSD"
constexpr int DISK1_PENALTY_MULTIPLIER = 1;
#define DISK2_NAME "HDD1"
constexpr int DISK2_PENALTY_MULTIPLIER = 5;
#define DISK3_NAME "HDD2"
constexpr int DISK3_PENALTY_MULTIPLIER = 15;
#define DISK4_NAME "HDD3"
constexpr int DISK4_PENALTY_MULTIPLIER = 45;
#define DISK5_NAME "HDD4"
constexpr int DISK5_PENALTY_MULTIPLIER = 135;

// FIRST LEVEL DEFINITION
constexpr int FIRST_LEVEL_NUM = 1;

// RUN DEFINITIONS
constexpr int FILE_DESCRIPTOR_UNINITIALIZED = -1;

// CLIENT / SERVER DEFINITIONS
constexpr int BUFFER_SIZE = 4096;
constexpr int DEFAULT_SERVER_PORT = 1234;
const std::string END_OF_MESSAGE = "<END_OF_MESSAGE>";
const std::string NO_VALUE = "<NO_VALUE>";
const std::string OK = "<OK>";
const std::string SERVER_SHUTDOWN = "<SERVER_SHUTDOWN>";

// KEY-VALUE PAIR
struct kvPair {
    KEY_t key;
    VAL_t value;
};
