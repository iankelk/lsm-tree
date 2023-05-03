#pragma once
#include <string>
#include <map>

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
constexpr size_t DEFAULT_FANOUT = 10;
constexpr size_t DEFAULT_NUM_PAGES = 128;
constexpr double DEFAULT_ERROR_RATE = 0.01;
#define DEFAULT_LEVELING_POLICY Level::TIERED
constexpr size_t DEFAULT_NUM_THREADS = 10;
constexpr double DEFAULT_COMPACTION_PERCENTAGE = 0.2;
const std::string DEFAULT_DATA_DIRECTORY = "data";
constexpr bool DEFAULT_VERBOSE_PRINTING = false;
constexpr size_t DEFAULT_VERBOSE_FREQUENCY = 100000;
constexpr bool DEFAULT_THROUGHPUT_PRINTING = false;
constexpr size_t DEFAULT_THROUGHPUT_FREQUENCY = 1000000;

// LSM TREE DEFINITIONS
constexpr int STATS_PRINT_EVERYTHING = -1;
constexpr int NUM_LOGICAL_PAIRS_NOT_CACHED = -1;

// BLOOM FILTER DEFINITIONS
constexpr float BLOOM_FILTER_UNUSED = -1.0f;

// FILE DEFINITIONS
const std::string LSM_TREE_JSON_FILE = "lsm-tree.json";
const std::string SSTABLE_FILE_TEMPLATE = "lsm-XXXXXX.bin";

// DISK DEFINITIONS
constexpr int NUM_DISK_TYPES = 5;
const std::string DISK1_NAME = "SSD";
const std::string DISK2_NAME = "HDD1";
const std::string DISK3_NAME = "HDD2";
const std::string DISK4_NAME = "HDD3";
const std::string DISK5_NAME = "HDD4";

constexpr int DISK1_PENALTY_MULTIPLIER = 1;
constexpr int DISK2_PENALTY_MULTIPLIER = 5;
constexpr int DISK3_PENALTY_MULTIPLIER = 15;
constexpr int DISK4_PENALTY_MULTIPLIER = 45;
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

// Helper structure for priority queues
struct PQEntry {
    KEY_t key;
    VAL_t value;
    size_t runIdx;
    typename std::vector<kvPair>::iterator vecIter;

    bool operator<(const PQEntry& other) const {
        return key > other.key; // Min heap based on key
    }
};

template <typename T>
KEY_t get_key(const T& it) {
    if constexpr (std::is_same_v<T, std::map<KEY_t, VAL_t>::const_iterator>) {
        return it->first;
    } else {
        return it->key;
    }
}

template <typename T>
VAL_t get_value(const T& it) {
    if constexpr (std::is_same_v<T, std::map<KEY_t, VAL_t>::const_iterator>) {
        return it->second;
    } else {
        return it->value;
    }
}

template<typename InputIterator>
std::vector<kvPair> get_kv_buffer(InputIterator begin, InputIterator end) {
    if constexpr (std::is_same_v<typename std::iterator_traits<InputIterator>::value_type, kvPair>) {
        // If the iterator points to kvPair, it's already a vector<kvPair>, so we can just use a reference to the original vector
        return std::vector<kvPair>(begin, end);
    } else {
        // If the iterator points to a map, we need to create a new vector<kvPair>
        std::vector<kvPair> kvBuffer;
        kvBuffer.reserve(std::distance(begin, end));

        for (auto it = begin; it != end; ++it) {
            kvBuffer.push_back({get_key(it), get_value(it)});
        }

        return kvBuffer;
    }
}
