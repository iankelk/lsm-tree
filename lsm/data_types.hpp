/**
 * ================================================
 * = Harvard University | CS265 | Systems Project =
 * ================================================
 * =====     LSM TREE DATA TYPES (Modified)   =====
 * ================================================
 */
#ifndef DATA_TYPES_H
#define DATA_TYPES_H

// KEY
typedef int32_t KEY_t;
#define KEY_MAX 2147483647
#define KEY_MIN -2147483647

// VALUE
typedef int32_t VAL_t;
#define VAL_MAX 2147483647
#define VAL_MIN -2147483647
#define TOMBSTONE -2147483648

#define DEFAULT_FANOUT 2 // 10
#define DEFAULT_NUM_PAGES 100 // 1000
#define DEFAULT_CAPACITY 100
#define DEFAULT_ERROR_RATE 0.05
#define DEFAULT_BITSET_SIZE 250
#define DEFAULT_LEVELING_POLICY Level::LAZY_LEVELED

// FIRST LEVEL DEFINITION
#define FIRST_LEVEL_NUM 1

// SSTABLE DEFINITIONS
#define FILE_DESCRIPTOR_UNINITIALIZED -1
#define SSTABLE_FILE_TEMPLATE "/tmp/lsm-XXXXXX"

// CLIENT / SERVER DEFINITIONS
#define BUFFER_SIZE 2048
#define SERVER_PORT 1234
#define END_OF_MESSAGE "<END_OF_MESSAGE>"
#define NO_VALUE "<NO_VALUE>"

// Key Value Pair
struct kv_pair {
    KEY_t key;
    VAL_t value;
    // Define comparison operators for kv_pair
    bool operator==(const kv_pair& rhs) const {
        return key == rhs.key;
    }
    bool operator!=(const kv_pair& rhs) const {
        return key != rhs.key;
    }
    bool operator<(const kv_pair& rhs) const {
        return key < rhs.key;
    }
    bool operator>(const kv_pair& rhs) const {
        return key > rhs.key;
    }
};

#endif