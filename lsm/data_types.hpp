/**
 * ================================================
 * = Harvard University | CS265 | Systems Project =
 * ================================================
 * =====     LSM TREE DATA TYPES (Modified)   =====
 * ================================================
 */

#include <stdint.h>
#include <stdlib.h>

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

#define DEFAULT_FANOUT 10
#define DEFAULT_NUM_PAGES 1000
#define DEFAULT_CAPACITY 1000
#define DEFAULT_ERROR_RATE 0.01
#define DEFAULT_BITSET_SIZE 10000
#define DEFAULT_LEVELING_POLICY false

// FIRST LEVEL DEFINITION
#define FIRST_LEVEL_NUM 1

// SSTABLE DEFINITIONS
#define FILE_DESCRIPTOR_UNINITIALIZED -1
#define SSTABLE_FILE_TEMPLATE "/tmp/lsm-XXXXXX"

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