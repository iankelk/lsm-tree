/**
 * ================================================
 * = Harvard University | CS265 | Systems Project =
 * ================================================
 * =====     LSM TREE DATA TYPES (Modified)   =====
 * ================================================
 */

#include <stdint.h>

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

// SCAN PATTERNS
#define PUT_PATTERN_SCAN "%d %d"
#define GET_PATTERN_SCAN "%d"
#define RANGE_PATTERN_SCAN "%d %d"
#define DELETE_PATTERN_SCAN "%d"

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