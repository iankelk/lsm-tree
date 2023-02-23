#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H

#include <cmath>
#include <functional>
#include <string>

#include "dynamic_bitset.hpp"

class BloomFilter {
public:
    BloomFilter(int capacity, double error_rate, int bitset_size);

    void add(const std::string& key);
    bool contains(const std::string& key);

private:
    int capacity;
    double error_rate;
    int bitset_size;
    int num_levels;
    int bits_per_level;
    DynamicBitset bits;
    std::hash<std::string> hasher;
};

#endif /* BLOOM_FILTER_H */
