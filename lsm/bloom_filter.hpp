#ifndef BLOOM_FILTER_HPP
#define BLOOM_FILTER_HPP
#include "dynamic_bitset.hpp"
#include "data_types.hpp"
#include <nlohmann/json.hpp>
using json = nlohmann:: json;

class BloomFilter {
public:
    BloomFilter(int capacity, double error_rate, int bitset_size);

    void add(const KEY_t key);
    bool contains(const KEY_t key);
    json serialize() const;
    void deserialize(const json& j);


private:
    int capacity;
    double error_rate;
    int bitset_size;
    int num_levels;
    int bits_per_level;
    DynamicBitset bits;
    std::hash<std::string> hasher;
};

#endif /* BLOOM_FILTER_HPP */
