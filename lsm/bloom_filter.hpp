#ifndef BLOOM_FILTER_HPP
#define BLOOM_FILTER_HPP
#include "dynamic_bitset.hpp"
#include "data_types.hpp"
#include <nlohmann/json.hpp>
using json = nlohmann:: json;

class BloomFilter {
public:
    BloomFilter(int capacity, double error_rate);

    void add(const KEY_t key);
    bool contains(const KEY_t key);
    json serialize() const;
    void deserialize(const json& j);

private:
    int capacity;
    double error_rate;
    int num_bits;
    int num_hashes;
    DynamicBitset bits;
};

#endif /* BLOOM_FILTER_HPP */
