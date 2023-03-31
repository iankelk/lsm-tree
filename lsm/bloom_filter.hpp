#ifndef BLOOM_FILTER_HPP
#define BLOOM_FILTER_HPP
#include <boost/dynamic_bitset.hpp>
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
    int getNumBits() { return num_bits; }
    int getNumHashes() { return num_hashes; }
private:
    int capacity;
    double error_rate;
    int num_bits;
    int num_hashes;
    boost::dynamic_bitset<> bits;
};

#endif /* BLOOM_FILTER_HPP */