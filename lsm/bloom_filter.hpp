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
    int getNumBits() { return numBits; }
    int getNumHashes() { return numHashes; }
private:
    int capacity;
    double errorRate;
    int numBits;
    int numHashes;
    boost::dynamic_bitset<> bits;
};

#endif /* BLOOM_FILTER_HPP */