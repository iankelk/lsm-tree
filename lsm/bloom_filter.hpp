#ifndef BLOOM_FILTER_HPP
#define BLOOM_FILTER_HPP
#include <boost/dynamic_bitset.hpp>
#include "data_types.hpp"
#include <nlohmann/json.hpp>
using json = nlohmann:: json;

class BloomFilter {
public:
    BloomFilter(size_t capacity, double error_rate);

    void add(const KEY_t key);
    bool contains(const KEY_t key);
    json serialize() const;
    void deserialize(const json& j);
    size_t getNumBits() { return numBits; }
    void setNumBits(size_t numBits) { this->numBits = numBits; } 
    int getNumHashes() { return numHashes; }
    void resetBitset();
    void resize(size_t newNumBits);

private:
    size_t capacity;
    double errorRate;
    size_t numBits;
    int numHashes;
    boost::dynamic_bitset<> bits;
};

#endif /* BLOOM_FILTER_HPP */