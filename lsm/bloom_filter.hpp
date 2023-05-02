#pragma once
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
    void resize(size_t newNumBits);
    double theoreticalErrorRate() const;

private:
    size_t capacity;
    double errorRate;
    size_t numBits;
    int numHashes;
    boost::dynamic_bitset<> bits;
};
