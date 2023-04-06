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
    long long getNumBits() { return numBits; }
    void setNumBits(long long numBits) { this->numBits = numBits; } 
    int getNumHashes() { return numHashes; }
    void clear();
    void resize(long long numBits);

private:
    int capacity;
    double errorRate;
    long long numBits;
    int numHashes;
    boost::dynamic_bitset<> bits;
};

#endif /* BLOOM_FILTER_HPP */