#include <iostream>
#include <cmath>
#include <sstream>
#include "../lib/xxhash.h"
#include "bloom_filter.hpp"
#include "utils.hpp"

BloomFilter::BloomFilter(int capacity, double errorRate) :
    capacity(capacity), errorRate(errorRate),
    numBits(std::ceil(-(capacity * std::log(errorRate)) / std::log(2) / std::log(2))),
    numHashes(std::ceil(std::log(2) * (numBits / capacity))),
    bits(numBits) {
        if (capacity < 0) {
            die("BloomFilter::Constructor: Capacity must be non-negative.");
    }
}

void BloomFilter::add(const KEY_t key) {
    XXH128_hash_t hash = XXH3_128bits(static_cast<const void*>(&key), sizeof(KEY_t));
    uint64_t hash1 = hash.high64;
    uint64_t hash2 = hash.low64;

    for (int i = 0; i < this->numHashes; i++) {
        int index = std::abs(static_cast<int>((hash1 + i * hash2) % this->numBits));
        this->bits.set(index);
    }
}

bool BloomFilter::contains(const KEY_t key) {
    XXH128_hash_t hash = XXH3_128bits(static_cast<const void*>(&key), sizeof(KEY_t));
    uint64_t hash1 = hash.high64;
    uint64_t hash2 = hash.low64;

    for (int i = 0; i < this->numHashes; i++) {
        int index = std::abs(static_cast<int>((hash1 + i * hash2) % this->numBits));
        if (!this->bits.test(index)) {
            return false;
        }
    }
    return true;
}

json BloomFilter::serialize() const {
    json j;
    j["capacity"] = capacity;
    j["errorRate"] = errorRate;
    j["numBits"] = numBits;
    j["numHashes"] = numHashes;

    std::stringstream ss;
    ss << bits;
    j["bits"] = ss.str();

    return j;
}

void BloomFilter::deserialize(const json& j) {
    if (!j.contains("capacity") || !j.contains("errorRate") || !j.contains("numBits") || !j.contains("numHashes") || !j.contains("bits")) {
        std::cerr << "BloomFilter::deserialize: Invalid JSON format for deserializing BloomFilter. Skipping..." << std::endl;
        return;
    }
    capacity = j["capacity"];
    errorRate = j["errorRate"];
    numBits = j["numBits"];
    numHashes = j["numHashes"];
    bits = boost::dynamic_bitset<>(j["bits"].get<std::string>());
}