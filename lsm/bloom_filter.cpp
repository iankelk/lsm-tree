#include <iostream>
#include <cmath>
#include <sstream>
#include "../lib/xxhash.h"
#include "bloom_filter.hpp"

BloomFilter::BloomFilter(int capacity, double error_rate) :
    capacity(capacity), error_rate(error_rate),
    num_bits(std::ceil(-(capacity * std::log(error_rate)) / std::log(2) / std::log(2))),
    num_hashes(std::ceil(std::log(2) * (num_bits / capacity))),
    bits(num_bits) {
        if (capacity < 0) {
            throw std::invalid_argument("Capacity must be non-negative.");
    }
}

void BloomFilter::add(const KEY_t key) {
    XXH128_hash_t hash = XXH3_128bits(static_cast<const void*>(&key), sizeof(KEY_t));
    uint64_t hash1 = hash.high64;
    uint64_t hash2 = hash.low64;

    for (int i = 0; i < this->num_hashes; i++) {
        int index = std::abs(static_cast<int>((hash1 + i * hash2) % this->num_bits));
        this->bits.set(index);
    }
}

bool BloomFilter::contains(const KEY_t key) {
    XXH128_hash_t hash = XXH3_128bits(static_cast<const void*>(&key), sizeof(KEY_t));
    uint64_t hash1 = hash.high64;
    uint64_t hash2 = hash.low64;

    for (int i = 0; i < this->num_hashes; i++) {
        int index = std::abs(static_cast<int>((hash1 + i * hash2) % this->num_bits));
        if (!this->bits.test(index)) {
            return false;
        }
    }
    return true;
}

json BloomFilter::serialize() const {
    json j;
    j["capacity"] = capacity;
    j["error_rate"] = error_rate;
    j["num_bits"] = num_bits;
    j["num_hashes"] = num_hashes;

    std::stringstream ss;
    ss << bits;
    j["bits"] = ss.str();

    return j;
}

void BloomFilter::deserialize(const json& j) {
    if (!j.contains("capacity") || !j.contains("error_rate") || !j.contains("num_bits") || !j.contains("num_hashes") || !j.contains("bits")) {
        throw std::runtime_error("Invalid JSON format for deserializing BloomFilter");
    }
    capacity = j["capacity"];
    error_rate = j["error_rate"];
    num_bits = j["num_bits"];
    num_hashes = j["num_hashes"];
    bits = boost::dynamic_bitset<>(j["bits"].get<std::string>());
}