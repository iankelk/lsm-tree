#include <iostream>
#include <cmath>
#include "bloom_filter.hpp"

BloomFilter::BloomFilter(int capacity, double error_rate) :
    capacity(capacity), error_rate(error_rate) {
    if (capacity < 0) {
        throw std::invalid_argument("Capacity must be non-negative.");
    }
    this->num_bits = std::ceil(-(capacity * std::log(error_rate)) / std::log(2) / std::log(2));
    this->num_hashes = std::ceil(std::log(2) * (num_bits / capacity));
    this->bits = DynamicBitset(num_bits);
}

void BloomFilter::add(const KEY_t key) {
    std::hash<KEY_t> hasher;
    int hash_value = hasher(key);
    for (int i = 0; i < this->num_hashes; i++) {
        int index = std::abs(static_cast<int>(hash_value + i * hasher(key))) % this->num_bits;
        this->bits.set(index);
    }
}

bool BloomFilter::contains(const KEY_t key) {
    std::hash<KEY_t> hasher;
    int hash_value = hasher(key);
    for (int i = 0; i < this->num_hashes; i++) {
        int index = std::abs(static_cast<int>(hash_value + i * hasher(key))) % this->num_bits;
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
    j["bits"] = bits.serialize();
    return j;
}

void BloomFilter::deserialize(const json& j) {
    if (!j.contains("capacity") || !j.contains("error_rate") || !j.contains("num_bits") || !j.contains("num_hashes") || !j.contains("bits")) {
        throw std::runtime_error("Invalid JSON format for deserializing BloomFilter");
    }

    BloomFilter(j["capacity"], j["error_rate"]);
    num_bits = j["num_bits"];
    num_hashes = j["num_hashes"];
    bits.deserialize(j["bits"]);
}
