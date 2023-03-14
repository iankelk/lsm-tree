#include <iostream>
#include <cmath>
#include "bloom_filter.hpp"
#include "data_types.hpp"

BloomFilter::BloomFilter(int capacity, double error_rate, int bitset_size) :
    error_rate(error_rate), bitset_size(bitset_size) {
    if (capacity < 0) {
        throw std::invalid_argument("Capacity must be non-negative.");
    }
    this->capacity = capacity;
    int num_bits = std::ceil(-(capacity * std::log(error_rate)) / std::log(2) / std::log(2));
    this->num_levels = std::max(1, static_cast<int>(std::floor(std::log2(num_bits))));
    this->bits_per_level = std::ceil((double)num_bits / num_levels);
    int total_bits = num_levels * bits_per_level;
    this->bits = DynamicBitset((this->bitset_size > total_bits) ? this->bitset_size : total_bits);
    std::cout << "Bloom filter: " << num_bits << " bits, " << num_levels << " levels, " << bits_per_level << " bits per level" << std::endl;
    std::cout << "Bloom filter: " << this->bits.size() << " bits total" << std::endl;
}


void BloomFilter::add(const KEY_t key) {
    std::hash<KEY_t> hasher;
    int hash_value = hasher(key);
    for (int i = 0; i < this->num_levels; i++) {
        int index =  std::abs((hash_value >> i) % this->bits_per_level + i * this->bits_per_level);
        //cout << "Level " + to_string(i) + ": " + to_string(index) << endl;
        this->bits.set(index);
    }
}

bool BloomFilter::contains(const KEY_t key) {
    std::hash<KEY_t> hasher;
    int hash_value = hasher(key);
    for (int i = 0; i < this->num_levels; i++) {
        int index = std::abs((hash_value >> i) % this->bits_per_level + i * this->bits_per_level);
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
    j["bitset_size"] = bitset_size;
    j["num_levels"] = num_levels;
    j["bits_per_level"] = bits_per_level;
    j["bits"] = bits.serialize();
    return j;
}