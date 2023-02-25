#include <iostream>
#include <cstdlib>

#include "bloom_filter.hpp"
#include "data_types.hpp"

BloomFilter::BloomFilter(int capacity, double error_rate, int bitset_size) :
    capacity(capacity), error_rate(error_rate), bitset_size(bitset_size) {
    int num_bits = std::ceil(-(capacity * std::log(error_rate)) / std::log(2) / std::log(2));
    this->num_levels = std::max(1, static_cast<int>(std::floor(std::log2(num_bits))));
    this->bits_per_level = std::ceil((double)num_bits / num_levels);
    int total_bits = num_levels * bits_per_level;
    this->bits = DynamicBitset((this->bitset_size > total_bits) ? this->bitset_size : total_bits);
}


void BloomFilter::add(const KEY_t key) {
    std::hash<KEY_t> hasher;
    int hash_value = hasher(key);
    for (int i = 0; i < this->num_levels; i++) {
        int index =  std::abs((hash_value >> i) % this->bits_per_level + i * this->bits_per_level);
        cout << "Level " + to_string(i) + ": " + to_string(index) << endl;
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

// int main() {
//     BloomFilter bf(1000, 0.01, 10000);
//     bf.add("hello");
//     bf.add("world");
//     std::cout << bf.contains("hello") << std::endl; // prints 1
//     std::cout << bf.contains("foo") << std::endl; // prints 0
//     return 0;
// }
