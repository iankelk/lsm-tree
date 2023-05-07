#include <bitset>
#include <cmath>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>
#include "xxhash.h"

class BloomFilter {
public:
    // BloomFilter(size_t m, size_t k);
    BloomFilter(std::vector<uint8_t> buf, size_t k);
    BloomFilter(size_t capacity, double errorRate);

    void insert(const void* data, size_t len);
    bool contains(const void* data, size_t len);

    void resize(size_t new_m);
    std::vector<uint8_t> serialize() const;
    static BloomFilter deserialize(const std::vector<uint8_t>& data);

    size_t length() const { return m; }
    size_t numHashFunctions() const { return k; }
    const std::vector<uint8_t>& buffer() const { return buf; }
    size_t getNumBits() const { return m; }
    void setNumBits(size_t numBits);

private:
    size_t m;
    size_t k;
    size_t mask;
    size_t capacity;
    double errorRate;
    std::vector<uint8_t> buf;

    size_t location(uint64_t h1, uint64_t h2, size_t i) const;
    std::array<uint64_t, 2> hash(const void* data, size_t len) const;
    static size_t nextPow2(size_t v);
};