#include "bloom_filter.hpp"


BloomFilter::BloomFilter(size_t capacity, double errorRate) :
    capacity(capacity), errorRate(errorRate),
    m(nextPow2(std::ceil(-(capacity * std::log(errorRate)) / std::log(2) / std::log(2)))),
    k(std::ceil(std::log(2) * (m / capacity))),
    mask(m - 1)
    {
        buf.resize(m >> 3);
    }

// BloomFilter::BloomFilter(size_t m, size_t k)
//     : k(k), m(nextPow2(m)), mask(m - 1) {
//     buf.resize(m >> 3);
// }

BloomFilter::BloomFilter(std::vector<uint8_t> buf, size_t k)
    : k(k), m(nextPow2(buf.size() * 8)), mask(m - 1), buf(std::move(buf)) {
    if (m != this->buf.size() * 8) {
        throw std::runtime_error("BloomFilter: buffer bit count must be a power of two");
    }
}

void BloomFilter::insert(const void* data, size_t len) {
    auto h = hash(data, len);
    for (size_t i = 0; i < k; ++i) {
        size_t loc = location(h[0], h[1], i);
        buf[loc >> 3] |= 1 << (loc & 7);
    }
}

bool BloomFilter::contains(const void* data, size_t len) {
    auto h = hash(data, len);
    for (size_t i = 0; i < k; ++i) {
        size_t loc = location(h[0], h[1], i);
        if ((buf[loc >> 3] & (1 << (loc & 7))) == 0) {
            return false;
        }
    }
    return true;
}

size_t BloomFilter::location(uint64_t h1, uint64_t h2, size_t i) const {
    return static_cast<size_t>((h1 + h2 * i) & mask);
}

// std::array<uint64_t, 2> BloomFilter::hash(const void* data, size_t len) const {
//     // Two different large prime numbers as seeds
//     uint64_t seed1 = 0x9e3779b1;
//     uint64_t seed2 = 0x85ebca77;
    
//     uint64_t v1 = XXH64(data, len, seed1);
//     uint64_t v2 = XXH64(data, len, seed2);
//     return {v1, v2};
// }

std::array<uint64_t, 2> BloomFilter::hash(const void* data, size_t len) const {
    uint64_t output[2];
    MurmurHash3_x64_128(data, len, 0, output);
    return {output[0], output[1]};
}



size_t BloomFilter::nextPow2(size_t v) {
    for (size_t i = 8; i < (1ull << 62); i *= 2) {
        if (i >= v) {
            return i;
        }
    }
    throw std::runtime_error("unreachable");
}

void BloomFilter::resize(size_t new_m) {
    new_m = nextPow2(new_m);
    if (new_m != m) {
        m = new_m;
        mask = m - 1;
        buf.resize(m >> 3);
    }
}

void BloomFilter::setNumBits(size_t numBits) {
    size_t new_m = nextPow2(numBits);
    if (new_m != m) {
        m = new_m;
        mask = m - 1;
        buf.resize(m >> 3);
    }
}

double theoreticalFalsePositiveRate(size_t m, size_t k, size_t n) {
    return std::pow(1 - std::exp(-static_cast<double>(k * n) / m), k);
}


std::vector<uint8_t> BloomFilter::serialize() const {
    std::vector<uint8_t> data;
    data.reserve((m >> 3) + sizeof(size_t) * 2);
    data.insert(data.end(), reinterpret_cast<const uint8_t*>(&m), reinterpret_cast<const uint8_t*>(&m) + sizeof(size_t));
    data.insert(data.end(), reinterpret_cast<const uint8_t*>(&k), reinterpret_cast<const uint8_t*>(&k) + sizeof(size_t));
    data.insert(data.end(), buf.begin(), buf.end());
    return data;
}

BloomFilter BloomFilter::deserialize(const std::vector<uint8_t>& data) {
    if (data.size() < sizeof(size_t) * 2) {
        throw std::runtime_error("BloomFilter: insufficient data for deserialization");
    }

    size_t m, k;
    memcpy(&m, data.data(), sizeof(size_t));
    memcpy(&k, data.data() + sizeof(size_t), sizeof(size_t));

    std::vector<uint8_t> buf(data.begin() + sizeof(size_t) * 2, data.end());
    return BloomFilter(std::move(buf), k);
}