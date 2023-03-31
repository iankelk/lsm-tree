#include <string>
#include "dynamic_bitset.hpp"

void DynamicBitset::resize(size_t size) {
    m_bits.resize(size);
}

size_t DynamicBitset::size() const {
    return m_bits.size();
}

void DynamicBitset::set(size_t pos) {
    if (pos >= size()) {
        throw std::out_of_range("set: Bitset index " + std::to_string(pos) + " out of range for size " + std::to_string(size()));
    }
    m_bits[pos] = true;
}

void DynamicBitset::reset(size_t pos) {
    if (pos >= size()) {
        throw std::out_of_range("reset: Bitset index " + std::to_string(pos) + " out of range for size " + std::to_string(size()));
    }
    m_bits[pos] = false;
}

bool DynamicBitset::test(size_t pos) {
    if (pos >= size()) {
        throw std::out_of_range("test: Bitset index " + std::to_string(pos) + " out of range for size " + std::to_string(size()));
    }
    return m_bits[pos];
}

json DynamicBitset::serialize() const {
    json j;
    std::vector<int> bits_as_ints;
    bits_as_ints.reserve(m_bits.size());
    for (const auto& bit : m_bits) {
        bits_as_ints.push_back(bit ? 1 : 0);
    }
    j["bits"] = bits_as_ints;
    return j;
}

void DynamicBitset::deserialize(const json& j) {
    if (!j.contains("bits") || !j["bits"].is_array()) {
        throw std::runtime_error("Invalid JSON format for deserializing DynamicBitset");
    }

    std::vector<int> bits_as_ints = j["bits"];
    resize(bits_as_ints.size());
    
    for (size_t i = 0; i < bits_as_ints.size(); ++i) {
        if (bits_as_ints[i] == 1) {
            set(i);
        } else if (bits_as_ints[i] != 0) {
            throw std::runtime_error("Invalid bit value encountered during deserialization");
        }
    }
}
