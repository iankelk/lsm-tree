#include <string>


#include "dynamic_bitset.hpp"

using namespace std;

void DynamicBitset::resize(size_t size) {
    m_bits.resize(size);
}

size_t DynamicBitset::size() {
    return m_bits.size();
}

void DynamicBitset::set(size_t pos) {
    if (pos >= size()) {
        throw out_of_range("set: Bitset index " + to_string(pos) + " out of range for size " + to_string(size()));
    }
    m_bits[pos] = true;
}

void DynamicBitset::reset(size_t pos) {
    if (pos >= size()) {
        throw out_of_range("reset: Bitset index " + to_string(pos) + " out of range for size " + to_string(size()));
    }
    m_bits[pos] = false;
}

bool DynamicBitset::test(size_t pos) {
    if (pos >= size()) {
        throw out_of_range("test: Bitset index " + to_string(pos) + " out of range for size " + to_string(size()));
    }
    return m_bits[pos];
}

