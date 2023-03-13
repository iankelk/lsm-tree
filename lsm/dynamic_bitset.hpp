#ifndef DYNAMIC_BITSET_HPP
#define DYNAMIC_BITSET_HPP
#include <vector>

class DynamicBitset {
public:
    DynamicBitset() : m_bits() {}
    explicit DynamicBitset(size_t size) : m_bits(size) {}

    void resize(size_t size);
    size_t size();
    void set(size_t pos);
    void reset(size_t pos);
    bool test(size_t pos);

private:
    std::vector<bool> m_bits;
};

#endif /* DYNAMIC_BITSET_HPP */