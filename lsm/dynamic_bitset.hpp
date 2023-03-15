#ifndef DYNAMIC_BITSET_HPP
#define DYNAMIC_BITSET_HPP
#include <vector>
#include <nlohmann/json.hpp>
using json = nlohmann:: json;

class DynamicBitset {
public:
    DynamicBitset() : m_bits() {}
    explicit DynamicBitset(size_t size) : m_bits(size) {}

    void resize(size_t size);
    size_t size();
    void set(size_t pos);
    void reset(size_t pos);
    bool test(size_t pos);
    json serialize() const;
    void deserialize(const json& j);

private:
    std::vector<bool> m_bits;
};

#endif /* DYNAMIC_BITSET_HPP */