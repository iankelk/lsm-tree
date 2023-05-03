#pragma once
#include <set>
#include <map>
#include "data_types.hpp"
#include <nlohmann/json.hpp>
using json = nlohmann::json;

class Memtable {
public:
    explicit Memtable(size_t maxKvPairs) : maxKvPairs(maxKvPairs) {}
    ~Memtable() {};

    bool put(KEY_t key, VAL_t value);
    std::unique_ptr<VAL_t> get(KEY_t key) const;
    std::set<kvPair> range(KEY_t start, KEY_t end) const;
    void clear();
    size_t size() const;
    std::set<kvPair> getSet() const; 
    long getMaxKvPairs() const;
    json serialize() const;
    void deserialize(const json& j);

    // Read-only iterators
    std::set<kvPair>::const_iterator begin() const;
    std::set<kvPair>::const_iterator end() const;
    std::set<kvPair>::const_reverse_iterator rbegin() const;

private:
    size_t maxKvPairs;
    std::set<kvPair> table_;  
};