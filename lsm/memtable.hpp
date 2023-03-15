#ifndef MEMTABLE_HPP
#define MEMTABLE_HPP
#include <map>
#include "data_types.hpp"
#include <nlohmann/json.hpp>
using json = nlohmann:: json;

class Memtable {
public:
    explicit Memtable(int max_kv_pairs) : max_kv_pairs(max_kv_pairs) {}
    ~Memtable() {};

    bool put(KEY_t key, VAL_t value);
    std::unique_ptr<VAL_t> get(KEY_t key) const;
    std::map<KEY_t, VAL_t> range(KEY_t start, KEY_t end) const;
    void clear();
    int size() const;
    std::map<KEY_t, VAL_t> getMap() const;    
    long getMaxKvPairs() const;
    json serialize() const;
    void deserialize(const json& j);
private:
    long max_kv_pairs;
    std::map<KEY_t, VAL_t> table_;  
};

#endif /* MEMTABLE_HPP */
