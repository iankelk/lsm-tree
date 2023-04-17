#ifndef MEMTABLE_CONCURRENT_HPP
#define MEMTABLE_CONCURRENT_HPP
#include <map>
#include <tbb/concurrent_map.h>
#include <nlohmann/json.hpp>
#include "data_types.hpp"
#include "memtable_base.hpp"
using json = nlohmann:: json;

class MemtableConcurrent : public MemtableBase {
public:
    explicit MemtableConcurrent(int maxKvPairs) : MemtableBase(maxKvPairs) {}
    ~MemtableConcurrent() {};

    bool put(KEY_t key, VAL_t value);
    std::unique_ptr<VAL_t> get(KEY_t key) const;
    std::map<KEY_t, VAL_t> range(KEY_t start, KEY_t end) const;
    void clear();
    int size() const;
    std::map<KEY_t, VAL_t> getMap() const;    
    json serialize() const;
    void deserialize(const json& j);
private:
    tbb::concurrent_map<KEY_t, VAL_t> table_;  
};

#endif /* MEMTABLE_CONCURRENT_HPP */
