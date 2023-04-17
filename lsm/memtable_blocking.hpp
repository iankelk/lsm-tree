#ifndef MEMTABLE_BLOCKING_HPP
#define MEMTABLE_BLOCKING_HPP
#include <map>
#include <shared_mutex>
#include <nlohmann/json.hpp>
#include "data_types.hpp"
#include "memtable_base.hpp"
using json = nlohmann:: json;

class MemtableBlocking : public MemtableBase {
public:
    explicit MemtableBlocking(int maxKvPairs) : MemtableBase(maxKvPairs) {}
    ~MemtableBlocking() {};

    bool put(KEY_t key, VAL_t value);
    std::unique_ptr<VAL_t> get(KEY_t key) const;
    std::map<KEY_t, VAL_t> range(KEY_t start, KEY_t end) const;
    void clear();
    int size() const;
    std::map<KEY_t, VAL_t> getMap() const;    
    json serialize() const;
    void deserialize(const json& j);
    bool clearAndPut(KEY_t key, VAL_t value);
private:
    std::map<KEY_t, VAL_t> table_;
    mutable std::shared_mutex mtx_;
};

#endif /* MEMTABLE_BLOCKING_HPP */
