#ifndef MEMTABLE_BASE_HPP
#define MEMTABLE_BASE_HPP

#include <map>
#include "data_types.hpp"
#include <nlohmann/json.hpp>
using json = nlohmann:: json;

class MemtableBase {
public:
    explicit MemtableBase(int maxKvPairs) : maxKvPairs(maxKvPairs) {}
    virtual ~MemtableBase() {};

    virtual bool put(KEY_t key, VAL_t value) = 0;
    virtual std::unique_ptr<VAL_t> get(KEY_t key) const = 0;
    virtual std::map<KEY_t, VAL_t> range(KEY_t start, KEY_t end) const = 0;
    virtual void clear() = 0;
    virtual int size() const = 0;
    virtual std::map<KEY_t, VAL_t> getMap() const = 0;
    virtual json serialize() const = 0;
    virtual void deserialize(const json& j) = 0;
    long getMaxKvPairs() const;
protected:
    long maxKvPairs;
};

#endif /* MEMTABLE_BASE_HPP */
