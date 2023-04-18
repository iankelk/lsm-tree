#include "memtable_concurrent.hpp"

// Insert a key-value pair into the memtable. If the key already exists, update its value to the new value. 
// If the key does not exist and inserting it would cause the size of table_ to exceed maxKvPairs, return false
bool MemtableConcurrent::put(KEY_t key, VAL_t value) {
    // Check if key already exists so we can update its value and not worry about the memtable growing
    auto it = table_.find(key);
    if (it != table_.end()) {
        it->second = value;
        return true;
    }

    if (table_.size() >= maxKvPairs) {
        return false;
    }

    table_.insert(std::make_pair(key, value));
    return true;
}

// Get the value associated with a key
std::unique_ptr<VAL_t> MemtableConcurrent::get(KEY_t key) const {
    auto it = table_.find(key);
    if (it == table_.end()) {
        return nullptr;
    }
    std::unique_ptr<VAL_t> val = std::make_unique<VAL_t>();
    *val = it->second;
    return val;
}

// Get all key-value pairs within a range, inclusive of the start and exclusive of the end key
std::map<KEY_t, VAL_t> MemtableConcurrent::range(KEY_t start, KEY_t end) const {
    std::map<KEY_t, VAL_t> range;
    for (auto it = table_.lower_bound(start); it != table_.end() && it->first < end; ++it) {
        range.insert(*it);
    }
    return range;
}

// Return a map of all key-value pairs in the memtable
std::map<KEY_t, VAL_t> MemtableConcurrent::getMap() const {
    std::map<KEY_t, VAL_t> result;
    for (const auto& kv : table_) {
        result.insert(kv);
    }
    return result;
}

// Remove all key-value pairs from the memtable
void MemtableConcurrent::clear() {
    table_.clear();
}

bool MemtableConcurrent::clearAndPut(KEY_t key, VAL_t value) {
    table_.clear();
    // Insert the new key-value pair
    table_[key] = value;
    return true;
}

// Return the number of key-value pairs in the memtable
int MemtableConcurrent::size() const {
    return table_.size();
}

bool MemtableConcurrent::contains(KEY_t key) const {
    return table_.find(key) != table_.end();
}

// Serialize the memtable to a JSON object
json MemtableConcurrent::serialize() const {
    json j;
    j["maxKvPairs"] = maxKvPairs;
    j["table"] = table_;
    return j;
}

// Deserialize the memtable from a JSON object
void MemtableConcurrent::deserialize(const json& j) {
    maxKvPairs = j["maxKvPairs"].get<long>();
    std::map<KEY_t, VAL_t> table = j["table"].get<std::map<KEY_t, VAL_t>>();
    for (const auto& kv : table) {
        put(kv.first, kv.second);
    }
}