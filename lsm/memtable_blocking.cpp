#include "memtable_blocking.hpp"

// Insert a key-value pair into the memtable. If the key already exists, update its value to the new value. 
// If the key does not exist and inserting it would cause the size of table_ to exceed maxKvPairs, return false
bool MemtableBlocking::put(KEY_t key, VAL_t value) {
    //std::unique_lock lock(mtx_);

    auto it = table_.find(key);
    if (it != table_.end()) {
        it->second = value;
        return true;
    } else if (table_.size() < maxKvPairs) {
        table_.emplace(key, value);
        return true;
    }
    
    return false;
}

bool MemtableBlocking::clearAndPut(KEY_t key, VAL_t value) {
    // std::unique_lock lock(mtx_);
    table_.clear();
    // Insert the new key-value pair
    table_[key] = value;
    return true;
}


// Get the value associated with a key
std::unique_ptr<VAL_t> MemtableBlocking::get(KEY_t key) const {
    std::shared_lock lock(mtx_);
    auto it = table_.find(key);
    if (it == table_.end()) {
        return nullptr;
    }
    std::unique_ptr<VAL_t> val = std::make_unique<VAL_t>();
    *val = it->second;
    return val;
}


// Get all key-value pairs within a range, inclusive of the start and exclusive of the end key
std::map<KEY_t, VAL_t> MemtableBlocking::range(KEY_t start, KEY_t end) const {
    std::shared_lock lock(mtx_);
    std::map<KEY_t, VAL_t> range;
    auto itStart = table_.lower_bound(start);
    auto itEnd = table_.upper_bound(end);
    range = std::map<KEY_t, VAL_t>(itStart, itEnd);
    // If the last key in the range is the end key, remove it
    if (range.size() > 0 && range.rbegin()->first == end) {
        range.erase(range.rbegin()->first);
    }
    // Return a map of key-value pairs within the range
    return range;
}

// Return a map of all key-value pairs in the memtable
std::map<KEY_t, VAL_t> MemtableBlocking::getMap() const {
    std::shared_lock lock(mtx_);
    return table_;
}

// Remove all key-value pairs from the memtable
void MemtableBlocking::clear() {
    std::unique_lock lock(mtx_); 
    table_.clear();
}

// Return the number of key-value pairs in the memtable
int MemtableBlocking::size() const {
    std::shared_lock lock(mtx_);
    return table_.size();
}

bool MemtableBlocking::contains(KEY_t key) const {
    std::shared_lock lock(mtx_);
    return table_.find(key) != table_.end();
}

// Serialize the memtable to a JSON object
json MemtableBlocking::serialize() const {
    std::unique_lock lock(mtx_);
    json j;
    j["maxKvPairs"] = maxKvPairs;
    j["table"] = table_;
    return j;
}

// Deserialize the memtable from a JSON object
void MemtableBlocking::deserialize(const json& j) {
    std::unique_lock lock(mtx_);
    maxKvPairs = j["maxKvPairs"].get<long>();
    std::map<KEY_t, VAL_t> table = j["table"].get<std::map<KEY_t, VAL_t>>();
    for (const auto& kv : table) {
        put(kv.first, kv.second);
    }
}