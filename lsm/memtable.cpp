#include "memtable.hpp"

// Insert a key-value pair into the memtable. If the key already exists, update its value to the new value. 
// If the key does not exist and inserting it would cause the size of table_ to exceed maxKvPairs, return false
bool Memtable::put(KEY_t key, VAL_t value) {
    // Check if key already exists so we can update its value and not worry about the memtable growing
    auto it = table_.find(key);
    if (it != table_.end()) {
        it->second = value;
        return true;
    }
    // Check if inserting the new key-value pair would cause the memtable to grow too large
    if (table_.size() >= maxKvPairs) {
        return false;
    }
    // Insert the new key-value pair
    table_[key] = value;
    return true;
}

// Get the value associated with a key
std::unique_ptr<VAL_t> Memtable::get(KEY_t key) const {
    auto it = table_.find(key);
    if (it == table_.end()) {
        return nullptr;
    }
    std::unique_ptr<VAL_t> val = std::make_unique<VAL_t>();
    *val = it->second;
    return val;
}


// Get all key-value pairs within a range, inclusive of the start and exclusive of the end key
std::map<KEY_t, VAL_t> Memtable::range(KEY_t start, KEY_t end) const {
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

// Remove all key-value pairs from the memtable
void Memtable::clear() {
    table_.clear();
}

// Return the number of key-value pairs in the memtable
size_t Memtable::size() const {
    return table_.size();
}
// Return a map of all key-value pairs in the memtable
std::map<KEY_t, VAL_t> Memtable::getMap() const {
    return table_;
}
// Return the maximum number of key-value pairs allowed in the memtable
long Memtable::getMaxKvPairs() const {
    return maxKvPairs;
}

std::map<KEY_t, VAL_t>::const_iterator Memtable::begin() const {
    return table_.begin();
}

std::map<KEY_t, VAL_t>::const_iterator Memtable::end() const {
    return table_.end();
}

std::map<KEY_t, VAL_t>::const_reverse_iterator Memtable::rbegin() const {
    return table_.rbegin();
}

// Serialize the memtable to a JSON object
json Memtable::serialize() const {
    json j;
    j["maxKvPairs"] = maxKvPairs;
    j["table"] = table_;
    return j;
}

// Deserialize the memtable from a JSON object
void Memtable::deserialize(const json& j) {
    maxKvPairs = j["maxKvPairs"].get<long>();
    std::map<KEY_t, VAL_t> table = j["table"].get<std::map<KEY_t, VAL_t>>();
    for (const auto& kv : table) {
        put(kv.first, kv.second);
    }
}