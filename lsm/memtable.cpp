#include "memtable.hpp"

// Insert a key-value pair into the memtable. If the key already exists, update its value to the new value. 
// If the key does not exist and inserting it would cause the size of table_ to exceed max_kv_pairs, return false
bool Memtable::put(KEY_t key, VAL_t value) {
    auto it = table_.find({ key, VAL_t{} });
    if (it != table_.end()) {
        table_.erase(it);
        table_.insert({ key, value });
    } else {
        if (table_.size() >= max_kv_pairs) {
            return false;
        }
        table_.insert({ key, value });
    }
    return true;
}

// Get the value associated with a key
VAL_t* Memtable::get(KEY_t key) {
    auto it = table_.find({ key, VAL_t{} });
    if (it != table_.end()) {
        VAL_t* val = new VAL_t;
        *val = it->value;
        return val;
    }
    return nullptr;
}

// Get all key-value pairs within a range
std::vector<kv_pair> Memtable::range(KEY_t start, KEY_t end) {
    std::vector<kv_pair> range;
    for (auto it = table_.lower_bound({ start, VAL_t{} }); it != table_.end() && it->key <= end; ++it) {
        range.push_back(*it);
    }
    return range;
}

// Remove all key-value pairs from the memtable
void Memtable::clear() {
    table_.clear();
}
