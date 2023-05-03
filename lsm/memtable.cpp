#include "memtable.hpp"

// Insert a key-value pair into the memtable. If the key already exists, update its value to the new value. 
// If the key does not exist and inserting it would cause the size of table_ to exceed maxKvPairs, return false
bool Memtable::put(KEY_t key, VAL_t value) {
    auto it = table_.find({key, value});
    if (it != table_.end()) {
        table_.erase(it);
    }
    if (table_.size() >= maxKvPairs) {
        return false;
    }
    table_.insert({key, value});
    return true;
}

std::unique_ptr<VAL_t> Memtable::get(KEY_t key) const {
    auto it = table_.find({key, VAL_t()});
    if (it == table_.end()) {
        return nullptr;
    }
    std::unique_ptr<VAL_t> val = std::make_unique<VAL_t>();
    *val = it->value;
    return val;
}

std::set<kvPair> Memtable::range(KEY_t start, KEY_t end) const {
    auto itStart = table_.lower_bound({start, VAL_t()});
    auto itEnd = table_.upper_bound({end, VAL_t()});
    std::set<kvPair> range(itStart, itEnd);
    return range;
}

void Memtable::clear() {
    table_.clear();
}

size_t Memtable::size() const {
    return table_.size();
}

std::set<kvPair> Memtable::getSet() const {
    return table_;
}

long Memtable::getMaxKvPairs() const {
    return maxKvPairs;
}

json Memtable::serialize() const {
    json j;
    j["maxKvPairs"] = maxKvPairs;
    
    // Serialize table_ as an array of objects
    json tableArray = json::array();
    for (const auto& kv : getSet()) {
        json kvJson;
        kvJson["key"] = kv.key;
        kvJson["value"] = kv.value;
        tableArray.push_back(kvJson);
    }
    j["table"] = tableArray;

    return j;
}

void Memtable::deserialize(const json& j) {
    maxKvPairs = j["maxKvPairs"].get<long>();
    
    // Deserialize table_ from an array of objects
    json tableArray = j["table"];
    for (const auto& kvJson : tableArray) {
        KEY_t key = kvJson["key"].get<KEY_t>();
        VAL_t value = kvJson["value"].get<VAL_t>();
        put(key, value);
    }
}


std::set<kvPair>::const_iterator Memtable::begin() const {
    return table_.begin();
}

std::set<kvPair>::const_iterator Memtable::end() const {
    return table_.end();
}

std::set<kvPair>::const_reverse_iterator Memtable::rbegin() const {
    return table_.rbegin();
}