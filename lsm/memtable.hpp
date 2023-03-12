#ifndef MEMTABLE_HPP
#define MEMTABLE_HPP

#include <map>
#include <vector>
#include "data_types.hpp"

class Memtable {
public:
    explicit Memtable(int max_kv_pairs) : max_kv_pairs(max_kv_pairs) {}
    ~Memtable() {};

    int max_kv_pairs;
    bool put(KEY_t key, VAL_t value);
    std::unique_ptr<VAL_t> get(KEY_t key) const;
    std::map<KEY_t, VAL_t> range(KEY_t start, KEY_t end) const;
    void clear();
    int size() const;
    std::map<KEY_t, VAL_t> table_;

private:
    
};

#endif /* MEMTABLE_HPP */
