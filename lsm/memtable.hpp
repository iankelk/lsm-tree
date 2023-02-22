#ifndef MEMTABLE_H
#define MEMTABLE_H

#include <set>
#include <vector>

#include "data_types.hpp"

class Memtable {
public:
    Memtable(int max_kv_pairs) : max_kv_pairs(max_kv_pairs) {}
    ~Memtable();

    int max_kv_pairs;
    bool put(KEY_t key, VAL_t value);
    VAL_t* get(KEY_t key);
    std::vector<kv_pair> range(KEY_t start, KEY_t end);
    void clear();

private:
    std::set<kv_pair> table_;
};

#endif /* MEMTABLE_H */