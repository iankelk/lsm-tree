#ifndef MEMTABLE_H
#define MEMTABLE_H

#include <map>
#include <vector>
#include "data_types.hpp"

using namespace std;

class Memtable {
public:
    Memtable(int max_kv_pairs) : max_kv_pairs(max_kv_pairs) {}
    ~Memtable() {};

    int max_kv_pairs;
    bool put(KEY_t key, VAL_t value);
    VAL_t* get(KEY_t key) const;
    map<KEY_t, VAL_t> range(KEY_t start, KEY_t end) const;
    void clear();
    map<KEY_t, VAL_t> table_;

private:
    
};

#endif /* MEMTABLE_H */
