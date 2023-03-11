#include <cstdlib>
#include <iostream>
#include <string>
#include <cassert>
#include <stdexcept>
#include <vector>

#include "unistd.h"

#include "data_types.hpp"
#include "error.hpp"
#include "bloom_filter.hpp"
#include "memtable.hpp"
#include "run.hpp"
#include "level.hpp"

class LSMTree {
public:
    LSMTree(int, float, int, int, int, Level::Policy);
    void put(KEY_t, VAL_t);
    unique_ptr<VAL_t> get(KEY_t key);
    unique_ptr<map<KEY_t, VAL_t>> range(KEY_t start, KEY_t end);
    void del(KEY_t key);
    void load(const string& filename);
    string printStats();
    string printTree();
private:
    Memtable buffer;
    int bf_capacity;
    double bf_error_rate;
    int bf_bitset_size;
    int fanout;
    Level::Policy level_policy;
    int countLogicalPairs();
    void removeTombstones(std::unique_ptr<std::map<KEY_t, VAL_t>> &range_map);


    vector<Level> levels;
    void merge_levels(int currentLevelNum);
};