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
    void printTree();
    void put(KEY_t, VAL_t);
    VAL_t* get(KEY_t);
    // void range(KEY_t, KEY_t);
    void del(KEY_t key);
    void load(const string& filename);
private:
    Memtable buffer;
    int bf_capacity;
    double bf_error_rate;
    int bf_bitset_size;
    int fanout;
    Level::Policy level_policy;

    vector<Level> levels;
    void merge_levels(int currentLevelNum);
};