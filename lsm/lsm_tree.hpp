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

using namespace std;

class LSMTree {
public:
    enum Policy {
        TIERED,
        LEVELED,
        LAZY_LEVELED
    };
    LSMTree(int, float, int, int, int, Policy);
    void printTree();
    void put(KEY_t, VAL_t);
    // void get(KEY_t);
    // void range(KEY_t, KEY_t);
    // void del(KEY_t);
    // void load(std::string);
private:
    Memtable buffer;
    int bf_capacity;
    double bf_error_rate;
    int bf_bitset_size;
    int fanout;
    Policy level_policy;

    vector<Level> levels;
    void merge_levels(int currentLevelNum);
};