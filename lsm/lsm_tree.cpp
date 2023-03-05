#include "lsm_tree.hpp"

#include <iostream>
#include <string>
#include <cassert>
#include <stdexcept>
#include <cmath>

#include "unistd.h"

using namespace std;

LSMTree::LSMTree(int bf_capacity, float bf_error_rate, int bf_bitset_size, int buffer_num_pages, int fanout) :
    bf_capacity(bf_capacity), bf_error_rate(bf_error_rate), bf_bitset_size(bf_bitset_size), fanout(fanout),
    buffer(buffer_num_pages * getpagesize() / sizeof(kv_pair))
{
    // TODO: correct the leveling to read from the command line
    level_policy = DEFAULT_LEVELING_POLICY;

    // Level(int n, bool l, int ln) : max_runs(n), leveling(l), level_num(ln), num_runs(0) {}
    levels.emplace_back(pow(fanout, FIRST_LEVEL_NUM), level_policy, FIRST_LEVEL_NUM);

    // print out the max_kv_pairs of the buffer
    //cout << "Buffer max_kv_pairs: " << buffer.max_kv_pairs << "\n";
}

// Insert a key-value pair of integers into the LSM tree
void LSMTree::put(KEY_t key, VAL_t val) {
    if(buffer.put(key, val)) {
        return;
    }
    // print the key and value
    cout << "Key: " << key << " Value: " << val << "\n";

    // print the max runs of the first level
    cout << "Max runs of first level: " << levels.front().max_runs << "\n";
    // print the number of runs of the first level
    cout << "Number of runs of first level: " << levels.front().num_runs << "\n";
    
    // Create a new run and add a unique pointer to it to the first level
    levels.front().put(make_unique<Run>(buffer.max_kv_pairs, bf_capacity, bf_error_rate, bf_bitset_size));

    // Flush the buffer to level 1
    for (auto it = buffer.table_.begin(); it != buffer.table_.end(); it++) {
        levels.front().runs.front()->put(it->first, it->second);
    }
    // Clear the buffer
    buffer.clear();
    // Add the key-value pair to the buffer
    buffer.put(key, val);
}


// Given an iterator for the levels vector, check to see if there is space to insert another run
// If the next level does not have space for the current level, recursively merge the next level downwards
// until a level has space for a new run in it.
void LSMTree::merge_levels(vector<Level>::iterator it) {
    vector<Level>::iterator next;

    // If the current level has space for another run, return
    if (it->num_runs < it->max_runs) {
        return;
    } else {
        // If there is another level, move next to it. Otherwise create a new level and move next to it.
        if (it + 1 != levels.end()) {
            next = it + 1;
        } else {
            levels.emplace_back(pow(fanout, it->level_num + 1), level_policy, it->level_num + 1);
            next = it + 1;
        }
    }
    if (next->num_runs >= next->max_runs) {
        merge_levels(next);
    }
    // Merge the current level into the next level by moving the entire deque of runs into the next level
    next->runs.insert(next->runs.end(), make_move_iterator(it->runs.begin()), make_move_iterator(it->runs.end()));
    // Clear the current level
    it->runs.clear();
    // Increment the number of runs in the next level
    next->num_runs += it->num_runs;
    // Decrement the number of runs in the current level
    it->num_runs = 0;
    // Compact the level
    next->compactLevel(buffer.max_kv_pairs * next->num_runs, bf_capacity, bf_error_rate, bf_bitset_size);
}