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
    // buffer_size(bs), fanout(f), level_policy(l), level_num(ln)
    levels.emplace_back(buffer.max_kv_pairs, fanout, level_policy, FIRST_LEVEL_NUM);

    // Set the level to indicate it's the last level
    levels.back().is_last_level = true;

    // print out the max_kv_pairs of the buffer
    //cout << "Buffer max_kv_pairs: " << buffer.max_kv_pairs << "\n";
}

// Insert a key-value pair of integers into the LSM tree
void LSMTree::put(KEY_t key, VAL_t val) {
    if(buffer.put(key, val)) {
        return;
    }

    // Merge the levels recursively
    merge_levels(levels.begin());

    // Set all levels in the levels vector to not be the last level unless their level_num is equal to the level vector's size
    // for (auto it = levels.begin(); it != levels.end(); it++) {
    //     it->is_last_level = (it->level_num == levels.size());
    // }

    // // print the key and value
    // cout << "Key: " << key << " Value: " << val << "\n";

    // // print the max runs of the first level
    // cout << "Max runs of first level: " << levels.front().max_runs << "\n";
    // // print the number of runs of the first level
    // cout << "Number of runs of first level: " << levels.front().num_runs << "\n";
    
    // Create a new run and add a unique pointer to it to the first level
    levels.front().put(make_unique<Run>(buffer.max_kv_pairs, bf_capacity, bf_error_rate, bf_bitset_size));
    //unique_ptr<Run> run_ptr = make_unique<Run>(buffer.max_kv_pairs, bf_capacity, bf_error_rate, bf_bitset_size);
    //levels.front().runs.emplace_front(move(run_ptr));

    // Create a new run with the right number of key/value pairs, bloom filter capacity, error rate, and bitset size
    //levels.front().runs.emplace_front(make_unique<Run>(buffer.max_kv_pairs, bf_capacity, bf_error_rate, bf_bitset_size));

    // print the capacity of the first run of the first level
    cout << "Capacity of first run of first level: " << levels.front().runs.front()->capacity << "\n";

    // Print the filename of the temporary file
    cout << "Filename: " << levels.front().runs.front()->tmp_file << endl;
    // increment the number of runs in the first level
    //levels.front().num_runs++;

    // Flush the buffer to level 1
    for (auto it = buffer.table_.begin(); it != buffer.table_.end(); it++) {
        levels.front().runs.front()->put(it->first, it->second);
    }
    // Clear the buffer
    buffer.clear();
    // Add the key-value pair to the buffer
    buffer.put(key, val);

    printTree();
}


// Given an iterator for the levels vector, check to see if there is space to insert another run
// If the next level does not have space for the current level, recursively merge the next level downwards
// until a level has space for a new run in it.
void LSMTree::merge_levels(vector<Level>::iterator it) {
    vector<Level>::iterator next;
    // Save the level number of the current level
    int level_num = it->level_num;

    // If the current level has space for another run, return
    if (it->willLowerLevelFit()) {
        return;
    } else {
        // If we have reached the end of the levels vector, create a new level. Move the next pointer to the new level.
        if (it + 1 == levels.end()) {
            levels.emplace_back(buffer.max_kv_pairs, fanout, level_policy, level_num + 1);
            // Get the iterators next and it which are lost when the vector is resized
            next = levels.end() - 1;
            it = levels.end() - 2;
            // Set the level to indicate it's the last level
            next->is_last_level = true;
            // // Make sure the next level is cleared
            // next->runs.clear();

            // print the size of the next->runs deque
            cout << "Size of next->runs deque: " << next->runs.size() << "\n";
        // If the next level has space for another run, move the next pointer to the next level
        } else {
            next = it + 1;
            if (!next->willLowerLevelFit()) {
                merge_levels(next);
             }
        }
    }

    // Clear the deque of runs in the next level
   // next->runs.clear();
    cout << "Size of runs queue in merge_levels before: " << next->runs.size() << "\n";

    // Merge the current level into the next level by moving the entire deque of runs into the next level
    next->runs.insert(next->runs.end(), make_move_iterator(it->runs.begin()), make_move_iterator(it->runs.end()));
    // Print the size of the runs queue
    cout << "Size of runs queue in merge_levels after: " << next->runs.size() << "\n";
    
     // Compact the level
    next->compactLevel(buffer.max_kv_pairs * next->num_runs, bf_capacity, bf_error_rate, bf_bitset_size);

    // Clear the current level
    it->runs.clear();
    // Increment the number of runs in the next level
    next->num_runs += it->num_runs;
    // Zero the number of runs in the current level
    it->num_runs = 0;
   
}

    // Print tree. Print the number of entries in the buffer. Then print the number of levels, then print 
    // the number of runs per each level.
    void LSMTree::printTree() {
        cout << "\nPRINTING TREE...\n";
        cout << "Number of entries in the buffer: " << buffer.table_.size() << "\n";
        cout << "Number of levels: " << levels.size() << "\n";
        for (auto it = levels.begin(); it != levels.end(); it++) {
            cout << "Number of runs in level " << it->level_num << ": " << it->num_runs << "\n";
        }
        cout << "PRINTING TREE COMPLETE\n\n";
    }