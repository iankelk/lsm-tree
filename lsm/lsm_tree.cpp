#include "lsm_tree.hpp"

#include <iostream>
#include <string>
#include <cassert>
#include <stdexcept>
#include <cmath>

#include "unistd.h"

using namespace std;

LSMTree::LSMTree(int bf_capacity, float bf_error_rate, int bf_bitset_size, int buffer_num_pages, int fanout, Level::Policy level_policy) :
    bf_capacity(bf_capacity), bf_error_rate(bf_error_rate), bf_bitset_size(bf_bitset_size), fanout(fanout), level_policy(level_policy),
    buffer(buffer_num_pages * getpagesize() / sizeof(kv_pair))
{
    // TODO: correct the leveling to read from the command line
    //level_policy = DEFAULT_LEVELING_POLICY;

    // Level(int n, bool l, int ln) : max_runs(n), leveling(l), level_num(ln), num_runs(0) {}
    // buffer_size(bs), fanout(f), level_policy(l), level_num(ln)
    levels.emplace_back(buffer.max_kv_pairs, fanout, level_policy, FIRST_LEVEL_NUM);

    // Set the first level in the levels vector to be the last level
    levels.front().is_last_level = true;
    // print out the max_kv_pairs of the buffer
    //cout << "Buffer max_kv_pairs: " << buffer.max_kv_pairs << "\n";
}

// Insert a key-value pair of integers into the LSM tree
void LSMTree::put(KEY_t key, VAL_t val) {

    if (val < VAL_MIN || val > VAL_MAX) {
        die("Could not insert value " + to_string(val) + ": out of range.");
    }

    if(buffer.put(key, val)) {
        return;
    }
    //printTree();

    // Check to see if the first level has space for the buffer. If not, merge the levels recursively
    if (!levels.front().willBufferFit()) {
        merge_levels(FIRST_LEVEL_NUM);
    }
    
    // Create a new run and add a unique pointer to it to the first level
    levels.front().put(make_unique<Run>(buffer.max_kv_pairs, bf_capacity, bf_error_rate, bf_bitset_size));

    // print out the max_kv_pairs of the first run of the first level
    cout << "Max kv pairs of first run of first level: " << levels.front().runs.front()->max_kv_pairs << "\n";

    // print the capacity of the first run of the first level
    cout << "Capacity of first run of first level: " << levels.front().runs.front()->capacity << "\n";

    // Print the filename of the temporary file
    cout << "Filename: " << levels.front().runs.front()->tmp_file << endl;

    // Flush the buffer to level 1
    for (auto it = buffer.table_.begin(); it != buffer.table_.end(); it++) {
        levels.front().runs.front()->put(it->first, it->second);
    }

    // If level_policy is true, then compact the first level
    if (level_policy == Level::LEVELED) {
        levels.front().compactLevel(bf_capacity, bf_error_rate, bf_bitset_size);
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
void LSMTree::merge_levels(int currentLevelNum) {
    vector<Level>::iterator it;
    vector<Level>::iterator next;

    // Get the iterator for the current level and subtract 1 since the levels vector is 0-indexed
    it = levels.begin() + currentLevelNum - 1;

    // If the current level has space for another run, return
    if (it->willLowerLevelFit()) {
        // print the current level number
        cout << "It FITS! Current level number: " << currentLevelNum << "\n";
        return;
    } else {
        // If we have reached the end of the levels vector, create a new level. Move the next pointer to the new level.
        if (it + 1 == levels.end()) {
            levels.emplace_back(buffer.max_kv_pairs, fanout, level_policy, currentLevelNum + 1);
            // Get the iterators "it" and "next" which are lost when the vector is resized
            it = levels.end() - 2;
            next = levels.end() - 1;
            it->is_last_level = false;
            next->is_last_level = true;
        // If the next level has space for another run, move the next pointer to the next level
        } else {
            next = it + 1;
            if (!next->willLowerLevelFit()) {
                merge_levels(currentLevelNum + 1);
             }
        }
    }

    it = levels.begin() + currentLevelNum - 1;
    next = levels.begin() + currentLevelNum;

    // Clear the deque of runs in the next level
    // next->runs.clear();
    cout << "Size of runs queue in merge_levels next before: " << next->runs.size() << "\n";
    cout << "Size of runs queue in merge_levels it before: " << it->runs.size() << "\n";

    // Merge the current level into the next level by moving the entire deque of runs into the next level
    next->runs.insert(next->runs.end(), make_move_iterator(it->runs.begin()), make_move_iterator(it->runs.end()));
    // Print the size of the runs queue
    cout << "Size of runs queue in merge_levels next after: " << next->runs.size() << "\n";
    cout << "Size of runs queue in merge_levels it after: " << it->runs.size() << "\n";

    if (level_policy == Level::LEVELED || level_policy == Level::LAZY_LEVELED && next->is_last_level) {
        next->compactLevel(bf_capacity, bf_error_rate, bf_bitset_size);
    }
    
    // Increment the number of runs in the next level
    next->num_runs = next->runs.size();
    // print out the filename of the first run in the next level
    //cout << "Filename of first run in next level: " << next->runs.front()->tmp_file << "\n";

    // Clear the current level
    it->runs.clear();
    // Zero the number of runs in the current level
    it->num_runs = it->runs.size();
    // Zero the number of key/value pairs in the current level
    it->kv_pairs = 0;
}
    // Print tree. Print the number of entries in the buffer. Then print the number of levels, then print 
    // the number of runs per each level.
    void LSMTree::printTree() {
        cout << "\nPRINTING TREE...\n";
        cout << "Number of entries in the buffer: " << buffer.table_.size() << "\n";
        cout << "Number of levels: " << levels.size() << "\n";
        for (auto it = levels.begin(); it != levels.end(); it++) {
            cout << "Number of runs in level " << it->level_num << ": " << it->runs.size() << "\n";
            // print the number of key/value pairs in each run using the numKVPairs() function
            cout << "Number of KV pairs in level " << it->level_num << ": " << it->numKVPairs() << "\n";
        }
        // For each level, print if it is the last level
        for (auto it = levels.begin(); it != levels.end(); it++) {
            cout << "Is level " << it->level_num << " the last level? " << it->is_last_level << "\n";
        }
        cout << "PRINTING TREE COMPLETE\n\n";
    }