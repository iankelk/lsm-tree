#include "level.hpp"
#include <iostream>

using namespace std;

// Add run to the beginning of the Level runs queue 
void Level::put(unique_ptr<Run> run_ptr) {
    if (!willLowerLevelFit()) {
        throw std::out_of_range("put: Attempted to add run to level with insufficient space");
    }
    runs.emplace_front(move(run_ptr));
    // Increment the kv_pairs in the level
    kv_pairs += runs.front()->max_kv_pairs;

    // Increment the number of runs in the level    
    num_runs++;
    // Update the number of key-value pairs in the level
    kv_pairs += runs.front()->max_kv_pairs;
    // print the number of key-value pairs in the level
    cout << "Number of key-value pairs in level: " << kv_pairs << endl;
    // print the max number of key-value pairs in the level
    cout << "Max number of key-value pairs in level: " << max_kv_pairs << endl;
}

// Dump the contents of the level
void Level::dump() {
    cout << "Level: " << endl;
    cout << "  num_runs: " << num_runs << endl;
    cout << "  kv_pairs: " << kv_pairs << endl;
    cout << "  max_kv_pairs: " << max_kv_pairs << endl;
    cout << "  buffer_size: " << buffer_size << endl;
    cout << "  fanout: " << fanout << endl;
    cout << "  level_num: " << level_num << endl;
    cout << "  is_last_level: " << is_last_level << endl;
    cout << "  level_policy: " << level_policy << endl;
    cout << "  runs: " << endl;
    // Iterate through the runs in the level
    for (auto &run : runs) {
        // Print out the map of the run returned by getMap()
        for (auto kv : run->getMap()) {
            cout << "    " << kv.first << " : " << kv.second << " : L" << level_num << endl;
        }
    }
}

// Run::Run(long max_kv_pairs, int capacity, double error_rate, int bitset_size) :
void Level::compactLevel(long max_kv_pairs, int capacity, double error_rate, int bitset_size) {
    // Create a new map to hold the merged data
    map<KEY_t, VAL_t> merged_map;

    // Print the size of the runs queue
    cout << "Size of runs queue: " << runs.size() << endl;

    // Iterate through the runs in the level
    for (auto &run : runs) {
        // Get the map of the run
        map<KEY_t, VAL_t> run_map = run->getMap();
        // Iterate through the key-value pairs in the run map
        for (const auto &kv : run_map) {
            // Check if the key is already in the merged map
            if (const auto &[it, inserted] = merged_map.emplace(kv.first, kv.second); !inserted) {
                // The key already exists, so merge the values by summing them up
                it->second += kv.second;
            }
        }
    }

    // Create a new run with the merged data
    // TODO: FANOUT should be a parameter
    Run merged_run(max_kv_pairs, capacity, error_rate, bitset_size);
    for (const auto &kv : merged_map) {
        // Check if the key-value pair is a tombstone and if the level is the last level
        if (!(is_last_level && kv.second == TOMBSTONE)) {
            merged_run.put(kv.first, kv.second);
        }
    }

    // Check if this is the last level


    // Delete the old runs
    // for (auto &run : runs) {
    //     delete &run;
    // }

    // Clear the runs queue
    runs.clear();
    // Add the merged run to the runs queue

    runs.emplace_front(move(unique_ptr<Run> (&merged_run)));
    // Set the number of runs to 1
    num_runs = 1;         
}
long Level::sumMaxKvPairs() {
    long sum = 0;
    for (const auto& run : runs) {
        sum += run->max_kv_pairs;
    }
    return sum;
}

long Level::calculateLevelSize(int level_num) {
    // Calculate the level size using the formula: f^ln * bs
    long level_size = pow(fanout, level_num) * buffer_size;
    // Insert the level size into the level_sizes vector at the index of level_num - 1 using insert()
    level_sizes.insert(level_sizes.begin() + level_num - 1, level_size);
    // Return the level size
    return level_size;
}

// Returns true if there is enough space in the level to add a run with max_kv_pairs
bool Level::willLowerLevelFit() {
    // Get the previous level number, or 1 if this is the first level
    int prevLevel = (level_num - 2) > 0 ? (level_num - 2) : 1;
    // Check if the prevLevel - 1 is in the level_sizes vector. If it is, get the level size from the vector. 
    // If not, calculate the level size using calculateLevelSize()
    long prevLevelSize = prevLevel < level_sizes.size() ? level_sizes[prevLevel - 2] : calculateLevelSize(prevLevel);
    // print the previous level size, the kv_pairs, and the max_kv_pairs
    cout << "prevLevelSize: " << prevLevelSize << " kv_pairs: " << kv_pairs << " max_kv_pairs: " << max_kv_pairs << endl;

    bool willFit = (kv_pairs + prevLevelSize <= max_kv_pairs);
    // print whether the level will fit
    cout << "willFit: " << willFit << endl;
    // Check if the sum of the current level's runs' kv_pairs and the previous level's size is less than or equal to this level's max_kv_pairs
    return (kv_pairs + prevLevelSize <= max_kv_pairs);
}

