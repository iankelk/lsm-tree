#include "level.hpp"
#include <iostream>

using namespace std;

// Add run to the beginning of the Level runs queue 
void Level::put(unique_ptr<Run> run_ptr) {
    if (num_runs >= max_runs) {
        throw std::out_of_range("put: Attempted to add run to full level");
    }
    runs.emplace_front(move(run_ptr));
    num_runs++;
}

// Dump the contents of the level
void Level::dump() {
    cout << "Level: " << endl;
    cout << "  num_runs: " << num_runs << endl;
    cout << "  max_runs: " << max_runs << endl;
    cout << "  leveling policy: " << level_policy << endl;
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

    // Print the number of runs in the level
    cout << "Number of runs in level: " << num_runs << endl;
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


