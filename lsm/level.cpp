#include "level.hpp"
#include <iostream>

using namespace std;

// Add run to the beginning of the Level runs queue 
void Level::put(Run run) {
    if (num_runs >= max_runs) {
        throw out_of_range("put: Attempted to add run to full level");
    }
    runs.emplace_front(run);
    num_runs++;
}

// Dump the contents of the level
void Level::dump() {
    cout << "Level: " << endl;
    cout << "  num_runs: " << num_runs << endl;
    cout << "  max_runs: " << max_runs << endl;
    cout << "  max_run_size: " << max_run_size << endl;
    cout << "  leveling: " << leveling << endl;
    cout << "  runs: " << endl;
    for (auto run : runs) {
        // Print out the map of the run returned by getMap()
        for (auto kv : run.getMap()) {
            cout << "    " << kv.first << " : " << kv.second << " : L" << level_num << endl;
        }
    }
}

void Level::compactLevel() {
    // Create a new map to hold the merged data
    map<KEY_t, VAL_t> merged_map;
    // Iterate through the runs in the level
    for (auto run : runs) {
        // Get the map of the run
        map<KEY_t, VAL_t> run_map = run.getMap();
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
    Run merged_run(runs.front().getMaxKvPairs(), runs.front().getCapacity(), runs.front().getErrorRate(), runs.front().getBitsetSize());
    for (const auto &kv : merged_map) {
        if (kv.second != TOMBSTONE) {
            merged_run.put(kv.first, kv.second);
        }
    }

    // Delete the old runs
    for (auto run : runs) {
        delete &run;
    }

    // Clear the runs queue
    runs.clear();
    // Add the merged run to the runs queue
    runs.emplace_front(merged_run);
    // Set the number of runs to 1
    num_runs = 1;         
}

