#include "level.hpp"
#include <iostream>

// Add run to the beginning of the Level runs queue 
void Level::put(std::unique_ptr<Run> run_ptr) {
    //assert(kv_pairs == numKVPairs());

    // Check if there is enough space in the level to add the run
    if (kv_pairs + run_ptr->getMaxKvPairs() > max_kv_pairs) {
        throw std::out_of_range("put: Attempted to add run to level with insufficient space");
    }
    runs.emplace_front(move(run_ptr));
    // Increment the kv_pairs in the level
    kv_pairs += runs.front()->getMaxKvPairs(); 
}

// Dump the contents of the level
void Level::dump() {
    std::cout << "Level: " << std::endl;
    std::cout << "  number of runs: " << runs.size() << std::endl;
    std::cout << "  kv_pairs: " << kv_pairs << std::endl;
    std::cout << "  max_kv_pairs: " << max_kv_pairs << std::endl;
    std::cout << "  buffer_size: " << buffer_size << std::endl;
    std::cout << "  fanout: " << fanout << std::endl;
    std::cout << "  level_num: " << level_num <<std:: endl;
    std::cout << "  is_last_level: " << is_last_level << std::endl;
    std::cout << "  level_policy: " << level_policy << std::endl;
    std::cout << "  runs: " << std::endl;
    // Iterate through the runs in the level
    for (auto &run : runs) {
        // Print out the map of the run returned by getMap()
        for (auto kv : run->getMap()) {
            std::cout << "    " << kv.first << " : " << kv.second << " : L" << level_num << std::endl;
        }
    }
}

void Level::compactLevel(double error_rate, int bitset_size) {
    // Nothing to compact if there is only one run
    if (runs.size() == 1) {
        return;
    }
    // Create a new map to hold the merged data
    std::map<KEY_t, VAL_t> merged_map;

    // Iterate through the runs in the level
    for  (const auto& run : runs) {
        // Get the map of the run
        std::map<KEY_t, VAL_t> run_map = run->getMap();
        // print the size of the run_map
        //cout << "Size of run_map: " << run_map.size() << endl;
        // Iterate through the key-value pairs in the run map
        for (const auto &kv : run_map) {
            // Check if the key is already in the merged map
            if (const auto &[it, inserted] = merged_map.try_emplace(kv.first, kv.second); !inserted) {
                // The key already exists, so replace the value with the new value
                it->second = kv.second;
            }
        }
    }

    // Delete all the old files in the runs queue that have now been compacted
    for (const auto &run : runs) {
        run->closeFile();
        run->deleteFile();
    }

    // Clear the runs queue and reset the kv_pairs
    runs.clear();
    kv_pairs = 0;

    put(std::make_unique<Run>(getLevelSize(level_num), error_rate, bitset_size, true));

    // Iterate through the merged map and add the key-value pairs to the run
    for (const auto &kv : merged_map) {
        // Check if the key-value pair is a tombstone and if the level is the last level
        if (!(is_last_level && kv.second == TOMBSTONE)) {
            runs.front()->put(kv.first, kv.second);
        }
    }
    //assert(kv_pairs == numKVPairs());
    runs.front()->closeFile();
}

// If we haven't cached the size of a level, calculate it and cache it. Otherwise return the cached value.
long Level::getLevelSize(int level_num) {
    // Check if the level_num - 1 is in the level_sizes map. 
    // If it is, get the level size from the map. If not, calculate it and cache it
    if (level_sizes.find(level_num) != level_sizes.end()) {
        return level_sizes[level_num];
    } else {
        long level_size = pow(fanout, level_num) * buffer_size;
        level_sizes[level_num] = level_size;
        return level_size;
    }
}

// Returns true if there is enough space in the level to flush the buffer
bool Level::willBufferFit() {
    // Check if the sum of the current level's runs' kv_pairs and the buffer size is less than or equal to this level's max_kv_pairs
    //assert(numKVPairs() == kv_pairs);
    return (kv_pairs + buffer_size <= max_kv_pairs);
}

// Returns true if there is enough space in the level to add a run with max_kv_pairs
bool Level::willLowerLevelFit() {
    // Get the previous level number, or 1 if this is the first level
    int prevLevel = (level_num - 2) > 0 ? (level_num - 2) : 1;
    // Get the size of the previous level
    long prevLevelSize = getLevelSize(prevLevel);
    return (kv_pairs + prevLevelSize <= max_kv_pairs);
}

// Count the number of kv_pairs in a level by iterating through the runs queue
int Level::numKVPairs() {
    int num_kv_pairs = 0;
    for (const auto& run : runs) {
        num_kv_pairs += run->getMaxKvPairs();
    }
    return num_kv_pairs;
}

// Returns the level number
int Level::getLevelNum() const {
    return level_num;
}

Level::Policy Level::getLevelPolicy() const {
    return level_policy;
}
// Get the number of kv_pairs in the level
long Level::getKvPairs() const {
    return kv_pairs;
}
// Set the number of kv_pairs in the level
void Level::setKvPairs(long kv_pairs) {
    this->kv_pairs = kv_pairs;
}

// Return the maximum number of key-value pairs allowed in the memtable
long Level::getMaxKvPairs() const {
    return max_kv_pairs;
}

json Level::serialize() const {
    json j;
    j["max_kv_pairs"] = max_kv_pairs;
    j["buffer_size"] = buffer_size;
    j["fanout"] = fanout;
    j["level_num"] = level_num;
    j["is_last_level"] = is_last_level;
    j["level_policy"] = policyToString(level_policy);
    j["kv_pairs"] = kv_pairs;
    j["runs"] = json::array();
    for (const auto& run : runs) {
        j["runs"].push_back(run->serialize());
    }
    return j;
}

void Level::deserialize(const json& j) {
    buffer_size = j["buffer_size"];
    fanout = j["fanout"];
    level_num = j["level_num"];
    is_last_level = j["is_last_level"];
    max_kv_pairs = j["max_kv_pairs"];
    kv_pairs = j["kv_pairs"];

    if (level_num == 1) {
        std::cout << "Deserializing level 1" << std::endl;
    } 
    std::string policy_str = j["level_policy"];
    level_policy = stringToPolicy(policy_str);

    for (auto& run_json : j["runs"]) {
        std::unique_ptr<Run> run = std::make_unique<Run>(max_kv_pairs, DEFAULT_ERROR_RATE, DEFAULT_BITSET_SIZE, false);
        run->deserialize(run_json);
        runs.emplace_back(std::move(run));
    }
}
