#include <set>
#include <iostream>
#include <fstream>
#include <sstream>
#include <unistd.h>

#include "lsm_tree.hpp"

LSMTree::LSMTree(int bf_capacity, float bf_error_rate, int bf_bitset_size, int buffer_num_pages, int fanout, Level::Policy level_policy) :
    bf_capacity(bf_capacity), bf_error_rate(bf_error_rate), bf_bitset_size(bf_bitset_size), fanout(fanout), level_policy(level_policy),
    buffer(buffer_num_pages * getpagesize() / sizeof(kv_pair))
{
    // Create the first level
    levels.emplace_back(buffer.getMaxKvPairs(), fanout, level_policy, FIRST_LEVEL_NUM);

    // Set the first level in the levels vector to be the last level
    levels.front().setLastLevel(true);
}

// Insert a key-value pair of integers into the LSM tree
void LSMTree::put(KEY_t key, VAL_t val) {
    // Try to insert the key-value pair into the buffer. If it succeeds, return.
    if(buffer.put(key, val)) {
        return;
    }

    // Buffer is full, so check to see if the first level has space for the buffer. If not, merge the levels recursively
    if (!levels.front().willBufferFit()) {
        merge_levels(FIRST_LEVEL_NUM);
    }
    
    // Create a new run and add a unique pointer to it to the first level
    levels.front().put(std::make_unique<Run>(buffer.getMaxKvPairs(), buffer.size(), bf_error_rate, bf_bitset_size));

    // Flush the buffer to level 1
    std::map<KEY_t, VAL_t> bufferContents = buffer.getMap();
    for (auto it = bufferContents.begin(); it != bufferContents.end(); it++) {
        levels.front().runs.front()->put(it->first, it->second);
    }

    // Close the run's file
    levels.front().runs.front()->closeFile();

    // If level_policy is true, then compact the first level
    if (level_policy == Level::LEVELED) {
        levels.front().compactLevel(buffer.size(), bf_error_rate, bf_bitset_size);
    }

    // Clear the buffer and add the key-value pair to it
    buffer.clear();
    buffer.put(key, val);

    serializeLSMTreeToFile("/tmp/LSMTree.json");
}


// Given an iterator for the levels vector, check to see if there is space to insert another run
// If the next level does not have space for the current level, recursively merge the next level downwards
// until a level has space for a new run in it.
void LSMTree::merge_levels(int currentLevelNum) {
    std::vector<Level>::iterator it;
    std::vector<Level>::iterator next;

    // Get the iterator for the current level and subtract 1 since the levels vector is 0-indexed
    it = levels.begin() + currentLevelNum - 1;

    // If the current level has space for another run, return
    if (it->willLowerLevelFit()) {
        // print the current level number
        std::cout << "It FITS! Current level number: " << currentLevelNum << "\n";
        return;
    } else {
        // If we have reached the end of the levels vector, create a new level. Move the next pointer to the new level.
        if (it + 1 == levels.end()) {
            levels.emplace_back(buffer.getMaxKvPairs(), fanout, level_policy, currentLevelNum + 1);
            // Get the iterators "it" and "next" which are lost when the vector is resized
            it = levels.end() - 2;
            next = levels.end() - 1;
            it->setLastLevel(false);
            next->setLastLevel(true);
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

    // Merge the current level into the next level by moving the entire deque of runs into the next level
    next->runs.insert(next->runs.end(), std::make_move_iterator(it->runs.begin()), make_move_iterator(it->runs.end()));

    if (level_policy == Level::LEVELED || level_policy == Level::LAZY_LEVELED && next->isLastLevel()) {
        next->compactLevel(buffer.size(), bf_error_rate, bf_bitset_size);
    }
    // Update the number of key/value pairs in the next level. 
    next->setKvPairs(next->numKVPairs());

    // Clear the current level and reset the number of key/value pairs to 0
    it->runs.clear();
    it->setKvPairs(0);
}

// Given a key, search the tree for the key. If the key is found, return the value. 
// If the key is not found, return an empty string.
std::unique_ptr<VAL_t> LSMTree::get(KEY_t key) {
    std::unique_ptr<VAL_t> val;
    // if key is not within the range of the available keys, throw an exception
    if (key < KEY_MIN || key > KEY_MAX) {
        throw std::out_of_range("Key is out of range");
    }
    
    // Search the buffer for the key
    val = buffer.get(key);
    // If the key is found in the buffer, return the value
    if (val != nullptr) {
        // Check that val is not the TOMBSTONE
        if (*val == TOMBSTONE) {
            return nullptr;
        }
        return val;
    }

    // If the key is not found in the buffer, search the levels
    for (auto level = levels.begin(); level != levels.end(); level++) {
        // Iterate through the runs in the level and check if the key is in the run
        for (auto run = level->runs.begin(); run != level->runs.end(); run++) {
            val = (*run)->get(key);
            // If the key is found in the run, return the value
            if (val != nullptr) {
                // Check that val is not the TOMBSTONE
                if (*val == TOMBSTONE) {
                    return nullptr;
                }
                return val;
            }
        }
    }
    // If the key is not found in the buffer or the levels, return nullptr
    return nullptr;
}


// Given 2 keys, get a map all the keys from start inclusive to end exclusive. If the range is completely empty then return a nullptr. 
// If the range is not empty, return a map of all the found pairs.
std::unique_ptr<std::map<KEY_t, VAL_t>> LSMTree::range(KEY_t start, KEY_t end) {
    // if either start or end is not within the range of the available keys, throw an exception
    if (start < KEY_MIN || start > KEY_MAX || end < KEY_MIN || end > KEY_MAX) {
        throw std::out_of_range("LSMTree::range: Key is out of range");
    }
    // If the start key is greater than the end key, throw an exception
    if (start > end) {
        throw std::out_of_range("LSMTree::range: Start key is greater than end key");
    }
    // If the start key is equal to the end key, return nullptr
    if (start == end) {
        return nullptr;
    }

    int allPossibleKeys = end - start;

    // Search the buffer for the key range and return the range map as a unique_ptr
    std::unique_ptr<std::map<KEY_t, VAL_t>> range_map = std::make_unique<std::map<KEY_t, VAL_t>>(buffer.range(start, end));

    // Print all the key and value pairs in the range map to cout
    // Create an iterator to iterate through the map
    std::map<KEY_t, VAL_t>::iterator it = range_map->begin();

    // If the range has the size of the entire range, return the range
    if (range_map->size() == allPossibleKeys) {
        // Remove all the TOMBSTONES from the range map
        removeTombstones(range_map);
        return range_map;
    }

    // If all of the keys are not found in the buffer, search the levels
    for (auto level = levels.begin(); level != levels.end(); level++) {
        // Iterate through the runs in the level and check if the range is in the run
        for (auto run = level->runs.begin(); run != level->runs.end(); run++) {
            std::map<KEY_t, VAL_t> temp_map = (*run)->range(start, end);
            // If keys from the range are found in the run, add them to the range map
            if (temp_map.size() != 0) {
                for (const auto &kv : temp_map) {
                    // Only add the key/value pair if the key is not already in the range map
                    range_map->try_emplace(kv.first, kv.second);
                }
            } 
            // If the range map has the size of the entire range, return the range
            if (range_map->size() == allPossibleKeys) {
                removeTombstones(range_map);
                return range_map;
            }
        }
    }
    // Remove all the TOMBSTONES from the range map
    removeTombstones(range_map);
    return range_map;
}

// Given a key, delete the key-value pair from the tree.
void LSMTree::del(KEY_t key) {
    put(key, TOMBSTONE);
}

void LSMTree::load(const std::string& filename) {
    KEY_t key;
    VAL_t val;
    std::ifstream file(filename);
    std::string line;

     // Start measuring time
    auto start_time = std::chrono::high_resolution_clock::now();
    std::cout << "Loading " << filename << std::endl;

    // If file can be located
    if (!file) {
        std::cerr << "Unable to open file " << filename << std::endl;
        exit(1);   // call system to stop
    }
    while (getline(file, line)) {
        std::stringstream ss(line);
        ss >> key >> val;
        put(key, val);
    }
    // End measuring time, calculate the duration, and print it
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "Loading " << filename << " file took " << duration.count() << " microseconds" << std::endl;
}

// Create a set of all the keys in the tree. Start from the bottom level and work up. If an upper level 
// has a key with a TOMBSTONE, remove the key from the set. Return the size of the set.
int LSMTree::countLogicalPairs() {
    // Create a set of all the keys in the tree
    std::set<KEY_t> keys;
    // Create a pointer to a map of key/value pairs
    std::map<KEY_t, VAL_t> kvMap;

    for (auto level = levels.begin(); level != levels.end(); level++) {
        for (auto run = level->runs.begin(); run != level->runs.end(); run++) {
            // Get the map of key/value pairs from the run
            kvMap = (*run)->getMap();
            // Insert the keys from the map into the set. If the key is a TOMBSTONE, check to see if it is in the set and remove it.
            for (auto it = kvMap.begin(); it != kvMap.end(); it++) {
                if (it->second == TOMBSTONE) {
                    if (keys.find(it->first) != keys.end()) {
                        keys.erase(it->first);
                    }
                } else {
                    keys.insert(it->first);
                }
            }
        }  
    }
    // Iterate through the buffer and insert the keys into the set. If the key is a TOMBSTONE, check to see if it is in the set and remove it.
    std::map<KEY_t, VAL_t> bufferContents = buffer.getMap();
    for (auto it = bufferContents.begin(); it != bufferContents.end(); it++) {
        if (it->second == TOMBSTONE) {
            if (keys.find(it->first) != keys.end()) {
                keys.erase(it->first);
            }
        } else {
            keys.insert(it->first);
        }
    }
    // Return the size of the set
    return keys.size();
}

// Print out a summary of the tree.
std::string LSMTree::printStats() {
    std::string output = "";
    // Create a string to hold the number of logical key value pairs in the tree
    std::string logicalPairs = "Logical Pairs: " + std::to_string(countLogicalPairs()) + "\n";
    // Create a string to hold the number of keys in each level of the tree
    std::string levelKeys = "";
    // Create a string to hold the dump of the tree
    std::string treeDump = "";

    // Iterate through the levels and add the number of keys in each level to the levelKeys string
    for (auto it = levels.begin(); it != levels.end(); it++) {
        levelKeys += "LVL" + std::to_string(it->getLevelNum()) + ": " + std::to_string(it->getKvPairs()) + ", ";
    }
    // Remove the last comma and space from the levelKeys string
    levelKeys = levelKeys.substr(0, levelKeys.size() - 2) + "\n";
    // Iterate through the buffer and add the key/value pairs to the treeDump string
    std::map<KEY_t, VAL_t> bufferContents = buffer.getMap();
    for (auto it = bufferContents.begin(); it != bufferContents.end(); it++) {
        if (it->second == TOMBSTONE) {
            treeDump += std::to_string(it->first) + ":TOMBSTONE:L0 ";
        } else {
            treeDump += std::to_string(it->first) + ":" + std::to_string(it->second) + ":L0 ";
        }
    }
    // Iterate through the levels and add the key/value pairs to the treeDump string
    for (auto level = levels.begin(); level != levels.end(); level++) {
        for (auto run = level->runs.begin(); run != level->runs.end(); run++) {
            std::map<KEY_t, VAL_t> kvMap = (*run)->getMap();
            // Insert the keys from the map into the set. If the key is a TOMBSTONE, change the key value to the word TOMBSTONE
            for (auto it = kvMap.begin(); it != kvMap.end(); it++) {
                if (it->second == TOMBSTONE) {
                    treeDump += std::to_string(it->first) + ":TOMBSTONE:L" + std::to_string(level->getLevelNum()) + " ";
                } else {
                    treeDump += std::to_string(it->first) + ":" + std::to_string(it->second) + ":L" + std::to_string(level->getLevelNum()) + " ";
                }
            }
        }
    }
    // Remove the last space from the treeDump string
    treeDump = treeDump.substr(0, treeDump.size() - 1);
    // Add the logicalPairs, levelKeys, and treeDump strings to the output string
    output += logicalPairs + levelKeys + treeDump;
    return output;
}

// Print tree. Print the number of entries in the buffer. Then print the number of levels, then print 
// the number of runs per each level.
std::string LSMTree::printTree() {
    std::string output = "";
    output += "Number of logical key-value pairs: " + std::to_string(countLogicalPairs()) + "\n";
    output += "Number of entries in the buffer: " + std::to_string(buffer.size()) + "\n";
    output += "Number of levels: " + std::to_string(levels.size()) + "\n";
    for (auto it = levels.begin(); it != levels.end(); it++) {
        output += "Number of SSTables in level " + std::to_string(it->getLevelNum()) + ": " + std::to_string(it->runs.size()) + "\n";
        //assert(it->numKVPairs() == it->kv_pairs);
        output += "Number of key-value pairs in level " + std::to_string(it->getLevelNum()) + ": " + std::to_string(it->getKvPairs()) + "\n";
    }
    // For each level, print if it is the last level
    for (auto it = levels.begin(); it != levels.end(); it++) {
        output += "Is level " + std::to_string(it->getLevelNum()) + " the last level? " + (it->isLastLevel() ? "Yes" : "No") + "\n";
    }
    // Remove the last newline from the output string
    output = output.substr(0, output.size() - 1);
    return output;
}

// Remove all TOMBSTONES from a given unique_ptr<map<KEY_t, VAL_t>> range_map
void LSMTree::removeTombstones(std::unique_ptr<std::map<KEY_t, VAL_t>> &range_map) {
    // Create an iterator to iterate through the map
    std::map<KEY_t, VAL_t>::iterator it = range_map->begin();
    // Iterate through the map and remove all TOMBSTONES
    while (it != range_map->end()) {
        if (it->second == TOMBSTONE) {
            it = range_map->erase(it);
        } else {
            it++;
        }
    }
}

json LSMTree::serialize() const {
    json j;
    j["buffer"] = buffer.serialize();
    j["bf_capacity"] = bf_capacity;
    j["bf_error_rate"] = bf_error_rate;
    j["bf_bitset_size"] = bf_bitset_size;
    j["fanout"] = fanout;
    j["level_policy"] = Level::policyToString(level_policy);
    j["levels"] = json::array();
    for (const auto& level : levels) {
        j["levels"].push_back(level.serialize());
    }
    return j;
}

void LSMTree::serializeLSMTreeToFile(const std::string& filename) {
    // Serialize the LSMTree to JSON
    json treeJson = serialize();
    // Write the JSON to file
    std::ofstream outfile(filename);
    outfile << treeJson.dump();
    outfile.close();
}

