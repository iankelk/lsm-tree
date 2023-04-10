#include <set>
#include <iostream>
#include <fstream>
#include <sstream>
#include <unistd.h>

#include "lsm_tree.hpp"
#include "run.hpp"
#include "utils.hpp"

LSMTree::LSMTree(float bfErrorRate, int buffer_num_pages, int fanout, Level::Policy levelPolicy, size_t numThreads) :
    bfErrorRate(bfErrorRate), fanout(fanout), levelPolicy(levelPolicy), bfFalsePositives(0), bfTruePositives(0),
    buffer(buffer_num_pages * getpagesize() / sizeof(kvPair)), threadPool(numThreads)
{
    // Create the first level
    levels.emplace_back(buffer.getMaxKvPairs(), fanout, levelPolicy, FIRST_LEVEL_NUM, this);
}

// Calculate whether an std::vector<Level>::iterator is pointing to the last level
bool LSMTree::isLastLevel(std::vector<Level>::iterator it) {
    return (it + 1 == levels.end());
}

// Insert a key-value pair of integers into the LSM tree
void LSMTree::put(KEY_t key, VAL_t val) {
    // Try to insert the key-value pair into the buffer. If it succeeds, return.
    if(buffer.put(key, val)) {
        return;
    }

    // Buffer is full, so check to see if the first level has space for the buffer. If not, merge the levels recursively
    if (!levels.front().willBufferFit()) {
        mergeLevels(FIRST_LEVEL_NUM);
    }
    
    // Create a new run and add a unique pointer to it to the first level
    levels.front().put(std::make_unique<Run>(buffer.getMaxKvPairs(), bfErrorRate, true, this));

    // Flush the buffer to level 1
    std::map<KEY_t, VAL_t> bufferContents = buffer.getMap();
    for (auto it = bufferContents.begin(); it != bufferContents.end(); it++) {
        levels.front().runs.front()->put(it->first, it->second);
    }

    // Close the run's file
    levels.front().runs.front()->closeFile();

    // If levelPolicy is Level::LEVELED, or if levelPolicy is Level::LAZY_LEVELED and there is only one level, compact the first level
    if (levelPolicy == Level::LEVELED || (levelPolicy == Level::LAZY_LEVELED && isLastLevel(levels.begin()))) {
        levels.front().compactLevel(bfErrorRate, levelPolicy == Level::LEVELED ? Level::TWO_RUNS : Level::UNKNOWN, true);
    }

    // Clear the buffer and add the key-value pair to it
    buffer.clear();
    buffer.put(key, val);
}


// Given an iterator for the levels vector, check to see if there is space to insert another run
// If the next level does not have space for the current level, recursively merge the next level downwards
// until a level has space for a new run in it.
void LSMTree::mergeLevels(int currentLevelNum) {
    std::vector<Level>::iterator it;
    std::vector<Level>::iterator next;

    // Get the iterator for the current level and subtract 1 since the levels vector is 0-indexed
    it = levels.begin() + currentLevelNum - 1;

    // If the current level has space for another run, return
    if (it->willLowerLevelFit()) {
        return;
    } else {
        // If we have reached the end of the levels vector, create a new level. Move the next pointer to the new level.
        if (it + 1 == levels.end()) {
            levels.emplace_back(buffer.getMaxKvPairs(), fanout, levelPolicy, currentLevelNum + 1, this);
            // Get the iterators "it" and "next" which are lost when the vector is resized
            it = levels.end() - 2;
            next = levels.end() - 1;
        // If the next level has space for another run, move the next pointer to the next level
        } else {
            next = it + 1;
            if (!next->willLowerLevelFit()) {
                mergeLevels(currentLevelNum + 1);
                // These iterators need to be reset when returning from the recursion in case the levels vector is resized
                it = levels.begin() + currentLevelNum - 1;
                next = levels.begin() + currentLevelNum;
            }
        }
    }
    
    std::mutex runs_mutex;
    std::atomic<size_t> tasksCompleted(0);
    std::condition_variable tasksCompletedCond;

    auto task1 = [&]() {
        {
            std::unique_lock<std::mutex> lock(runs_mutex);
            it->compactLevel(bfErrorRate, Level::FULL, isLastLevel(it));
            next->runs.push_back(std::move(it->runs.front()));
        }
        tasksCompleted.fetch_add(1);
        tasksCompletedCond.notify_one();
    };

    auto task2 = [&]() {
        {
            std::unique_lock<std::mutex> lock(runs_mutex);
            next->runs.insert(next->runs.end(), std::make_move_iterator(it->runs.begin()), std::make_move_iterator(it->runs.end()));
            next->compactLevel(bfErrorRate, levelPolicy == Level::LEVELED ? Level::TWO_RUNS : Level::UNKNOWN, isLastLevel(next));
        }
        tasksCompleted.fetch_add(1);
        tasksCompletedCond.notify_one();
    };

    if (levelPolicy == Level::TIERED || (levelPolicy == Level::LAZY_LEVELED && !isLastLevel(next))) {
        threadPool.enqueue(task1);
    }
    if (levelPolicy == Level::LEVELED || (levelPolicy == Level::LAZY_LEVELED && isLastLevel(next))) {
        threadPool.enqueue(task2);
    }

    if (levelPolicy == Level::TIERED || levelPolicy == Level::LEVELED || levelPolicy == Level::LAZY_LEVELED) {
        std::unique_lock<std::mutex> lock(runs_mutex);
        tasksCompletedCond.wait(lock, [&]() { return tasksCompleted.load() >= 1; });
    }

    if (levelPolicy == Level::PARTIAL) {
        auto segmentBounds = it->findBestSegmentToCompact();
        // If the segment is not the entire level, compact it
        if (segmentBounds.second - segmentBounds.first < it->runs.size()) {
            // Compact and replace the segment with the compacted run
            auto compactedRun = it->compactSegment(bfErrorRate, segmentBounds.first, segmentBounds.second, isLastLevel(it));
            it->replaceSegment(segmentBounds.first, segmentBounds.second, std::move(compactedRun));
        }
         // If the current level is still full after compaction, move the compacted run to the next level
        if (!it->willLowerLevelFit()) {
            next->runs.push_back(std::move(it->runs.back()));
            it->runs.pop_back();
            it->setKvPairs(it->addUpKVPairsInLevel());
            next->setKvPairs(next->addUpKVPairsInLevel());
        }
    }
    // Clear the current level and reset the number of key/value pairs to 0 if the policy is not Level::PARTIAL
    if (levelPolicy != Level::PARTIAL) {
        // Update the number of key/value pairs in the next level.
        next->setKvPairs(next->addUpKVPairsInLevel());
        it->runs.clear();
        it->setKvPairs(0);
    }
}

// Given a key, search the tree for the key. If the key is found, return the value. 
// If the key is not found, return an empty string.
std::unique_ptr<VAL_t> LSMTree::get(KEY_t key) {
    std::unique_ptr<VAL_t> val;
    // if key is not within the range of the available keys, print to the server stderr and skip it
    if (key < KEY_MIN || key > KEY_MAX) {
        std::cerr << "LSMTree::get: Key " << key << " is not within the range of available keys. Skipping..." << std::endl;
        return nullptr;
    }
    
    // Search the buffer for the key
    val = buffer.get(key);
    // If the key is found in the buffer, return the value
    if (val != nullptr) {
        // Check that val is not the TOMBSTONE
        if (*val == TOMBSTONE) {
            getMisses++;
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
                    getMisses++;
                    return nullptr;
                }
                return val;
            }
        }
    }
    getMisses++;
    return nullptr;  // If the key is not found in the buffer or the levels, return nullptr
}

// Given 2 keys, get a map all the keys from start inclusive to end exclusive. If the range is completely empty then return a nullptr. 
// If the range is not empty, return a map of all the found pairs.
std::unique_ptr<std::map<KEY_t, VAL_t>> LSMTree::range(KEY_t start, KEY_t end) {
    
    // if either start or end is not within the range of the available keys, print to the server stderr and skip it
    if (start < KEY_MIN || start > KEY_MAX || end < KEY_MIN || end > KEY_MAX) {
        std::cerr << "LSMTree::range: Key " << start << " or " << end << " is not within the range of available keys. Skipping..." << std::endl;
        return nullptr;
    }
    // If the start key is greater than the end key, swap them
    if (start > end) {
        std::cerr << "LSMTree::range: Start key is greater than end key. Swapping them..." << std::endl;    
        KEY_t temp = start;
        start = end;
        end = temp;
    }

    // If the start key is equal to the end key, return nullptr
    if (start == end) {
        return nullptr;
    }

    int allPossibleKeys = end - start;

    // Search the buffer for the key range and return the range map as a unique_ptr
    std::unique_ptr<std::map<KEY_t, VAL_t>> rangeMap = std::make_unique<std::map<KEY_t, VAL_t>>(buffer.range(start, end));

    // If the range has the size of the entire range, return the range
    if (rangeMap->size() == allPossibleKeys) {
        // Remove all the TOMBSTONES from the range map
        removeTombstones(rangeMap);
        return rangeMap;
    }

    // If all of the keys are not found in the buffer, search the levels
    for (auto level = levels.begin(); level != levels.end(); level++) {
        // Iterate through the runs in the level and check if the range is in the run
        for (auto run = level->runs.begin(); run != level->runs.end(); run++) {
            std::map<KEY_t, VAL_t> tempMap = (*run)->range(start, end);
            // If keys from the range are found in the run, add them to the range map
            if (tempMap.size() != 0) {
                for (const auto &kv : tempMap) {
                    // Only add the key/value pair if the key is not already in the range map
                    rangeMap->try_emplace(kv.first, kv.second);
                }
            } 
            // If the range map has the size of the entire range, return the range
            if (rangeMap->size() == allPossibleKeys) {
                removeTombstones(rangeMap);
                return rangeMap;
            }
        }
    }
    // Remove all the TOMBSTONES from the range map
    removeTombstones(rangeMap);
    if (rangeMap->size() == 0) {
        rangeMisses++;
    }
    return rangeMap;
}

// Given 2 keys, get a map all the keys from start inclusive to end exclusive. If the range is completely empty then return a nullptr. 
// If the range is not empty, return a map of all the found pairs.
std::unique_ptr<std::map<KEY_t, VAL_t>> LSMTree::cRange(KEY_t start, KEY_t end) {
    
    // if either start or end is not within the range of the available keys, print to the server stderr and skip it
    if (start < KEY_MIN || start > KEY_MAX || end < KEY_MIN || end > KEY_MAX) {
        std::cerr << "LSMTree::range: Key " << start << " or " << end << " is not within the range of available keys. Skipping..." << std::endl;
        return nullptr;
    }
    // If the start key is greater than the end key, swap them
    if (start > end) {
        std::cerr << "LSMTree::range: Start key is greater than end key. Swapping them..." << std::endl;    
        KEY_t temp = start;
        start = end;
        end = temp;
    }

    // If the start key is equal to the end key, return nullptr
    if (start == end) {
        return nullptr;
    }

    int allPossibleKeys = end - start;

    // Search the buffer for the key range and return the range map as a unique_ptr
    std::unique_ptr<std::map<KEY_t, VAL_t>> rangeMap = std::make_unique<std::map<KEY_t, VAL_t>>(buffer.range(start, end));

    // If the range has the size of the entire range, return the range
    if (rangeMap->size() == allPossibleKeys) {
        // Remove all the TOMBSTONES from the range map
        removeTombstones(rangeMap);
        return rangeMap;
    }

    std::vector<std::future<std::map<KEY_t, VAL_t>>> futures;

    for (auto level = levels.begin(); level != levels.end(); level++) {
        for (auto run = level->runs.begin(); run != level->runs.end(); run++) {
            // Enqueue task for searching in the run
            futures.push_back(threadPool.enqueue([&, run] {
                return (*run)->range(start, end);
            }));
        }
    }

    // Wait for all tasks to finish and aggregate the results
    for (auto &future : futures) {
        std::map<KEY_t, VAL_t> tempMap = future.get();
        if (tempMap.size() != 0) {
            for (const auto &kv : tempMap) {
                rangeMap->try_emplace(kv.first, kv.second);
            }
        }
        if (rangeMap->size() == allPossibleKeys) {
            removeTombstones(rangeMap);
            return rangeMap;
        }
    }

    removeTombstones(rangeMap);
    if (rangeMap->size() == 0) {
        rangeMisses++;
    }
    return rangeMap;
}


// Given a key, delete the key-value pair from the tree.
void LSMTree::del(KEY_t key) {
    put(key, TOMBSTONE);
}

// Print out getMisses and rangeMisses stats
void LSMTree::printMissesStats() {
    std::cout << "getMisses: " << getMisses << std::endl;
    std::cout << "rangeMisses: " << rangeMisses << std::endl;
}

// Benchmark the LSMTree by loading the file into the tree and measuring the time it takes to load the workload.
void LSMTree::benchmark(const std::string& filename, bool verbose, bool concurrent) {
    int count = 0;
    std::ifstream file(filename);

    if (!file) {
        std::cerr << "Unable to open file " << filename << std::endl;
        return;
    }

    std::stringstream ss;
    ss << file.rdbuf();

    auto start_time = std::chrono::high_resolution_clock::now();
    std::cout << "Benchmark: loaded \"" << filename << "\" and concurrent is " << concurrent << std::endl;

    std::string line;
    while (std::getline(ss, line)) {
        std::stringstream line_ss(line);
        char command_code;
        line_ss >> command_code;

        switch (command_code) {
            case 'p': {
                KEY_t key;
                VAL_t val;
                line_ss >> key >> val;
                put(key, val);
                break;
            }
            case 'd': {
                KEY_t key;
                line_ss >> key;
                del(key);
                break;
            }
            case 'g': {
                KEY_t key;
                line_ss >> key;
                get(key);
                break;
            }
            case 'r': {
                KEY_t start;
                KEY_t end;
                line_ss >> start >> end;
                
                concurrent ? cRange(start, end) : range(start, end);
                break;
            }
            default: {
                std::cerr << "Invalid command code: " << command_code << std::endl;
                break;
            }
        }

        count++;
        if (verbose) {
            if (count % BENCHMARK_REPORT_FREQUENCY == 0) {
                auto end_time = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
                std::cout << "Benchmark: " << count << " commands executed" << std::endl;
                std::cout << "Benchmark: " << duration.count() << " microseconds elapsed" << std::endl;
            }
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "Benchmark: Workload " << filename << " file took " << duration.count() << " microseconds ("
            << formatMicroseconds(duration.count()) + ") and " << getIoCount() << " I/O operations" << std::endl;
}

void LSMTree::load(const std::string& filename) {
    std::vector<kvPair> kvPairs;
    kvPair kv;
    // Create a file stream
    std::ifstream file(filename, std::ios::binary);

    // Check that the file exists
    if (!file) {
        std::cerr << "Unable to open file " << filename << std::endl;
        return;
    }
    // Read the file into the vector of kvPair structs.
    while (file.read((char*)&kv, sizeof(kvPair))) {
        kvPairs.push_back(kv);
    }
    std::cout << "Loaded: " << filename << std::endl;
    // Start measuring time. This way we only measure the time it takes put() to insert the key/value pairs
    auto start_time = std::chrono::high_resolution_clock::now();
    // Iterate through the vector and call put() on each key/value pair
    for (auto it = kvPairs.begin(); it != kvPairs.end(); it++) {
        put(it->key, it->value);
    }
    // End measuring time, calculate the duration, and print it
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "Processing " << filename << " file took " << duration.count() << " microseconds ("
    << formatMicroseconds(duration.count()) + ") and " << getIoCount() << " I/O operations" << std::endl;
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
    std::string logicalPairs = "Logical Pairs: " + addCommas(std::to_string(countLogicalPairs())) + "\n";
    std::string levelKeys = "";  // Create a string to hold the number of keys in each level of the tree   
    std::string treeDump = ""; // Create a string to hold the dump of the tree

    // Iterate through the levels and add the number of keys in each level to the levelKeys string
    for (auto it = levels.begin(); it != levels.end(); it++) {
        levelKeys += "LVL" + std::to_string(it->getLevelNum()) + ": " + std::to_string(it->getKvPairs()) + ", ";
    }
    // Remove the last comma and space from the levelKeys string
    levelKeys.resize(levelKeys.size() - 2);
    levelKeys += "\n";
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
    treeDump.pop_back(); // Remove the last space from the treeDump string
    output += logicalPairs + levelKeys + treeDump;
    return output;
}

// Print tree. Print the number of entries in the buffer. Then print the number of levels, then print 
// the number of runs per each level.
std::string LSMTree::printTree() {
    std::string output = "";
    std::string bfStatus = getBfFalsePositiveRate() == BLOOM_FILTER_UNUSED ? "Unused" : std::to_string(getBfFalsePositiveRate());
    output += "Number of logical key-value pairs: " + addCommas(std::to_string(countLogicalPairs())) + "\n";
    output += "Bloom filter false positive rate: " + bfStatus + "\n";
    output += "Number of I/O operations: " + addCommas(std::to_string(ioCount)) + "\n";
    output += "Number of entries in the buffer: " + addCommas(std::to_string(buffer.size())) + "\n";
    output += "Maximum number of key-value pairs in the buffer: " + addCommas(std::to_string(buffer.getMaxKvPairs())) + "\n";
    output += "Maximum size in bytes of the buffer: " + addCommas(std::to_string(buffer.getMaxKvPairs() * sizeof(kvPair))) + "\n";
    output += "Number of levels: " + std::to_string(levels.size()) + "\n";
    for (auto it = levels.begin(); it != levels.end(); it++) {
        output += "Number of SSTables in level " + std::to_string(it->getLevelNum()) + ": " + std::to_string(it->runs.size()) + "\n";
        output += "Number of key-value pairs in level " + std::to_string(it->getLevelNum()) + ": " + addCommas(std::to_string(it->getKvPairs())) + "\n";
        output += "Max number of key-value pairs in level " + std::to_string(it->getLevelNum()) + ": " + addCommas(std::to_string(it->getMaxKvPairs())) + "\n";
    }
    // For each level, print if it is the last level
    for (auto it = levels.begin(); it != levels.end(); it++) {
        output += "Level " + std::to_string(it->getLevelNum()) + " disk type: " + it->getDiskName() + ", disk penalty multiplier: " + 
                   std::to_string(it->getDiskPenaltyMultiplier()) + ", is it the last level? " + (isLastLevel(it) ? "Yes" : "No") + "\n";
    }
    // Remove the last newline from the output string
    output.pop_back();
    return output;
}

// Remove all TOMBSTONES from a given unique_ptr<map<KEY_t, VAL_t>> rangeMap
void LSMTree::removeTombstones(std::unique_ptr<std::map<KEY_t, VAL_t>> &rangeMap) {
    // Create an iterator to iterate through the map
    std::map<KEY_t, VAL_t>::iterator it = rangeMap->begin();
    // Iterate through the map and remove all TOMBSTONES
    while (it != rangeMap->end()) {
        if (it->second == TOMBSTONE) {
            it = rangeMap->erase(it);
        } else {
            ++it;
        }
    }
}

float LSMTree::getBfFalsePositiveRate() {
    int total = bfFalsePositives + bfTruePositives;
    if (total > 0) {
        return (float)bfFalsePositives / total;
    } else {
        return BLOOM_FILTER_UNUSED; // No false positives or true positives
    }
}

// For each level, list the level number, then for each run in the level list the run number, then call Run::getBloomFilterSummary() to get the bloom filter summary
std::string LSMTree::getBloomFilterSummary() {
    std::string output = "";
    for (auto it = levels.begin(); it != levels.end(); it++) {
        output += "Level " + std::to_string(it->getLevelNum()) + ":\n";
        for (auto run = it->runs.begin(); run != it->runs.end(); run++) {
            output += "Run " + std::to_string(std::distance(it->runs.begin(), run)) + ": ";
            output += (*run)->getBloomFilterSummary()+ "\n";
        }
    }
    // Remove the last newline from the output string
    output.pop_back();
    return output;
}


json LSMTree::serialize() const {
    json j;
    j["buffer"] = buffer.serialize();
    j["bfErrorRate"] = bfErrorRate;
    j["fanout"] = fanout;
    j["levelPolicy"] = Level::policyToString(levelPolicy);
    j["levels"] = json::array();
    j["bfFalsePositives"] = bfFalsePositives;
    j["bfTruePositives"] = bfTruePositives;
    j["ioCount"] = ioCount;
    j["getMisses"] = getMisses;
    j["rangeMisses"] = rangeMisses;
    for (const auto& level : levels) {
        j["levels"].push_back(level.serialize());
    }
    return j;
}

void LSMTree::serializeLSMTreeToFile(const std::string& filename) {
    std::cout << "Writing LSMTree to file: " << filename << std::endl;
    // Serialize the LSMTree to JSON
    json treeJson = serialize();
    // Write the JSON to file
    std::ofstream outfile(filename);
    outfile << treeJson.dump();
    outfile.close();
    std::cout << "Finished writing LSMTree to file: " << filename << std::endl;
}

void LSMTree::deserialize(const std::string& filename) {
    std::ifstream infile(filename);
    if (!infile) {
        std::cerr << "No file " << filename << " found or unable to open it. Creating fresh database." << std::endl;
        return;
    }

    std::cout << "Previous LSM Tree found! Deserializing LSMTree from file: " << filename << std::endl;

    json treeJson;
    infile >> treeJson;

    bfErrorRate = treeJson["bfErrorRate"].get<float>();
    fanout = treeJson["fanout"].get<int>();
    levelPolicy = Level::stringToPolicy(treeJson["levelPolicy"].get<std::string>());
    bfFalsePositives = treeJson["bfFalsePositives"].get<long long>();
    bfTruePositives = treeJson["bfTruePositives"].get<long long>();
    ioCount = treeJson["ioCount"].get<long long>();
    getMisses = treeJson["getMisses"].get<long long>();
    rangeMisses = treeJson["rangeMisses"].get<long long>();

    buffer.deserialize(treeJson["buffer"]);

    levels.clear();
    for (const auto& levelJson : treeJson["levels"]) {
        levels.emplace_back();
        levels.back().deserialize(levelJson);
    }
    infile.close();

    // Restore lsm_tree pointers in all Runs
    for (auto& level : levels) {
        for (auto& run : level.runs) {
            run->setLSMTree(this);
        }
    }
    std::cout << "Finished!\n" << std::endl;
    std::cout << "Command line parameters will be ignored and configuration loaded from the saved database.\n" << std::endl;
}

// Get the total amount of bits currently used by all runs in the LSMTree
size_t LSMTree::getTotalBits() const {
    size_t totalBits = 0;
    for (auto it = levels.begin(); it != levels.end(); it++) {
        for (auto run = it->runs.begin(); run != it->runs.end(); run++) {
            totalBits += (*run)->getBloomFilterNumBits();
        }
    }
    return totalBits;
}

double LSMTree::TrySwitch(Run* run1, Run* run2, size_t delta, double R) const {
    size_t run1Bits = run1->getBloomFilterNumBits();
    size_t run2Bits = run2->getBloomFilterNumBits();

    size_t run1Entries = run1->getSize();
    size_t run2Entries = run2->getSize();

    double rNew = R - eval(run1Bits, run1Entries)
                - eval(run2Bits, run2Entries)
                + eval(run1Bits + delta, run1Entries)
                + eval(run2Bits - delta, run2Entries);
    
    if ((rNew < R) && ((run2Bits - delta) > 0)) {
        R = rNew;
        run1->setBloomFilterNumBits(run1Bits + delta);
        run2->setBloomFilterNumBits(run2Bits - delta);
    }
    return R;
}

double LSMTree::eval(size_t bits, size_t entries) const {
    return std::exp(static_cast<double>(-static_cast<int64_t>(bits)) / static_cast<double>(entries) * std::pow(std::log(2), 2));
}

double LSMTree::AutotuneFilters(size_t mFilters) {
    size_t delta = mFilters;
    double rNew;

    // Flatten the tree structure into a single runs vector and zero out all the bits
    std::vector<Run*> allRuns;
    for (auto& level : levels) {
        for (auto& runPtr : level.runs) {
            runPtr->setBloomFilterNumBits(0);
            allRuns.push_back(runPtr.get());
        }
    }
    levels.front().runs.front()->setBloomFilterNumBits(mFilters);
    double R = allRuns.size() - 1 + eval(levels.front().runs.front()->getBloomFilterNumBits(), levels.front().runs.front()->getSize());

    while (delta >= 1) {
        rNew = R;
        // Iterate through every run in the whole tree
        for (size_t i = 0; i < allRuns.size() - 1; i++) {
            for (size_t j = i + 1; j < allRuns.size(); j++) {
                rNew = TrySwitch(allRuns[i], allRuns[j], delta, std::min(R, rNew));
                rNew = TrySwitch(allRuns[j], allRuns[i], delta, std::min(R, rNew));
            }
        }

        if (rNew == R) {
            delta /= 2;
        } else {
            R = rNew;
        }
    }
    return R;
}

void LSMTree::monkeyOptimizeBloomFilters() {
    size_t totalBits = getTotalBits();
    std::cout << "Total bits: " << totalBits << std::endl;
    double R = AutotuneFilters(totalBits);
    std::cout << "Total cost R: " << R << std::endl;
    for (auto it = levels.begin(); it != levels.end(); it++) {
        for (auto run = it->runs.begin(); run != it->runs.end(); run++) {
            (*run)->resizeBloomFilterBitset((*run)->getBloomFilterNumBits());
            (*run)->populateBloomFilter();
        }
    }
    std::cout << "\nNew Bloom Filter summaries:" << std::endl;
    std::cout << getBloomFilterSummary() << std::endl;
}
