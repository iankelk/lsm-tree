#include <set>
#include <iostream>
#include <fstream>
#include <sstream>
#include <unistd.h>
#include <numeric>
#include <shared_mutex>
#include <algorithm>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/lock_algorithms.hpp>
#include "lsm_tree.hpp"
#include "run.hpp"
#include "utils.hpp"

LSMTree::LSMTree(float bfErrorRate, int buffer_num_pages, int fanout, Level::Policy levelPolicy, size_t numThreads) :
    bfErrorRate(bfErrorRate), fanout(fanout), levelPolicy(levelPolicy), bfFalsePositives(0), bfTruePositives(0),
    buffer(buffer_num_pages * getpagesize() / sizeof(kvPair)), threadPool(numThreads)
{
    // Create the first level
    levels.emplace_back(std::make_unique<Level>(buffer.getMaxKvPairs(), fanout, levelPolicy, FIRST_LEVEL_NUM, this));
    // levels.emplace_back(buffer->getMaxKvPairs(), fanout, levelPolicy, FIRST_LEVEL_NUM, this);
    levelIoCountAndTime.push_back(std::make_pair(0, std::chrono::microseconds()));
}

// Calculate whether an std::deque<Level>::iterator is pointing to the last level
bool LSMTree::isLastLevel(std::vector<std::shared_ptr<Level>>::iterator it) {
    return (it + 1 == levels.end());
}

bool LSMTree::isLastLevel(int levelNum) {
    return (levelNum == levels.size() - 1);
}

// Insert a key-value pair of integers into the LSM tree
void LSMTree::put(KEY_t key, VAL_t val) {
    size_t bufferMaxKvPairs;
    std::map<KEY_t, VAL_t> bufferContents;

    {
        boost::unique_lock<boost::upgrade_mutex> lock(numLogicalPairsMutex);
        numLogicalPairs = NUM_LOGICAL_PAIRS_NOT_CACHED;
    }
    {
        std::unique_lock<std::shared_mutex> lock(bufferMutex);
        if(buffer.put(key, val)) {
            return;
        }
        // The buffer is full, so do all buffer operations while protected by the bufferMutex
        bufferContents = buffer.getMap();
        bufferMaxKvPairs = buffer.getMaxKvPairs();
        buffer.clear();
        buffer.put(key, val);
    }
    // Lock the first level
    std::unique_lock<std::shared_mutex> firstLevelLock(levels.front()->levelMutex);

    if (!levels.front()->willBufferFit()) {
        std::unique_lock<std::shared_mutex> lock(moveRunsMutex);
        moveRuns(FIRST_LEVEL_NUM);
    } else if (levels.front()->runs.size() > 0) {
        if (levelPolicy == Level::LEVELED || (levelPolicy == Level::LAZY_LEVELED && isLastLevel(FIRST_LEVEL_NUM))) {
            std::unique_lock<std::shared_mutex> lock(compactionPlanMutex);
            compactionPlan[FIRST_LEVEL_NUM] = std::make_pair<int, int>(0, levels[FIRST_LEVEL_NUM-1]->runs.size());
        }
    }
    auto start_time = std::chrono::high_resolution_clock::now();

    // Create a new run and add a unique pointer to it to the first level
    levels.front()->put(std::make_unique<Run>(bufferMaxKvPairs, bfErrorRate, true, 1, this));
    // Flush the buffer to level 1
    for (auto it = bufferContents.begin(); it != bufferContents.end(); it++) {
        levels.front()->runs.front()->put(it->first, it->second);
    }
    // Close the run's file
    levels.front()->runs.front()->closeFile();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    incrementLevelIoCountAndTime(FIRST_LEVEL_NUM, duration);

    if (getCompactionPlanSize() > 0) {
        // Create a vector to store the upgrade locks
        std::vector<std::unique_lock<std::shared_mutex>> compactionLocks;
        // reserve space for the locks. We don't need to reserve space for the first level because it's already locked.
        compactionLocks.reserve(getCompactionPlanSize() - 1);
        // Lock the levels in the compaction plan
        for (auto &entry : compactionPlan) {
            int levelNumber = entry.first;
            if (levelNumber == FIRST_LEVEL_NUM) {
                continue;
            }
            compactionLocks.emplace_back(levels[levelNumber - 1]->levelMutex);
        }
        executeCompactionPlan();
        clearCompactionPlan();
    }
}

// Move runs until the first level has space. Precondition: the currentLevelNum is exclusively locked.
void LSMTree::moveRuns(int currentLevelNum) {
    std::vector<std::shared_ptr<Level>>::iterator it;
    std::vector<std::shared_ptr<Level>>::iterator next;

    // Locks for the specific levels
    std::unique_lock<std::shared_mutex> nextLevelLock; // Lock for the next level

    // Locks for the level vector
    boost::upgrade_lock<boost::upgrade_mutex> levelVectorLock;

    if (currentLevelNum == FIRST_LEVEL_NUM) {
        boost::upgrade_lock<boost::upgrade_mutex> levelVectorLock(levelsMutex); // Lock for reading the vector. Only lock it once.
    }
    it = levels.begin() + currentLevelNum - 1;

    // If the current level has space, we're done
    if ((*it)->willLowerLevelFit()) {
        return;
    }

    // If currentLevelNum is not the last level
    if (it + 1 != levels.end()) {
        nextLevelLock = std::unique_lock<std::shared_mutex>(levels[currentLevelNum]->levelMutex);
        next = it + 1;
        if (!(*next)->willLowerLevelFit()) {
            moveRuns(currentLevelNum + 1);
            it = levels.begin() + currentLevelNum - 1;
            next = levels.begin() + currentLevelNum;
        }
    } else {
        // Upgrade the levelVectorLock to exclusive using boost::upgrade_to_unique_lock
        boost::upgrade_to_unique_lock<boost::upgrade_mutex> uniqueLevelVectorLock(levelVectorLock);
        // Create a new level
        std::unique_ptr<Level> newLevel;
        {
            std::shared_lock<std::shared_mutex> lock(bufferMutex);
            newLevel = std::make_unique<Level>(buffer.getMaxKvPairs(), fanout, levelPolicy, currentLevelNum + 1, this);
        }
        nextLevelLock = std::unique_lock<std::shared_mutex>(newLevel->levelMutex);
        levels.push_back(std::move(newLevel));
        {
            std::unique_lock<std::shared_mutex> lock(levelIoCountAndTimeMutex);
            levelIoCountAndTime.push_back(std::make_pair(0, std::chrono::microseconds()));
            it = levels.end() - 2;
            next = levels.end() - 1;
        }
    } 

    if (levelPolicy != Level::PARTIAL) { // TIERED, LAZY_LEVELED, LEVELED move the whole level to the next level
        int numRuns = (*it)->runs.size();
        if (levelPolicy == Level::TIERED || (levelPolicy == Level::LAZY_LEVELED && !isLastLevel(next))) {
            std::unique_lock<std::shared_mutex> lock(compactionPlanMutex);
            compactionPlan[(*next)->getLevelNum()] = std::make_pair<int, int>(0, numRuns - 1);
        } else if (levelPolicy == Level::LEVELED || (levelPolicy == Level::LAZY_LEVELED && isLastLevel(next))) {
            std::unique_lock<std::shared_mutex> lock(compactionPlanMutex);
            compactionPlan[(*next)->getLevelNum()] = std::make_pair<int, int>(0, (*next)->runs.size() + numRuns - 1);
        } 
        (*next)->runs.insert((*next)->runs.begin(), std::make_move_iterator((*it)->runs.begin()), std::make_move_iterator((*it)->runs.end()));
        (*it)->runs.clear();
        (*it)->setKvPairs(0);
    } else { // PARTIAL moves the best segment of 2 runs to the next level
        auto segmentBounds = (*it)->findBestSegmentToCompact();
        if (!(*it)->willLowerLevelFit()) {
            {
                std::unique_lock<std::shared_mutex> lock(compactionPlanMutex);
                compactionPlan[(*next)->getLevelNum()] = std::make_pair<int, int>(0, segmentBounds.second - segmentBounds.first);
            }
            (*next)->runs.insert((*next)->runs.begin(), std::make_move_iterator((*it)->runs.begin() + segmentBounds.first), std::make_move_iterator((*it)->runs.begin() + segmentBounds.second + 1));
            (*it)->runs.erase((*it)->runs.begin() + segmentBounds.first, (*it)->runs.begin() + segmentBounds.second + 1);
            (*it)->setKvPairs((*it)->addUpKVPairsInLevel());
            (*next)->setKvPairs((*next)->addUpKVPairsInLevel());
        } else {
            std::unique_lock<std::shared_mutex> lock(compactionPlanMutex);
            compactionPlan[currentLevelNum] = segmentBounds;
        } 
    }
}

void LSMTree::executeCompactionPlan() {
    std::unique_lock<std::shared_mutex> lock(compactionPlanMutex);
    std::vector<std::future<void>> compactResults;
    compactResults.reserve(compactionPlan.size());

    for (const auto &[levelNum, segmentBounds] : compactionPlan) {
        int first = segmentBounds.first;
        int second = segmentBounds.second;
        auto &level = levels[levelNum - 1];
        auto task = [this, &level, first, second] {
            auto compactedRun = level->compactSegment(bfErrorRate, {first, second}, isLastLevel(level->getLevelNum()));
            level->replaceSegment({first, second}, std::move(compactedRun));
        };
        compactResults.push_back(threadPool.enqueue(task));
    }
    // Wait for all compacting tasks to complete
    threadPool.waitForAllTasks();
}

std::vector<Level*> LSMTree::getLocalLevelsCopy() {
    std::vector<Level*> localLevelsCopy;
    {
        boost::shared_lock<boost::upgrade_mutex> levelVectorLock(levelsMutex);
        // Create a copy of raw pointers to the levels
        localLevelsCopy.resize(levels.size());
        std::transform(levels.begin(), levels.end(), localLevelsCopy.begin(), [](const auto &level) {
            return level.get();
        });
    }
    return localLevelsCopy;
}

// Given a key, search the tree for the key. If the key is found, return the value, otherwise return a nullptr. 
std::unique_ptr<VAL_t> LSMTree::get(KEY_t key) {
    std::unique_ptr<VAL_t> val;
    // std::vector<Level*> localLevelsCopy;

    // if key is not within the range of the available keys, print to the server stderr and skip it
    if (key < KEY_MIN || key > KEY_MAX) {
        SyncedCerr() << "LSMTree::get: Key " << key << " is not within the range of available keys. Skipping..." << std::endl;
        return nullptr;
    }
    {
        std::shared_lock<std::shared_mutex> bufferLock(bufferMutex);
        val = buffer.get(key);
    }
    if (val != nullptr) {
        incrementGetHits();
        if (*val == TOMBSTONE) {
            return nullptr;
        }
        return val;
    }
    std::vector<Level*> localLevelsCopy = getLocalLevelsCopy();

    // If the key is not found in the buffer, search the levels
    for (auto level = localLevelsCopy.begin(); level != localLevelsCopy.end(); level++) {
        {
            // Lock the level with a shared lock
            std::shared_lock<std::shared_mutex> levelLock((*level)->levelMutex);
            // Iterate through the runs in the level and check if the key is in the run
            for (auto run = (*level)->runs.begin(); run != (*level)->runs.end(); run++) {
                val = (*run)->get(key);
                // If the key is found in the run, break from the inner loop
                if (val != nullptr) {
                    break;
                }
            }
        }
        // If the key is found in any run within the level, break from the outer loop
        if (val != nullptr) {
            incrementGetHits();
            // Check that val is not the TOMBSTONE
            if (*val == TOMBSTONE) {
                return nullptr;
            }
            return val;
        }
    }
    incrementGetMisses();
    return nullptr;  // If the key is not found in the buffer or the levels, return nullptr
}
// Returns a map of all the key-value pairs in the range [start, end] or an empty map if the range is invalid
std::unique_ptr<std::map<KEY_t, VAL_t>> LSMTree::range(KEY_t start, KEY_t end) {
    std::unique_ptr<std::map<KEY_t, VAL_t>> rangeMap;

    // if either start or end is not within the range of the available keys, print to the server stderr and skip it
    if (start < KEY_MIN || start > KEY_MAX || end < KEY_MIN || end > KEY_MAX) {
        SyncedCerr() << "LSMTree::range: Key " << start << " or " << end << " is not within the range of available keys. Skipping..." << std::endl;
        return std::make_unique<std::map<KEY_t, VAL_t>>(); // Return an empty map
    }
    // If the start key is greater than the end key, swap them
    if (start > end) {
        SyncedCerr() << "LSMTree::range: Start key is greater than end key. Swapping them..." << std::endl;
        KEY_t temp = start;
        start = end;
        end = temp;
    }

    // If the start key is equal to the end key, return an empty map
    if (start == end) {
        return std::make_unique<std::map<KEY_t, VAL_t>>(); // Return an empty map
    }
    int allPossibleKeys = end - start;

    {
        std::shared_lock<std::shared_mutex> bufferLock(bufferMutex);
        // Search the buffer for the key range and return the range map as a unique_ptr
        rangeMap = std::make_unique<std::map<KEY_t, VAL_t>>(buffer.range(start, end));
    }

    // If the range has the size of the entire range, return the range
    if (rangeMap->size() == allPossibleKeys) {
        // Remove all the TOMBSTONES from the range map
        removeTombstones(rangeMap);
        incrementRangeHits();
        return rangeMap;
    }
    std::vector<Level*> localLevelsCopy = getLocalLevelsCopy();
    std::vector<std::future<std::map<KEY_t, VAL_t>>> futures;

    // If all of the keys are not found in the buffer, search the levels
    for (auto level = localLevelsCopy.begin(); level != localLevelsCopy.end(); level++) {
        // Lock the level with a shared lock
        std::shared_lock<std::shared_mutex> lock((*level)->levelMutex);
        futures.reserve((*level)->runs.size());

        for (auto run = (*level)->runs.begin(); run != (*level)->runs.end(); run++) {
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
            	// Only add the key-value pair if the key is not already in the range map
                rangeMap->try_emplace(kv.first, kv.second);
            }
        }
        if (rangeMap->size() == allPossibleKeys) {
            removeTombstones(rangeMap);
            incrementRangeHits();
            return rangeMap;
        }
    }
    if (rangeMap->size() == 0) {
        incrementRangeMisses();
    } else {
        incrementRangeHits();
    }
    removeTombstones(rangeMap);
    return rangeMap;
}

// Given a key, delete the key-value pair from the tree.
void LSMTree::del(KEY_t key) {
    put(key, TOMBSTONE);
}

// Print out getMisses and rangeMisses stats
void LSMTree::printHitsMissesStats() {
    SyncedCout() << "getHits: " << getGetHits() << std::endl;
    SyncedCout() << "getMisses: " << getGetMisses() << std::endl;
    SyncedCout() << "rangeHits: " << getRangeHits() << std::endl;
    SyncedCout() << "rangeMisses: " << getRangeMisses() << std::endl;
}

// Benchmark the LSMTree by loading the file into the tree and measuring the time it takes to load the workload.
void LSMTree::benchmark(const std::string& filename, bool verbose, size_t verboseFrequency) {
    int count = 0;
    std::ifstream file(filename);

    if (!file) {
        SyncedCerr() << "Unable to open file " << filename << std::endl;
        return;
    }

    std::stringstream ss;
    ss << file.rdbuf();

    auto start_time = std::chrono::high_resolution_clock::now();
    SyncedCout() << "Benchmark: loaded \"" << filename << "\"" << std::endl;

    std::string line;
    while (std::getline(ss, line)) {
        std::stringstream line_ss(line);
        char command_code;
        line_ss >> command_code;

        std::unique_ptr<std::map<KEY_t, VAL_t>> rangePtr;

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
                rangePtr = range(start, end);
                if (rangePtr->size() > 0) {
                    SyncedCout() << rangePtr->size() << std::endl;
                }
                break;
            }
            default: {
                SyncedCerr() << "Invalid command code: " << command_code << std::endl;
                break;
            }
        }

        count++;
        if (verbose) {
            if (count % verboseFrequency == 0) {
                auto end_time = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
                SyncedCout() << "Benchmark: " << count << " commands executed" << std::endl;
                SyncedCout() << "Benchmark: " << duration.count() << " microseconds elapsed" << std::endl;
            }
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    SyncedCout() << "Benchmark: Workload " << filename << " file took " << duration.count() << " microseconds ("
            << formatMicroseconds(duration.count()) + ") and " << getIoCount() << " I/O operations" << std::endl;
}

void LSMTree::load(const std::string& filename) {
    std::vector<kvPair> kvPairs;
    kvPair kv;
    // Create a file stream
    std::ifstream file(filename, std::ios::binary);

    // Check that the file exists
    if (!file) {
        SyncedCerr() << "Unable to open file " << filename << std::endl;
        return;
    }
    // Read the file into the vector of kvPair structs.
    while (file.read((char*)&kv, sizeof(kvPair))) {
        kvPairs.push_back(kv);
    }
    SyncedCout() << "Loaded: " << filename << std::endl;
    // Start measuring time. This way we only measure the time it takes put() to insert the key/value pairs
    auto start_time = std::chrono::high_resolution_clock::now();
    // Iterate through the vector and call put() on each key/value pair
    for (auto it = kvPairs.begin(); it != kvPairs.end(); it++) {
        put(it->key, it->value);
    }
    // End measuring time, calculate the duration, and print it
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    SyncedCout() << "Processing " << filename << " file took " << duration.count() << " microseconds ("
    << formatMicroseconds(duration.count()) + ") and " << getIoCount() << " I/O operations" << std::endl;
}

std::tuple<size_t, std::map<KEY_t, VAL_t>, std::vector<Level*>> LSMTree::countLogicalPairs() {
    std::map<KEY_t, VAL_t> bufferContents;
    {
        std::shared_lock<std::shared_mutex> bufferLock(bufferMutex);
        bufferContents = buffer.getMap();
    }
    std::vector<Level*> localLevelsCopy = getLocalLevelsCopy();
    
    boost::upgrade_lock<boost::upgrade_mutex> lock(numLogicalPairsMutex);
    if (numLogicalPairs != NUM_LOGICAL_PAIRS_NOT_CACHED) {
        return std::make_tuple(numLogicalPairs, buffer.getMap(), getLocalLevelsCopy());
    }

    // Create a set of all the keys in the tree
    std::set<KEY_t> keys;
    
    std::vector<std::future<std::map<KEY_t, VAL_t>>> futures;

    for (auto level = localLevelsCopy.begin(); level != localLevelsCopy.end(); level++) {
        std::shared_lock<std::shared_mutex> levelLock((*level)->levelMutex);
        futures.reserve((*level)->runs.size());

        for (auto run = (*level)->runs.begin(); run != (*level)->runs.end(); run++) {
            // Enqueue task for getting the map from the run
            futures.push_back(threadPool.enqueue([&, run] {
                return (*run)->getMap();
            }));
        }
    }

    // Wait for all tasks to finish and aggregate the results
    for (auto &future : futures) {
        std::map<KEY_t, VAL_t> kvMap = future.get();
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

    // Iterate through the buffer and insert the keys into the set. If the key is a TOMBSTONE, check to see if it is in the set and remove it.
    for (auto it = bufferContents.begin(); it != bufferContents.end(); it++) {
        if (it->second == TOMBSTONE) {
            if (keys.find(it->first) != keys.end()) {
                keys.erase(it->first);
            }
        } else {
            keys.insert(it->first);
        }
    }
    {
        boost::upgrade_to_unique_lock<boost::upgrade_mutex> uniqueLock(lock);
        numLogicalPairs = keys.size();
    }
    // Return the size of the set
    return std::make_tuple(keys.size(), buffer.getMap(), getLocalLevelsCopy());
}

// Print out a summary of the tree.
std::string LSMTree::printStats(size_t numToPrintFromEachLevel) {
    size_t numLogicalPairs;
    std::map<KEY_t, VAL_t> bufferContents;
    std::vector<Level*> localLevelsCopy;

    // Call countLogicalPairs and unpack the returned tuple
    std::tie(numLogicalPairs, bufferContents, localLevelsCopy) = countLogicalPairs();

    std::string output = "";
    // Create a string to hold the number of logical key value pairs in the tree
    std::string logicalPairs = "Logical Pairs: " + addCommas(std::to_string(numLogicalPairs)) + "\n";
    std::string levelKeys = "";  // Create a string to hold the number of keys in each level of the tree   
    std::string treeDump = "";   // Create a string to hold the dump of the tree

    // Iterate through the buffer and add the key/value pairs to the treeDump string
    size_t pairsCounter = 0;
    for (auto it = bufferContents.begin(); it != bufferContents.end(); it++) {
        if (numToPrintFromEachLevel != STATS_PRINT_EVERYTHING && pairsCounter >= numToPrintFromEachLevel) {
            break;
        }
        if (it->second == TOMBSTONE) {
            treeDump += std::to_string(it->first) + ":TOMBSTONE:L0 ";
        } else {
            treeDump += std::to_string(it->first) + ":" + std::to_string(it->second) + ":L0 ";
        }
        pairsCounter++;
    }
    if (pairsCounter > 0) {
        treeDump += "\n\n";
    }
    // Iterate through the levels and add the key/value pairs to the treeDump string
    for (auto level = localLevelsCopy.begin(); level != localLevelsCopy.end(); level++) {
        std::shared_lock<std::shared_mutex> levelLock((*level)->levelMutex);
        levelKeys += "LVL" + std::to_string((*level)->getLevelNum()) + ": " + std::to_string((*level)->getKvPairs()) + ", ";
        pairsCounter = 0;
        for (auto run = (*level)->runs.begin(); run != (*level)->runs.end(); run++) {
            std::map<KEY_t, VAL_t> kvMap = (*run)->getMap();
            // Insert the keys from the map into the set. If the key is a TOMBSTONE, change the key value to the word TOMBSTONE
            for (auto it = kvMap.begin(); it != kvMap.end(); it++) {
                if (numToPrintFromEachLevel != STATS_PRINT_EVERYTHING && pairsCounter >= numToPrintFromEachLevel) {
                    break;
                }
                pairsCounter++;
                if (it->second == TOMBSTONE) {
                    treeDump += std::to_string(it->first) + ":TOMBSTONE:L" + std::to_string((*level)->getLevelNum()) + " ";
                } else {
                    treeDump += std::to_string(it->first) + ":" + std::to_string(it->second) + ":L" + std::to_string((*level)->getLevelNum()) + " ";
                }
            }
        }
        if (pairsCounter > 0) {
            treeDump += "\n\n";
        }
    }

    // Remove the last comma and space from the levelKeys string
    levelKeys.resize(levelKeys.size() - 2);
    levelKeys += "\n";

    treeDump.pop_back(); // Remove the last space from the treeDump string
    output += logicalPairs + levelKeys + treeDump;
    return output;
}

// Print tree. Print the number of entries in the buffer. Then print the number of levels, then print 
// the number of runs per each level.
std::string LSMTree::printTree() {
    size_t numLogicalPairs;
    std::map<KEY_t, VAL_t> bufferContents;
    std::vector<Level*> localLevelsCopy;
    double percentage;

    // Call countLogicalPairs and unpack the returned tuple
    std::tie(numLogicalPairs, bufferContents, localLevelsCopy) = countLogicalPairs();
    std::string output = "";
    std::string levelDiskSummary = "";
    std::string bfStatus = getBfFalsePositiveRate() == BLOOM_FILTER_UNUSED ? "Unused" : std::to_string(getBfFalsePositiveRate());
    output += "\nNumber of logical key-value pairs: " + addCommas(std::to_string(numLogicalPairs)) + "\n";
    output += "Bloom filter measured false positive rate: " + bfStatus + "\n";
    output += "Number of I/O operations: " + addCommas(std::to_string(getIoCount())) + "\n";
    percentage = (static_cast<double>(bufferContents.size()) / buffer.getMaxKvPairs()) * 100;
    output += "Number of entries in the buffer: " + addCommas(std::to_string(bufferContents.size()))
               + " (Max " + addCommas(std::to_string(buffer.getMaxKvPairs())) + " entries, or "
               + addCommas(std::to_string(buffer.getMaxKvPairs() * sizeof(kvPair))) + " bytes, "
               + std::to_string(static_cast<int>(percentage)) + "% full)\n\n";

    output += "Number of Levels: " + std::to_string(localLevelsCopy.size()) + "\n\n";
    for (auto it = localLevelsCopy.begin(); it != localLevelsCopy.end(); it++) {
        std::shared_lock<std::shared_mutex> levelLock((*it)->levelMutex);
        output += "Number of Runs in Level " + std::to_string((*it)->getLevelNum()) + ": " + std::to_string((*it)->runs.size()) + "\n";
        percentage = (static_cast<double>((*it)->getKvPairs()) / (*it)->getMaxKvPairs()) * 100;
        output += "Number of key-value pairs in level " + std::to_string((*it)->getLevelNum()) + ": "
                   + addCommas(std::to_string((*it)->getKvPairs()))
                   + " (Max " + addCommas(std::to_string((*it)->getMaxKvPairs())) + ", "
                   + std::to_string(static_cast<int>(percentage)) + "% full)\n\n";

        levelDiskSummary += "Level " + std::to_string((*it)->getLevelNum()) + " disk type: " + (*it)->getDiskName()
                             + ", disk penalty multiplier: " + std::to_string((*it)->getDiskPenaltyMultiplier())
                             + ", is it the last level? " + ((it + 1 == localLevelsCopy.end()) ? "Yes" : "No") + "\n";
    }
    output += levelDiskSummary;
    return output;
}

// Print the I/O count for each level and the total I/O count.
std::string LSMTree::printLevelIoCount() {
    std::vector<Level*> localLevelsCopy = getLocalLevelsCopy();
    std::string output = "";
    std::string penaltyOutput = "";
    size_t penaltyTime;
    size_t totalPenaltyTime = 0;
    for (auto it = localLevelsCopy.begin(); it != localLevelsCopy.end(); it++) {
        output += "Level " + std::to_string((*it)->getLevelNum()) + " I/O count: " + addCommas(std::to_string(getLevelIoCount((*it)->getLevelNum()))) + ", Microseconds: " + 
                   std::to_string(getLevelIoTime((*it)->getLevelNum()).count()) + " (" + formatMicroseconds(getLevelIoTime((*it)->getLevelNum()).count()) + "), Disk name: " +
                   (*it)->getDiskName() + ", Disk penalty multiplier: " + std::to_string((*it)->getDiskPenaltyMultiplier()) + "\n";
        penaltyTime = getLevelIoTime((*it)->getLevelNum()).count() * (*it)->getDiskPenaltyMultiplier();
        totalPenaltyTime += penaltyTime;
        penaltyOutput += "Level " + std::to_string((*it)->getLevelNum()) + " microseconds: " + std::to_string(getLevelIoTime((*it)->getLevelNum()).count()) + " x " + 
                         std::to_string((*it)->getDiskPenaltyMultiplier()) + " = " + std::to_string(penaltyTime) + " (" + formatMicroseconds(penaltyTime) + ")\n"; 
    }
    // Add up all the I/O counts for each level
    output += "Total I/O count (sum of all levels): " + addCommas(std::to_string(getIoCount())) + "\n\n";
    output + "Using the multiplier penalties to simulate slower drives for the higher levels:\n";
    output += penaltyOutput;
    output += "\nTotal time with penalties: " + std::to_string(totalPenaltyTime) + "microseconds (" + formatMicroseconds(totalPenaltyTime) + ")\n";

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
    std::shared_lock<std::shared_mutex> lockF(bfFalsePositivesMutex);
    std::shared_lock<std::shared_mutex> lockT(bfTruePositivesMutex);
    size_t total = bfFalsePositives + bfTruePositives;
    if (total > 0) {
        return (float)bfFalsePositives / total;
    } else {
        return BLOOM_FILTER_UNUSED; // No false positives or true positives
    }
}

// For each level, list the level number, then for each run in the level list the run number, then call Run::getBloomFilterSummary() to get the bloom filter summary
std::string LSMTree::getBloomFilterSummary() {
    std::vector<Level*> localLevelsCopy = getLocalLevelsCopy();
    std::string output = "";
    for (auto it = localLevelsCopy.begin(); it != localLevelsCopy.end(); it++) {
        output += "Level " + std::to_string((*it)->getLevelNum()) + ":\n";
        std::shared_lock<std::shared_mutex> levelLock((*it)->levelMutex);
        for (auto run = (*it)->runs.begin(); run != (*it)->runs.end(); run++) {
            output += "Run " + std::to_string(std::distance((*it)->runs.begin(), run)) + ": ";
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
    j["getMisses"] = getMisses;
    j["getHits"] = getHits;
    j["rangeMisses"] = rangeMisses;
    j["rangeHits"] = rangeHits;
    j["levelIoCountAndTime"] = json::array();
    for (const auto& levelIoCountAndTime : levelIoCountAndTime) {
        j["levelIoCountAndTime"].push_back(levelIoCountAndTime.first);
        j["levelIoCountAndTime"].push_back(levelIoCountAndTime.second.count());
    }
    for (const auto& level : levels) {
        j["levels"].push_back(level->serialize());
    }
    return j;
}

void LSMTree::serializeLSMTreeToFile(const std::string& filename) {
    SyncedCout() << "Writing LSMTree to file: " << filename << std::endl;
    // Serialize the LSMTree to JSON
    json treeJson = serialize();
    // Write the JSON to file
    std::ofstream outfile(filename);
    outfile << treeJson.dump();
    outfile.close();
    SyncedCout() << "Finished writing LSMTree to file: " << filename << std::endl;
}

void LSMTree::deserialize(const std::string& filename) {
    std::ifstream infile(filename);
    if (!infile) {
        SyncedCerr() << "No file " << filename << " found or unable to open it. Creating fresh database." << std::endl;
        return;
    }

    SyncedCout() << "Previous LSM Tree found! Deserializing LSMTree from file: " << filename << std::endl;

    json treeJson;
    infile >> treeJson;

    bfErrorRate = treeJson["bfErrorRate"].get<float>();
    fanout = treeJson["fanout"].get<int>();
    levelPolicy = Level::stringToPolicy(treeJson["levelPolicy"].get<std::string>());
    bfFalsePositives = treeJson["bfFalsePositives"].get<size_t>();
    bfTruePositives = treeJson["bfTruePositives"].get<size_t>();
    levelIoCountAndTime = std::vector<std::pair<size_t, std::chrono::microseconds>>();
    for (size_t i = 0; i < treeJson["levelIoCountAndTime"].size(); i += 2) {
        levelIoCountAndTime.emplace_back(treeJson["levelIoCountAndTime"][i].get<size_t>(), 
        std::chrono::microseconds(treeJson["levelIoCountAndTime"][i + 1].get<size_t>()));
    }
    getMisses = treeJson["getMisses"].get<size_t>();
    getHits = treeJson["getHits"].get<size_t>();
    rangeMisses = treeJson["rangeMisses"].get<size_t>();
    rangeHits = treeJson["rangeHits"].get<size_t>();

    buffer.deserialize(treeJson["buffer"]);

    levels.clear();
    for (const auto& levelJson : treeJson["levels"]) {
        levels.emplace_back();
        levels.back()->deserialize(levelJson, this);
    }
    infile.close();

    // Restore lsm_tree pointers in all Runs
    for (auto& level : levels) {
        for (auto& run : level->runs) {
            run->setLSMTree(this);
        }
    }
    SyncedCout() << "Finished!\n" << std::endl;
    SyncedCout() << "Command line parameters will be ignored and configuration loaded from the saved database.\n" << std::endl;
}

// Get the total amount of bits currently used by all runs in the LSMTree
size_t LSMTree::getTotalBits() const {
    size_t totalBits = 0;
    for (auto it = levels.begin(); it != levels.end(); it++) {
        totalBits += std::accumulate((*it)->runs.begin(), (*it)->runs.end(), size_t(0),
            [](size_t sum, const std::unique_ptr<Run>& run) {
                return sum + run->getBloomFilterNumBits();
            });
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

    // Flatten the tree structure into a single runs vector and zero out all the bits
    std::vector<Run*> allRuns;
    for (auto& level : levels) {
        for (auto& runPtr : level->runs) {
            runPtr->setBloomFilterNumBits(0);
            allRuns.push_back(runPtr.get());
        }
    }
    levels.front()->runs.front()->setBloomFilterNumBits(mFilters);
    double R = allRuns.size() - 1 + eval(levels.front()->runs.front()->getBloomFilterNumBits(), levels.front()->runs.front()->getSize());

    while (delta >= 1) {
        double rNew = R;
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
    SyncedCout() << "Total bits: " << totalBits << std::endl;
    double R = AutotuneFilters(totalBits);
    SyncedCout() << "Total cost R: " << R << std::endl;
    for (auto it = levels.begin(); it != levels.end(); it++) {
        for (auto run = (*it)->runs.begin(); run != (*it)->runs.end(); run++) {
            (*run)->resizeBloomFilterBitset((*run)->getBloomFilterNumBits());
            (*run)->populateBloomFilter();
        }
    }
    SyncedCout() << "\nNew Bloom Filter summaries:" << std::endl;
    SyncedCout() << getBloomFilterSummary() << std::endl;
}

size_t LSMTree::getLevelIoCount(int levelNum) {
    std::shared_lock<std::shared_mutex> lock(levelIoCountAndTimeMutex);
    return levelIoCountAndTime[levelNum-1].first;
}

std::chrono::microseconds LSMTree::getLevelIoTime(int levelNum) {
    std::shared_lock<std::shared_mutex> lock(levelIoCountAndTimeMutex);
    return levelIoCountAndTime[levelNum-1].second;
}

void LSMTree::incrementLevelIoCountAndTime(int levelNum, std::chrono::microseconds duration) { 
    std::unique_lock<std::shared_mutex> lock(levelIoCountAndTimeMutex);
    levelIoCountAndTime[levelNum-1].first++;
    levelIoCountAndTime[levelNum-1].second += duration;
}

size_t LSMTree::getIoCount() { 
    std::shared_lock<std::shared_mutex> lock(levelIoCountAndTimeMutex);
    size_t ioCount = std::accumulate(levelIoCountAndTime.begin(), levelIoCountAndTime.end(), 0,
    [](size_t acc, const std::pair<size_t, std::chrono::microseconds>& p) {
        return acc + p.first;
    });
    return ioCount;
}

void LSMTree::incrementBfFalsePositives() { 
    std::unique_lock<std::shared_mutex> lock(bfFalsePositivesMutex);
    bfFalsePositives++;
}
void LSMTree::incrementBfTruePositives() {
    std::unique_lock<std::shared_mutex> lock(bfTruePositivesMutex);
    bfTruePositives++; 
}

void LSMTree::incrementGetHits() { 
    std::unique_lock<std::shared_mutex> lock(getHitsMutex);
    getHits++;
}

void LSMTree::incrementGetMisses() { 
    std::unique_lock<std::shared_mutex> lock(getMissesMutex);
    getMisses++;
}

void LSMTree::incrementRangeMisses() { 
    std::unique_lock<std::shared_mutex> lock(rangeMissesMutex);
    rangeMisses++;
}
void LSMTree::incrementRangeHits() { 
    std::unique_lock<std::shared_mutex> lock(rangeHitsMutex);
    rangeHits++;
}

size_t LSMTree::getCompactionPlanSize() {
    std::shared_lock<std::shared_mutex> lock(compactionPlanMutex);
    return compactionPlan.size();
}

void LSMTree::clearCompactionPlan() {
    std::unique_lock<std::shared_mutex> lock(compactionPlanMutex);
    compactionPlan.clear();
}

size_t LSMTree::getGetHits() const { 
    std::shared_lock<std::shared_mutex> lock(getHitsMutex);
    return getHits;
}
size_t LSMTree::getGetMisses() const { 
    std::shared_lock<std::shared_mutex> lock(getMissesMutex);
    return getMisses;
}
size_t LSMTree::getRangeHits() const { 
    std::shared_lock<std::shared_mutex> lock(rangeHitsMutex);
    return rangeHits;
}
size_t LSMTree::getRangeMisses() const { 
    std::shared_lock<std::shared_mutex> lock(rangeMissesMutex);
    return rangeMisses;
}
 