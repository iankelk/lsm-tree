#include <set>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <unistd.h>
#include <numeric>
#include <shared_mutex>
#include <algorithm>
#include <filesystem>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/lock_algorithms.hpp>
#include "lsm_tree.hpp"
#include "run.hpp"
#include "utils.hpp"

LSMTree::LSMTree(float bfErrorRate, int buffer_num_pages, int fanout, Level::Policy levelPolicy, size_t numThreads,
                 float compactionPercentage, const std::string& dataDirectory, bool throughputPrinting, size_t throughputFrequency) :
    bfErrorRate(bfErrorRate), fanout(fanout), levelPolicy(levelPolicy), bfFalsePositives(0), bfTruePositives(0),
    buffer(buffer_num_pages * getpagesize() / sizeof(kvPair)), threadPool(numThreads), compactionPercentage(compactionPercentage),
    dataDirectory(dataDirectory), throughputPrinting(throughputPrinting), throughputFrequency(throughputFrequency)
{
    // Create the first level
    levels.emplace_back(std::make_unique<Level>(buffer.getMaxKvPairs(), fanout, levelPolicy, FIRST_LEVEL_NUM, this));
    levelIoCountAndTime.push_back(std::make_pair(0, std::chrono::microseconds()));
    SyncedCout() << "Page size: " << getpagesize() << std::endl;
}

void LSMTree::calculateAndPrintThroughput() {
    double slidingWindowThroughput;
    double overallThroughput;
    uint64_t slidingWindowIo;
    uint64_t overallIo;
    uint64_t currentCounter = ++commandCounter;
    uint64_t elapsedTimeSinceLastReport;
    uint64_t elapsedTimeSinceStart;
    {
        boost::upgrade_lock<boost::upgrade_mutex> upgradeLock(throughputMutex);
        if (!timerStarted) {
            boost::upgrade_to_unique_lock<boost::upgrade_mutex> uniqueLock(upgradeLock);
            startTime = std::chrono::steady_clock::now();
            lastReportTime = startTime;
            timerStarted = true;
            return;
        }
        if (currentCounter % throughputFrequency != 0) {
            return;
        }
        auto currentTime = std::chrono::steady_clock::now();
        elapsedTimeSinceLastReport = std::chrono::duration_cast<std::chrono::microseconds>(currentTime - lastReportTime).count();
        elapsedTimeSinceStart = std::chrono::duration_cast<std::chrono::microseconds>(currentTime - startTime).count();
        
        // Calculate sliding window throughput in commands per second
        slidingWindowThroughput = (static_cast<double>(throughputFrequency) / elapsedTimeSinceLastReport) * 1e6;
        
        // Calculate overall throughput in commands per second
        overallThroughput = (static_cast<double>(currentCounter) / elapsedTimeSinceStart) * 1e6;
        
        // Calculate sliding window and overall I/O counts
        uint64_t currentIoCount = getIoCount();
        slidingWindowIo = currentIoCount - lastReportIoCount;
        overallIo = currentIoCount;

        boost::upgrade_to_unique_lock<boost::upgrade_mutex> uniqueLock(upgradeLock);
        // Update the lastReportTime and lastReportIoCount
        lastReportTime = currentTime;
        lastReportIoCount = currentIoCount;
    }
    SyncedCout() << "Total commands: " << currentCounter 
                 << ", Sliding Window Time: " << std::fixed << std::setprecision(2) << elapsedTimeSinceLastReport / 1e6 
                 << " Throughput: " << std::fixed << std::setprecision(2) << slidingWindowThroughput 
                 << " cps I/O: " << slidingWindowIo
                 << ", Overall Time: " << std::fixed << std::setprecision(2) << elapsedTimeSinceStart / 1e6 
                 << " Throughput: " << std::fixed << std::setprecision(2) << overallThroughput 
                 << " cps I/O: " << overallIo << std::endl;
}




// Insert a key-value pair of integers into the LSM tree
void LSMTree::put(KEY_t key, VAL_t val) {
    size_t bufferMaxKvPairs;
    std::vector<kvPair> bufferVector;

    if (throughputPrinting) {
        calculateAndPrintThroughput();
    }
    std::chrono::high_resolution_clock::time_point start_time;
    std::unique_lock<std::shared_mutex> firstLevelLock;
    {
        boost::unique_lock<boost::upgrade_mutex> lock(numLogicalPairsMutex);
        numLogicalPairs = NUM_LOGICAL_PAIRS_NOT_CACHED;
    }
    {
        std::unique_lock<std::shared_mutex> lock(bufferMutex);
        // Do all buffer operations while protected by the bufferMutex
        if(buffer.put(key, val)) {
            return;
        }
        // Buffer is full
        bufferVector.reserve(buffer.size());
        // Copy the buffer into a vector of kvPairs
        std::transform(buffer.begin(), buffer.end(), std::back_inserter(bufferVector),
                       [](const auto &kv) { return kvPair{kv.first, kv.second}; });
        bufferMaxKvPairs = buffer.getMaxKvPairs();
        buffer.clear();
        buffer.put(key, val);
    }

    // Lock the first level
    firstLevelLock = std::unique_lock<std::shared_mutex>(levels.front()->levelMutex);

    if (!levels.front()->willBufferFit()) {
        std::unique_lock<std::shared_mutex> mrLock(moveRunsMutex);
        moveRuns(FIRST_LEVEL_NUM);
    } else if (levels.front()->runs.size() > 0) {
        if (levelPolicy == Level::LEVELED || (levelPolicy == Level::LAZY_LEVELED && isLastLevel(FIRST_LEVEL_NUM))) {
            std::unique_lock<std::shared_mutex> cpLock(compactionPlanMutex);
            compactionPlan[FIRST_LEVEL_NUM] = std::make_pair<int, int>(0, levels[FIRST_LEVEL_NUM-1]->runs.size());
        }
    }
    start_time = std::chrono::high_resolution_clock::now();

    // Create a new run and add a unique pointer to it to the first level
    levels.front()->put(std::make_unique<Run>(bufferMaxKvPairs, bfErrorRate, true, FIRST_LEVEL_NUM, this));
    // Save the first and last keys for partial compaction
    levels.front()->runs.front()->setFirstAndLastKeys(bufferVector.front().key, bufferVector.back().key);

    // Flush the buffer to level 1
    std::unique_ptr<std::vector<kvPair>> bufferVectorPtr = std::make_unique<std::vector<kvPair>>(std::move(bufferVector));
    levels.front()->runs.front()->flush(std::move(bufferVectorPtr));

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    incrementLevelIoCountAndTime(FIRST_LEVEL_NUM, duration);

    if (getCompactionPlanSize() > 0) {
        // Create a vector to store the upgrade locks
        std::vector<std::unique_lock<std::shared_mutex>> compactionLocks;
        // reserve space for the locks. We don't need to reserve space for the first level because it's already locked.
        compactionLocks.reserve(getCompactionPlanSize() - 1);
        // Lock the levels in the compaction plan
        for (const auto &entry : compactionPlan) {
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
        levelVectorLock = boost::upgrade_lock<boost::upgrade_mutex>(levelsVectorMutex); // Lock for reading the vector. Only lock it once.
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
        newLevel = std::make_unique<Level>(buffer.getMaxKvPairs(), fanout, levelPolicy, currentLevelNum + 1, this);
        
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
        int start, end;
        std::tie(start, end) = segmentBounds;
        auto &level = levels[levelNum - 1];
        auto task = [this, &level, start, end] {
            auto compactedRun = level->compactSegment(bfErrorRate, {start, end}, isLastLevel(level->getLevelNum()));
            level->replaceSegment({start, end}, std::move(compactedRun));
        };
        compactResults.push_back(threadPool.enqueue(task));
    }
    // Wait for all compacting tasks to complete
    threadPool.waitForAllTasks();
}

std::vector<Level*> LSMTree::getLocalLevelsCopy() {
    std::vector<Level*> localLevelsCopy;
    {
        boost::shared_lock<boost::upgrade_mutex> levelVectorLock(levelsVectorMutex);
        // Create a copy of raw pointers to the levels
        localLevelsCopy.resize(levels.size());
        std::transform(levels.begin(), levels.end(), localLevelsCopy.begin(), [](const auto &level) {
            return level.get();
        });
    }
    return localLevelsCopy;
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

// Remove all TOMBSTONES from a given std::unique_ptr<std::vector<kvPair>> rangeResult
void LSMTree::removeTombstones(std::unique_ptr<std::vector<kvPair>> &rangeResult) {
    rangeResult->erase(std::remove_if(rangeResult->begin(), rangeResult->end(),
                                      [](const kvPair &kv) { return kv.value == TOMBSTONE; }),
                       rangeResult->end());
}


// Given a key, search the tree for the key. If the key is found, return the value, otherwise return a nullptr. 
std::unique_ptr<VAL_t> LSMTree::get(KEY_t key) {
    if (throughputPrinting) {
        calculateAndPrintThroughput();
    }
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
// Returns a vector of all the key-value pairs in the range [start, end] or an empty vector if the range is invalid
std::unique_ptr<std::vector<kvPair>> LSMTree::range(KEY_t start, KEY_t end) {
    if (throughputPrinting) {
        calculateAndPrintThroughput();
    }
    std::unique_ptr<std::vector<kvPair>> rangeResult = std::make_unique<std::vector<kvPair>>();
    std::priority_queue<PQEntry> pq;
    std::optional<KEY_t> mostRecentKey;

    // if either start or end is not within the range of the available keys, print to the server stderr and skip it
    if (start < KEY_MIN || start > KEY_MAX || end < KEY_MIN || end > KEY_MAX) {
        SyncedCerr() << "LSMTree::range: Key " << start << " or " << end << " is not within the range of available keys. Skipping..." << std::endl;
        return rangeResult; // Return an empty vector
    }
    // If the start key is greater than the end key, swap them
    if (start > end) {
        SyncedCerr() << "LSMTree::range: Start key is greater than end key. Swapping them..." << std::endl;
        std::swap(start, end);
    }

    // If the start key is equal to the end key, return an empty map
    if (start == end) {
        return rangeResult; // Return an empty vector

    }
    const size_t allPossibleKeys = end - start;
    size_t keysFound = 0;
    bool searchLevels = true;

    {
        std::shared_lock<std::shared_mutex> bufferLock(bufferMutex);
        // Search the buffer for the key range and add the found key-value pairs to the priority queue
        auto bufferResult = buffer.range(start, end);
        for (const auto &kv : bufferResult) {
            pq.push(PQEntry{kv.first, kv.second, 0, {}});
            keysFound++;

            // If the range has the size of the entire range, break the loop
            if (keysFound == allPossibleKeys) {
                searchLevels = false;
                break;
            }
        }
    }
    if (searchLevels) {
        std::vector<Level*> localLevelsCopy = getLocalLevelsCopy();
        std::vector<std::future<std::vector<kvPair>>> futures;

        // Search the levels
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

        // Wait for all tasks to finish and add the results to the priority queue
        for (auto &future : futures) {
            std::vector<kvPair> tempVec = future.get();
            if (!tempVec.empty()) {
                for (const auto &kv : tempVec) {
                    pq.push(PQEntry{kv.key, kv.value, 0, {}});
                }
            }
        }
    }
    // Reset keysFound to 0 since now we're searching everything
    keysFound = 0;
    // Merge the sorted key-value pairs using the priority queue
    while (!pq.empty()) {
        PQEntry top = pq.top();
        pq.pop();
        // Check if the key is the same as the most recent key processed to avoid duplicates
        if (!mostRecentKey.has_value() || mostRecentKey.value() != top.key) {
            rangeResult->push_back(kvPair{top.key, top.value});
            mostRecentKey = top.key; // Update the most recent key processed
            keysFound++;
            // If the range has the size of the entire range, break the loop
            if (keysFound == allPossibleKeys) {
                break;
            }
        }
    }
    // Remove all the TOMBSTONES from the rangeResult
    removeTombstones(rangeResult);

    // Update range hits and misses
    if (rangeResult->empty()) {
        incrementRangeMisses();
    } else {
        incrementRangeHits();
    }
    return rangeResult;
}

// Given a key, delete the key-value pair from the tree.
void LSMTree::del(KEY_t key) {
    put(key, TOMBSTONE);
}

// Benchmark the LSMTree by loading the file into the tree and measuring the time it takes to load the workload.
void LSMTree::benchmark(const std::string& filename, bool verbose, size_t verboseFrequency) {
    int count = 0;
    std::ifstream file(filename);

    if (!file) {
        SyncedCerr() << "Unable to open file " << filename << std::endl;
        return;
    }
    {
        // Restart the timer
        boost::unique_lock<boost::upgrade_mutex> lock(throughputMutex);
        startTime = std::chrono::steady_clock::now();
        lastReportTime = startTime;
        lastReportIoCount = 0;
        timerStarted = true;
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
                range(start, end);
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

// Check whether an iterator is at the last level
bool LSMTree::isLastLevel(std::vector<std::shared_ptr<Level>>::iterator it) {
    return (it + 1 == levels.end());
}
// Check if a level number is the last level
bool LSMTree::isLastLevel(unsigned int levelNum) {
    return (levelNum == levels.size() - 1);
}

// Set the number of logical pairs in the tree by creating a set of all the keys in the tree
std::pair<std::map<KEY_t, VAL_t>, std::vector<Level*>> LSMTree::setNumLogicalPairs() {
    std::map<KEY_t, VAL_t> bufferContents;
    {
        std::shared_lock<std::shared_mutex> bufferLock(bufferMutex);
        bufferContents = buffer.getMap();
    }
    std::vector<Level*> localLevelsCopy = getLocalLevelsCopy();
    
    boost::upgrade_lock<boost::upgrade_mutex> lock(numLogicalPairsMutex);
    if (numLogicalPairs != NUM_LOGICAL_PAIRS_NOT_CACHED) {
        return std::make_pair(buffer.getMap(), getLocalLevelsCopy());
    }

    // Create a set of all the keys in the tree
    std::set<KEY_t> keys;
    
    std::vector<std::future<std::vector<kvPair>>> futures;

    for (auto level = localLevelsCopy.begin(); level != localLevelsCopy.end(); level++) {
        std::shared_lock<std::shared_mutex> levelLock((*level)->levelMutex);
        futures.reserve((*level)->runs.size());

        for (auto run = (*level)->runs.begin(); run != (*level)->runs.end(); run++) {
            // Enqueue task for getting the vector from the run
            futures.push_back(threadPool.enqueue([&, run] {
                return (*run)->getVector();
            }));
        }
    }

    // Wait for all tasks to finish and aggregate the results
    for (auto &future : futures) {
        std::vector<kvPair> kvVec = future.get();
        // Insert the keys from the vector into the set. If the key is a TOMBSTONE, check to see if it is in the set and remove it.
        for (const auto &kv : kvVec) {
            if (kv.value == TOMBSTONE) {
                if (keys.find(kv.key) != keys.end()) {
                    keys.erase(kv.key);
                }
            } else {
                keys.insert(kv.key);
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
    // Return the buffer and localLevelsCopy
    return std::make_pair(buffer.getMap(), getLocalLevelsCopy());
}

// Print out getMisses and rangeMisses stats
void LSMTree::printHitsMissesStats() {
    SyncedCout() << "getHits: " << getGetHits() << std::endl;
    SyncedCout() << "getMisses: " << getGetMisses() << std::endl;
    SyncedCout() << "rangeHits: " << getRangeHits() << std::endl;
    SyncedCout() << "rangeMisses: " << getRangeMisses() << std::endl;
}

// Print out a summary of the tree.
std::string LSMTree::printStats(ssize_t numToPrintFromEachLevel) {
    std::map<KEY_t, VAL_t> bufferContents;
    std::vector<Level*> localLevelsCopy;

    // Call setNumLogicalPairs and unpack the returned pair
    std::tie(bufferContents, localLevelsCopy) = setNumLogicalPairs();

    std::string output = "";
    // Create a string to hold the number of logical key value pairs in the tree
    std::string logicalPairs = "Logical Pairs: " + addCommas(std::to_string(numLogicalPairs)) + "\n";
    std::string levelKeys = "";  // Create a string to hold the number of keys in each level of the tree
    std::string treeDump = "";   // Create a string to hold the dump of the tree

    // Iterate through the buffer and add the key/value pairs to the treeDump string
    ssize_t pairsCounter = 0;
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
            std::vector<kvPair> kvVec = (*run)->getVector();
            // Insert the keys from the vector into the set. If the key is a TOMBSTONE, change the key value to the word TOMBSTONE
            for (const auto &kv : kvVec) {
                if (numToPrintFromEachLevel != STATS_PRINT_EVERYTHING && pairsCounter >= numToPrintFromEachLevel) {
                    break;
                }
                pairsCounter++;
                if (kv.value == TOMBSTONE) {
                    treeDump += std::to_string(kv.key) + ":TOMBSTONE:L" + std::to_string((*level)->getLevelNum()) + " ";
                } else {
                    treeDump += std::to_string(kv.key) + ":" + std::to_string(kv.value) + ":L" + std::to_string((*level)->getLevelNum()) + " ";
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

// Print out information on the LSM tree
std::string LSMTree::printInfo() {
    std::map<KEY_t, VAL_t> bufferContents;
    std::vector<Level*> localLevelsCopy;
    double percentage;

    // Call setNumLogicalPairs and unpack the returned pair
    std::tie(bufferContents, localLevelsCopy) = setNumLogicalPairs();
    std::stringstream output;
    std::stringstream levelDiskSummary;
    std::vector<std::string> levelStrings, keyValueStrings, maxKeyValueStrings, diskNameStrings, multiplierStrings;
    output << std::right;
    levelDiskSummary << std::right;

    std::string bfStatus = getBfFalsePositiveRate() == BLOOM_FILTER_UNUSED ? "Unused" : std::to_string(getBfFalsePositiveRate());
    output << "\nNumber of logical key-value pairs: " + addCommas(std::to_string(numLogicalPairs)) + "\n";
    output << "Bloom filter measured false positive rate: " + bfStatus + "\n";
    output << "Number of I/O operations: " + addCommas(std::to_string(getIoCount())) + "\n";
    percentage = (static_cast<double>(bufferContents.size()) / buffer.getMaxKvPairs()) * 100;
    output << "Number of entries in the buffer: " << addCommas(std::to_string(bufferContents.size()))
           << " (Max " << addCommas(std::to_string(buffer.getMaxKvPairs())) << " entries, or "
           << addCommas(std::to_string(buffer.getMaxKvPairs() * sizeof(kvPair))) << " bytes, "
           << std::to_string(static_cast<int>(percentage)) << "% full)\n\n";

    output << "Number of Levels: " + std::to_string(localLevelsCopy.size()) + "\n\n";

    // Collect the container values for each level
    for (auto it = localLevelsCopy.begin(); it != localLevelsCopy.end(); it++) {
        std::shared_lock<std::shared_mutex> levelLock((*it)->levelMutex);

        levelStrings.push_back(std::to_string((*it)->getLevelNum()));
        keyValueStrings.push_back(addCommas(std::to_string((*it)->getKvPairs())));
        maxKeyValueStrings.push_back(addCommas(std::to_string((*it)->getMaxKvPairs())));
        diskNameStrings.push_back((*it)->getDiskName());
        multiplierStrings.push_back(std::to_string((*it)->getDiskPenaltyMultiplier()));
    }

    // Find the longest strings in each container using the helper function. Additional +2 is for commas and spaces.
    const int levelWidth = getLongestStringLength(levelStrings);
    const int keyValueWidth = getLongestStringLength(keyValueStrings);
    const int maxKeyValueWidth = getLongestStringLength(maxKeyValueStrings) + 2;
    const int diskNameWidth = getLongestStringLength(diskNameStrings) + 2;
    const int multiplierWidth = getLongestStringLength(multiplierStrings) + 2;

    // Iterate through the length of one of the containers and create the output string
    for (size_t i = 0; i < levelStrings.size(); i++) {
        output << "Number of Runs in Level " + levelStrings[i] + ": " + std::to_string(localLevelsCopy[i]->runs.size()) + "\n";
        percentage = (static_cast<double>(localLevelsCopy[i]->getKvPairs()) / localLevelsCopy[i]->getMaxKvPairs()) * 100;
        output << "Number of key-value pairs allocated for level " << std::setw(levelWidth) << levelStrings[i] + ": "
            << std::setw(keyValueWidth) << keyValueStrings[i]
            << " (Max " << std::setw(maxKeyValueWidth) << maxKeyValueStrings[i] + ", "
            << std::to_string(static_cast<int>(percentage)) << "% full)\n\n";

        levelDiskSummary << "Level " << std::setw(levelWidth) << levelStrings[i]
                         << " disk type: " << std::setw(diskNameWidth) << diskNameStrings[i] + ", "
                         << "disk penalty multiplier: " << std::setw(multiplierWidth) << multiplierStrings[i] + ", "
                         << "is it the last level? " << ((i + 1 == localLevelsCopy.size()) ? "Yes" : "No") << "\n";
    }
    output << levelDiskSummary.str();
    return output.str();
}

// Print the I/O count for each level and the total I/O count.
std::string LSMTree::printLevelIoCount() {
    std::vector<Level*> localLevelsCopy = getLocalLevelsCopy();
    std::stringstream output;
    std::stringstream penaltyOutput;

    // Collect the container values for each level
    std::vector<std::string> levelStrings, ioCountStrings, timeStrings, diskNameStrings, multiplierStrings;
    for (auto it = localLevelsCopy.begin(); it != localLevelsCopy.end(); it++) {
        levelStrings.push_back(std::to_string((*it)->getLevelNum()));
        ioCountStrings.push_back(addCommas(std::to_string(getLevelIoCount((*it)->getLevelNum()))));
        timeStrings.push_back(std::to_string(getLevelIoTime((*it)->getLevelNum()).count()));
        diskNameStrings.push_back((*it)->getDiskName());
        multiplierStrings.push_back(std::to_string((*it)->getDiskPenaltyMultiplier()));
    }

    // Find the longest strings in each container using the helper function. Additional 1 or 2 is for commas and spaces.
    const int levelWidth = getLongestStringLength(levelStrings) + 1;
    const int ioCountWidth = getLongestStringLength(ioCountStrings) + 2;
    const int timeWidth = getLongestStringLength(timeStrings);
    const int diskNameWidth = getLongestStringLength(diskNameStrings) + 2;
    const int multiplierWidth = getLongestStringLength(multiplierStrings);

    std::vector<std::string> penaltyTimes;
    size_t totalPenaltyTime = 0;

    output << std::right;
    penaltyOutput << std::right;

    for (size_t i = 0; i < localLevelsCopy.size(); i++) {
        output << "Level" << std::setw(levelWidth) << levelStrings[i]
               << " I/O count: " << std::setw(ioCountWidth) << ioCountStrings[i] + ", "
               << "Disk name: " << std::setw(diskNameWidth) << diskNameStrings[i] + ", "
               << "Disk penalty multiplier: " << std::setw(multiplierWidth) << multiplierStrings[i] + ", "
               << "Microseconds: " << std::setw(timeWidth) << timeStrings[i]
               << " (" << formatMicroseconds(std::stol(timeStrings[i])) << ")\n";

        size_t penaltyTime = std::stol(timeStrings[i]) * std::stol(multiplierStrings[i]);
        penaltyTimes.push_back(std::to_string(penaltyTime));
        totalPenaltyTime += penaltyTime;
    }
    const int penaltyTimeWidth = getLongestStringLength(penaltyTimes);
    
    for (size_t i = 0; i < localLevelsCopy.size(); i++) {
        penaltyOutput << "Level" << std::setw(levelWidth) << levelStrings[i]
                      << " microseconds: " << std::setw(timeWidth) << timeStrings[i]
                      << " x " << std::setw(multiplierWidth) << multiplierStrings[i]
                      << " = " << std::setw(penaltyTimeWidth) << penaltyTimes[i]
                      << " (" << formatMicroseconds(std::stoul(penaltyTimes[i])) << ")\n";
    }

    // Add up all the I/O counts for each level
    output << "Total I/O count (sum of all levels): " << addCommas(std::to_string(getIoCount())) << "\n\n";
    output << "Using the multiplier penalties to simulate slower drives for the higher levels:\n";
    output << penaltyOutput.str();
    output << "\nTotal time with penalties: " << addCommas(std::to_string(totalPenaltyTime)) 
           << " microseconds (" << formatMicroseconds(totalPenaltyTime) << ")\n";
    return output.str();
}

// For each level, list the level number, then for each run in the level list the run number, then call Run::getBloomFilterSummary() to get the bloom filter summary
std::string LSMTree::getBloomFilterSummary() {
    std::vector<Level*> localLevelsCopy = getLocalLevelsCopy();
    std::stringstream output;
    output << std::right;

    std::string bfStatus = getBfFalsePositiveRate() == BLOOM_FILTER_UNUSED ? "Unused" : std::to_string(getBfFalsePositiveRate());
    output << "\nBloom filter total measured FPR: " << bfStatus << "\n";

    std::vector<std::vector<std::map<std::string, std::string>>> summaries(localLevelsCopy.size());
    for (size_t i = 0; i < localLevelsCopy.size(); i++) {
        std::shared_lock<std::shared_mutex> levelLock(localLevelsCopy[i]->levelMutex);
        for (size_t j = 0; j < localLevelsCopy[i]->runs.size(); j++) {
            summaries[i].push_back(localLevelsCopy[i]->runs[j]->getBloomFilterSummary());
        }
    }

    // Set the width for each field/column. Additional + 2 is for commas and spaces.
    const int runWidth = std::to_string(getLongestVectorLength(summaries)).length();
    const int bloomSizeWidth = getLongestStringLength(getMapValuesByKey(summaries, "bloomFilterSize")) + 2;
    const int numHashFunctionsWidth = getLongestStringLength(getMapValuesByKey(summaries, "hashFunctions")) + 2;
    const int keysWidth = getLongestStringLength(getMapValuesByKey(summaries, "keys")) + 2;
    const int fprWidth = getLongestStringLength(getMapValuesByKey(summaries, "theoreticalFPR")) + 2;
    const int tpfpWidth = getLongestStringLength(getMapValuesByKey(summaries, "truePositives")) + 2;

    for (size_t i = 0; i < localLevelsCopy.size(); i++) {
        output << "\nLevel " << i + 1 << ":\n";
        for (size_t j = 0; j < summaries[i].size(); j++) {
            output << "Run " << std::setw(runWidth) << j << ": ";
            output << "Bloom Filter Size: " << std::setw(bloomSizeWidth) << summaries[i][j]["bloomFilterSize"] + ", "
            << "Hash Functions: " << std::setw(numHashFunctionsWidth) << summaries[i][j]["hashFunctions"] + ", "
            << "Number of Keys: " << std::setw(keysWidth) << summaries[i][j]["keys"] + ", "
            << "Theoretical FPR: " << std::setw(fprWidth) << summaries[i][j]["theoreticalFPR"] + ", "
            << "TP: " << std::setw(tpfpWidth) << summaries[i][j]["truePositives"] + ", "
            << "FP: " << std::setw(tpfpWidth) << summaries[i][j]["falsePositives"] + ", "
            << "Measured FPR: " << summaries[i][j]["measuredFPR"] << "\n";
        }
    }

    return output.str();
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
    for (const auto& level : levels) {
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

json LSMTree::serialize() const {
    json j;
    j["buffer"] = buffer.serialize();
    j["bfErrorRate"] = bfErrorRate;
    j["fanout"] = fanout;
    j["compactionPercentage"] = compactionPercentage;
    j["levelPolicy"] = Level::policyToString(levelPolicy);
    j["levels"] = json::array();
    j["bfFalsePositives"] = bfFalsePositives;
    j["bfTruePositives"] = bfTruePositives;
    j["getMisses"] = getMisses;
    j["getHits"] = getHits;
    j["rangeMisses"] = rangeMisses;
    j["rangeHits"] = rangeHits;
    j["levelIoCountAndTime"] = json::array();
    j["commandCounter"] = commandCounter.load();

    for (const auto& lvlIo : levelIoCountAndTime) {
        j["levelIoCountAndTime"].push_back(lvlIo.first);
        j["levelIoCountAndTime"].push_back(lvlIo.second.count());
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

    std::filesystem::path absolute_path = std::filesystem::absolute(filename);
    SyncedCout() << "Previous LSM Tree found! Deserializing LSMTree from file: " << absolute_path << std::endl;

    json treeJson;
    infile >> treeJson;

    bfErrorRate = treeJson["bfErrorRate"].get<float>();
    fanout = treeJson["fanout"].get<int>();
    compactionPercentage = treeJson["compactionPercentage"].get<float>();
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
    commandCounter.store(treeJson["commandCounter"].get<uint64_t>());

    buffer.deserialize(treeJson["buffer"]);

    levels.clear();
    for (const auto& levelJson : treeJson["levels"]) {
        Level *newLevel = new Level();
        levels.emplace_back(newLevel);
        newLevel->deserialize(levelJson, this);
    }
    infile.close();

    // Restore lsm_tree pointers in all Runs
    for (const auto& level : levels) {
        for (auto& run : level->runs) {
            run->setLSMTree(this);
        }
    }
    SyncedCout() << "Finished!\n" << std::endl;
    SyncedCout() << "Command line parameters will be ignored and configuration loaded from the saved database.\n" << std::endl;
}