#include <iostream>
#include "level.hpp"
#include "run.hpp"
#include "lsm_tree.hpp"
#include "utils.hpp"

// Add run to the beginning of the Level runs queue 
void Level::put(std::unique_ptr<Run> runPtr) {
    // Check if there is enough space in the level to add the run
    if (kvPairs + runPtr->getMaxKvPairs() > maxKvPairs) {
        die("Level::put: Attempted to add run to level with insufficient space");
    }
    runs.emplace_front(std::move(runPtr));
    // Increment the kvPairs in the level
    kvPairs += runs.front()->getMaxKvPairs(); 
}

// Merge the key-value pairs from the segment runs, eliminating duplicate keys and reducing the overall size of the level.
// The compacted data is then stored in a new run, which replaces the original runs in the segment.
std::unique_ptr<Run> Level::compactSegment(double errorRate, std::pair<size_t, size_t> segmentBounds, bool isLastLevel) {
    std::map<KEY_t, VAL_t> mergedMap;
    size_t newMaxKvPairs = 0;

    // Iterate through the runs in the segment and merge the data
    for (size_t idx = segmentBounds.first; idx <= segmentBounds.second; ++idx) {
        std::map<KEY_t, VAL_t> runMap = runs[idx]->getMap();
        for (const auto &kv : runMap) {
            if (const auto &[it, inserted] = mergedMap.try_emplace(kv.first, kv.second); !inserted) {
                it->second = kv.second;
            }
        }
        newMaxKvPairs += runs[idx]->getMaxKvPairs();
    }

    // Start the timer for the query
    auto start_time = std::chrono::high_resolution_clock::now();

    // Create a new run with the merged data
    auto compactedRun = std::make_unique<Run>(newMaxKvPairs, errorRate, true, levelNum, lsmTree);
    for (const auto &kv : mergedMap) {
        if (!(isLastLevel && kv.second == TOMBSTONE)) {
            compactedRun->put(kv.first, kv.second);
        }
    }
    compactedRun->closeFile();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    //lsmTree->incrementIoCount();
    lsmTree->incrementLevelIoCountAndTime(levelNum, duration);

    return compactedRun;
}

void Level::replaceSegment(std::pair<size_t, size_t> segmentBounds, std::unique_ptr<Run> compactedRun) {
    // Close and delete files for old runs in the segment
    for (size_t idx = segmentBounds.first; idx <= segmentBounds.second; ++idx) {
        runs[idx]->closeFile();
        runs[idx]->deleteFile();
    }

    // Replace the old runs with the compacted one
    runs.erase(runs.begin() + segmentBounds.first, runs.begin() + segmentBounds.second + 1);
    runs.insert(runs.begin() + segmentBounds.first, std::move(compactedRun));

    // Update the number of key-value pairs in the level
    setKvPairs(addUpKVPairsInLevel());
}

// If we haven't cached the size of a level, calculate it and cache it. Otherwise return the cached value.
size_t Level::getLevelSize(int levelNum) {
    // Check if the levelNum - 1 is in the levelSizes map. 
    // If it is, get the level size from the map. If not, calculate it and cache it
    if (levelSizes.find(levelNum) != levelSizes.end()) {
        return levelSizes[levelNum];
    } else {
        long level_size = pow(fanout, levelNum) * bufferSize;
        levelSizes[levelNum] = level_size;
        return level_size;
    }
}
// Iterate through the runs of the level, calculating the difference between the last key of a run and the first
// key of the next run. Return the start and end indices of the segment with the minimum key difference.
std::pair<size_t, size_t> Level::findBestSegmentToCompact() {
    size_t bestStartIdx = 0;
    size_t bestEndIdx = 1;
    long best_diff = LONG_MAX;

    for (size_t idx = 0; idx < runs.size() - 1; ++idx) {
        long diff = labs(runs[idx]->getMap().rbegin()->first - runs[idx + 1]->getMap().begin()->first);
        if (diff < best_diff) {
            best_diff = diff;
            bestStartIdx = idx;
            bestEndIdx = idx + 1;
        }
    }
    return {bestStartIdx, bestEndIdx};
}

// Returns true if there is enough space in the level to flush the buffer
bool Level::willBufferFit() {
    // Check if the sum of the current level's runs' kvPairs and the buffer size is less than or equal to this level's maxKvPairs
    return (kvPairs + bufferSize <= maxKvPairs);
}

// Returns true if there is enough space in the level to add a run with maxKvPairs
bool Level::willLowerLevelFit() {
    // Get the previous level number, or 1 if this is the first level
    int prevLevel = (levelNum - 2) > 0 ? (levelNum - 2) : 1;
    // Get the size of the previous level
    long prevLevelSize = getLevelSize(prevLevel);
    return (kvPairs + prevLevelSize <= maxKvPairs);
}

// Count the number of kvPairs in a level by accumulating the runs queue
size_t Level::addUpKVPairsInLevel() {
    return std::accumulate(runs.begin(), runs.end(), 0, [](int total, const auto& run) { return total + run->getMaxKvPairs(); });
}

// Returns the level number
int Level::getLevelNum() const {
    return levelNum;
}

Level::Policy Level::getLevelPolicy() const {
    return levelPolicy;
}
// Get the number of kvPairs in the level
size_t Level::getKvPairs() const {
    return kvPairs;
}
// Set the number of kvPairs in the level
void Level::setKvPairs(long kvPairs) {
    this->kvPairs = kvPairs;
}

// Return the maximum number of key-value pairs allowed in the memtable
size_t Level::getMaxKvPairs() const {
    return maxKvPairs;
}

std::string Level::getDiskName() const {
    return diskName;
}

int Level::getDiskPenaltyMultiplier() const {
    return diskPenaltyMultiplier;
}

json Level::serialize() const {
    json j;
    j["maxKvPairs"] = maxKvPairs;
    j["bufferSize"] = bufferSize;
    j["fanout"] = fanout;
    j["levelNum"] = levelNum;
    j["levelPolicy"] = policyToString(levelPolicy);
    j["kvPairs"] = kvPairs;
    j["runs"] = json::array();
    j["diskName"] = diskName;
    j["diskPenaltyMultiplier"] = diskPenaltyMultiplier;
    for (const auto& run : runs) {
        j["runs"].push_back(run->serialize());
    }
    return j;
}

void Level::deserialize(const json& j, LSMTree* lsmTreePtr) {
    bufferSize = j["bufferSize"];
    fanout = j["fanout"];
    levelNum = j["levelNum"];
    maxKvPairs = j["maxKvPairs"];
    kvPairs = j["kvPairs"];
    diskName = j["diskName"];
    diskPenaltyMultiplier = j["diskPenaltyMultiplier"];

    std::string policy_str = j["levelPolicy"];
    levelPolicy = stringToPolicy(policy_str);

    for (const auto& runJson : j["runs"]) {
        std::unique_ptr<Run> run = std::make_unique<Run>(maxKvPairs, DEFAULT_ERROR_RATE, false, levelNum, lsmTree);
        run->deserialize(runJson);
        runs.emplace_back(std::move(run));
    }

    // Set the lsmTree pointer for the level
    lsmTree = lsmTreePtr;
}
