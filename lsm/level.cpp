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

// Dump the contents of the level
void Level::dump() {
    std::cout << "Level: " << std::endl;
    std::cout << "  number of runs: " << runs.size() << std::endl;
    std::cout << "  kvPairs: " << kvPairs << std::endl;
    std::cout << "  maxKvPairs: " << maxKvPairs << std::endl;
    std::cout << "  bufferSize: " << bufferSize << std::endl;
    std::cout << "  fanout: " << fanout << std::endl;
    std::cout << "  levelNum: " << levelNum <<std:: endl;
    std::cout << "  levelPolicy: " << levelPolicy << std::endl;
    std::cout << "  runs: " << std::endl;
    std::cout << "  diskName: " << diskName << std::endl;
    std::cout << "  diskPenaltyMultiplier: " << diskPenaltyMultiplier << std::endl;
    // Iterate through the runs in the level
    for (auto &run : runs) {
        // Print out the map of the run returned by getMap()
        for (auto kv : run->getMap()) {
            std::cout << "    " << kv.first << " : " << kv.second << " : L" << levelNum << std::endl;
        }
    }
}

void Level::compactLevel(double errorRate, State state, bool isLastLevel) {
    long size;
    // Nothing to compact if there is only one run
    if (runs.size() == 1) {
        return;
    }
    if (state == State::FULL) {
        size = getLevelSize(levelNum);
    }
    else if (state == State::TWO_RUNS) {
        size = runs[0]->getMaxKvPairs() + runs[1]->getMaxKvPairs();
    }
    else {
        size = addUpKVPairsInLevel();
    }
    // Create a new map to hold the merged data
    std::map<KEY_t, VAL_t> mergedMap;

    // Iterate through the runs in the level
    for  (const auto& run : runs) {     
        // Get the map of the run
        std::map<KEY_t, VAL_t> run_map = run->getMap();
        // print the size of the run_map
        //cout << "Size of run_map: " << run_map.size() << endl;
        // Iterate through the key-value pairs in the run map
        for (const auto &kv : run_map) {
            // Check if the key is already in the merged map
            if (const auto &[it, inserted] = mergedMap.try_emplace(kv.first, kv.second); !inserted) {
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
    // Clear the runs queue and reset the kvPairs
    runs.clear();
    kvPairs = 0;

    put(std::make_unique<Run>(size, errorRate, true, lsmTree));

    // Iterate through the merged map and add the key-value pairs to the run
    for (const auto &kv : mergedMap) {
        // Check if the key-value pair is a tombstone and if the level is the last level
        if (!(isLastLevel && kv.second == TOMBSTONE)) {
            runs.front()->put(kv.first, kv.second);
        }
    }
    runs.front()->closeFile();
}

void Level::compactSegment(double errorRate, size_t segStartIdx, size_t segEndIdx, bool isLastLevel) {
    std::map<KEY_t, VAL_t> mergedMap;

    for (size_t idx = segStartIdx; idx < segEndIdx; ++idx) {
        std::map<KEY_t, VAL_t> run_map = runs[idx]->getMap();
        for (const auto &kv : run_map) {
            if (const auto &[it, inserted] = mergedMap.try_emplace(kv.first, kv.second); !inserted) {
                it->second = kv.second;
            }
        }
    }

    // Delete all the old files in the segment and remove the runs that have now been compacted
    for (size_t idx = segStartIdx; idx < segEndIdx; ++idx) {
        runs[idx]->deleteFile();
    }
    runs.erase(runs.begin() + segStartIdx, runs.begin() + segEndIdx);

    // Decrement kvPairs in level by the number of kvPairs in the merged_map
    kvPairs -= std::accumulate(runs.begin() + segStartIdx, runs.begin() + segEndIdx, 0, [](int total, const auto& run) { return total + run->getMaxKvPairs(); });

    // Create a new run with the merged data
    auto merged_run = std::make_unique<Run>(mergedMap.size(), errorRate, true, lsmTree);
    for (const auto &kv : mergedMap) {
        // Check if the key-value pair is a tombstone and if the level is the last level
        if (!(isLastLevel && kv.second == TOMBSTONE)) {
            merged_run->put(kv.first, kv.second);
        }
    }
    merged_run->closeFile();

    // Insert the new run into the segment
    runs.insert(runs.begin() + segStartIdx, std::move(merged_run));

    // Increment kvPairs in level by the number of kvPairs in the merged_map
    kvPairs += mergedMap.size();
}

// If we haven't cached the size of a level, calculate it and cache it. Otherwise return the cached value.
long Level::getLevelSize(int levelNum) {
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
int Level::addUpKVPairsInLevel() {
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
long Level::getKvPairs() const {
    return kvPairs;
}
// Set the number of kvPairs in the level
void Level::setKvPairs(long kvPairs) {
    this->kvPairs = kvPairs;
}

// Return the maximum number of key-value pairs allowed in the memtable
long Level::getMaxKvPairs() const {
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

void Level::deserialize(const json& j) {
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
        std::unique_ptr<Run> run = std::make_unique<Run>(maxKvPairs, DEFAULT_ERROR_RATE, false, lsmTree);
        run->deserialize(runJson);
        runs.emplace_back(std::move(run));
    }
}
