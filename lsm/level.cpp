#include <thread>
#include <iostream>
#include "level.hpp"
#include "run.hpp"
#include "lsm_tree.hpp"
#include "utils.hpp"

// Add run to the beginning of the Level runs queue 
void Level::put(std::unique_ptr<Run> runPtr) {
    // Check if there is enough space in the level to add the run
    if (kvPairs + runPtr->getMaxKvPairs() > maxKvPairs) {
        printTrace();
        die("Level::put: Attempted to add run to level with insufficient space");
    }
    runs.emplace_front(std::move(runPtr));
    // Increment the kvPairs in the level
    kvPairs += runs.front()->getMaxKvPairs(); 
}

std::unique_ptr<Run> Level::compactSegment(double errorRate, std::pair<size_t, size_t> segmentBounds, bool isLastLevel) {
    std::priority_queue<PQEntry> pq;
    size_t newMaxKvPairs = 0;
    std::vector<std::vector<kvPair>> runVectors(segmentBounds.second - segmentBounds.first + 1);
    std::optional<KEY_t> mostRecentKey;

    // Iterate through the runs in the segment, retrieve their vectors, and add the first element of each run to the priority queue
    for (size_t idx = segmentBounds.first; idx <= segmentBounds.second; ++idx) {
        runVectors[idx - segmentBounds.first] = runs[idx]->getVector();
        std::vector<kvPair> &runVec = runVectors[idx - segmentBounds.first];
        if (!runVec.empty()) {
            pq.push(PQEntry{runVec[0].key, runVec[0].value, idx, runVec.begin()});
        }
        newMaxKvPairs += runs[idx]->getMaxKvPairs();
    }

    // Start the timer for the query
    auto start_time = std::chrono::high_resolution_clock::now();

    // Create a new run with the merged data
    auto compactedRun = std::make_unique<Run>(newMaxKvPairs, errorRate, true, levelNum, lsmTree);

    // Create a vector to accumulate key-value pairs
    std::vector<kvPair> compactedKvPairs;
    compactedKvPairs.reserve(newMaxKvPairs);

    // Merge the sorted runs using the priority queue
    while (!pq.empty()) {
        PQEntry top = pq.top();
        pq.pop();

        if (!(isLastLevel && top.value == TOMBSTONE)) {
            // Check if the key is the same as the most recent key processed to avoid duplicates
            if (!mostRecentKey.has_value() || mostRecentKey.value() != top.key) { 
                compactedKvPairs.push_back({top.key, top.value});
                mostRecentKey = top.key; // Update the most recent key processed
            }
        }

        // Add the next element from the same run to the priority queue
        ++top.vecIter;
        const std::vector<kvPair> &runVec = runVectors[top.runIdx - segmentBounds.first];
        if (top.vecIter != runVec.end()) {
            pq.push(PQEntry{top.vecIter->key, top.vecIter->value, top.runIdx, top.vecIter});
        }
    }
    // Flush the accumulated key-value pairs to the compactedRun
    std::unique_ptr<std::vector<kvPair>> kvPairsPtr = std::make_unique<std::vector<kvPair>>(std::move(compactedKvPairs));
    compactedRun->flush(std::move(kvPairsPtr));

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    lsmTree->incrementLevelIoCountAndTime(levelNum, duration);

    return compactedRun;
}

void Level::replaceSegment(std::pair<size_t, size_t> segmentBounds, std::unique_ptr<Run> compactedRun) {
    // Close and delete files for old runs in the segment
    for (size_t idx = segmentBounds.first; idx <= segmentBounds.second; ++idx) {
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

// Calculate the sum of key differences for a range of runs
long Level::sumOfKeyDifferences(size_t start, size_t end) {
    long sum = 0;
    for (size_t i = start; i < end; ++i) {
        std::optional<KEY_t> lastKey1 = runs[i]->getLastKey();
        std::optional<KEY_t> firstKey2 = runs[i + 1]->getFirstKey();
        if (lastKey1 && firstKey2) {
            sum += labs(*lastKey1 - *firstKey2);
        }
    }
    return sum;
}

// Iterate through the runs of the level, calculating the sum of key differences for segments
// Return the start and end indices of the best segment to compact
std::pair<size_t, size_t> Level::findBestSegmentToCompact() {
    size_t num_runs_to_merge = std::max(2, static_cast<int>(std::round(lsmTree->getCompactionPercentage() * runs.size())));
    size_t bestStartIdx = 0;
    size_t bestEndIdx = num_runs_to_merge - 1;
    long best_diff = sumOfKeyDifferences(bestStartIdx, bestEndIdx - 1);

    for (size_t idx = 1; idx <= runs.size() - num_runs_to_merge; ++idx) {
        long diff = sumOfKeyDifferences(idx, idx + num_runs_to_merge - 2);
        if (diff < best_diff) {
            best_diff = diff;
            bestStartIdx = idx;
            bestEndIdx = idx + num_runs_to_merge - 1;
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

    lsmTree = lsmTreePtr;

    for (const auto& runJson : j["runs"]) {
        std::unique_ptr<Run> run = std::make_unique<Run>(maxKvPairs, DEFAULT_ERROR_RATE, false, levelNum, lsmTree);
        run->deserialize(runJson);
        runs.emplace_back(std::move(run));
    }
}
