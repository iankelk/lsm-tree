#pragma once
#include <queue>
#include <cmath>
#include <shared_mutex>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/lock_algorithms.hpp>
#include <nlohmann/json.hpp>
#include "run.hpp"
#include "storage.hpp"

class LSMTree;
class Run;

class Level {
public:
    enum Policy {
        TIERED,
        LEVELED,
        LAZY_LEVELED,
        PARTIAL
    };
    // constructor
    Level(long bufferSize, int fanout, Policy levelPolicy, int levelNum, LSMTree* lsmTree) :
    bufferSize(bufferSize), fanout(fanout), levelPolicy(levelPolicy), levelNum(levelNum),  
    lsmTree(lsmTree), kvPairs(0), maxKvPairs(pow(fanout, levelNum) * bufferSize), 
    diskName(Storage::getDiskName(levelNum)), diskPenaltyMultiplier(Storage::getDiskPenaltyMultiplier(levelNum)) {};


    Level() = default; // default constructor
    ~Level() {}; // destructor
    std::deque<std::unique_ptr<Run>> runs; // std::deque of std::unique_ptr pointing to runs in the level
    void put(std::unique_ptr<Run> runPtr); // adds a std::unique_ptr to the runs queue
    size_t getLevelSize(int levelNum); 
    std::string getDiskName() const;
    int getDiskPenaltyMultiplier() const;
    bool willLowerLevelFit(); // true if there is enough space in the level to add a run with maxKvPairs
    bool willBufferFit(); // true if there is enough space in the level to flush the memtable
    size_t addUpKVPairsInLevel(); // Iterates over the runs to calculate the total number of kvPairs in the level
    int getLevelNum() const;
    Policy getLevelPolicy() const;
    size_t getKvPairs() const;  // Get the number of kvPairs in the level
    void setKvPairs(long kvPairs); // Set the number of kvPairs in the level
    size_t getMaxKvPairs() const; // Get the max number of kvPairs in the level

    void replaceSegment(std::pair<size_t, size_t> segmentBounds, std::unique_ptr<Run> compactedRun);
    std::unique_ptr<Run> compactSegment(double errorRate, std::pair<size_t, size_t> segmentBounds, bool isLastLevel);
    std::pair<size_t, size_t> findBestSegmentToCompact(); 
    long sumOfKeyDifferences(size_t start, size_t end);

    mutable std::shared_mutex levelMutex;

    static std::string policyToString(Policy policy) {
        switch (policy) {
            case Policy::TIERED: return "TIERED";
            case Policy::LEVELED: return "LEVELED";
            case Policy::LAZY_LEVELED: return "LAZY_LEVELED";
            case Policy::PARTIAL: return "PARTIAL";
            default: return "ERROR";
        }
    }
    // Used for deserialization
    static Policy stringToPolicy(std::string policy) {
        switch (policy[0]) {
            case 'T': return Policy::TIERED;
            case 'L': return Policy::LEVELED;
            case 'Z': return Policy::LAZY_LEVELED;
            case 'P': return Policy::PARTIAL;
            default: return Policy::TIERED;
        }
    }

    json serialize() const;
    void deserialize(const json& j, LSMTree* lsmTreePtr);

    // Move constructor
    Level(Level&& other) noexcept
        : runs(std::move(other.runs)),
        levelMutex(), // Mutexes are default constructed
        bufferSize(other.bufferSize),
        fanout(other.fanout),
        levelPolicy(other.levelPolicy),
        levelNum(other.levelNum),
        lsmTree(other.lsmTree),
        kvPairs(other.kvPairs),
        maxKvPairs(other.maxKvPairs),
        diskName(other.diskName),
        diskPenaltyMultiplier(other.diskPenaltyMultiplier)
    {
    }
    // copy assignment operator
    Level& operator=(Level&& other) noexcept {
        if (this != &other) {
            runs = std::move(other.runs); 
            bufferSize = other.bufferSize;
            fanout = other.fanout;
            levelPolicy = other.levelPolicy;
            levelNum = other.levelNum;
            lsmTree = other.lsmTree;
            kvPairs = other.kvPairs;
            maxKvPairs = other.maxKvPairs;
            diskName = other.diskName;
            diskPenaltyMultiplier = other.diskPenaltyMultiplier;           
        }
        return *this;
    }

private:
    size_t bufferSize; // Memtable size
    int fanout; // Fanout of the tree
    Policy levelPolicy; // can be TIERED, LEVELED, LAZY_LEVELED, or PARTIAL
    unsigned int levelNum;
    LSMTree* lsmTree;
    size_t kvPairs; // the number of key-value pairs in the level 
    size_t maxKvPairs; // the maximum number of key-value pairs that can be in the level
    std::map<int, long> levelSizes; // Map of level sizes cached for faster lookup
    std::string diskName;
    int diskPenaltyMultiplier;
};
