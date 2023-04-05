#ifndef LEVEL_HPP
#define LEVEL_HPP
#include <queue>
#include <cmath>
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
    enum State {
        FULL,
        TWO_RUNS,
        UNKNOWN
    };
    // constructor
    Level(long bs, int f, Policy l, int ln, LSMTree* lsmTree) :
    bufferSize(bs), fanout(f), levelPolicy(l), levelNum(ln),
    kvPairs(0), maxKvPairs(pow(f, ln) * bs), lsmTree(lsmTree), 
    diskName(Storage::getDiskName(ln)), diskPenaltyMultiplier(Storage::getDiskPenaltyMultiplier(ln)) {};

    Level() = default; // default constructor
    ~Level() {}; // destructor
    std::deque<std::unique_ptr<Run>> runs; // std::deque of std::unique_ptr pointing to runs in the level
    void put(std::unique_ptr<Run> runPtr); // adds a std::unique_ptr to the runs queue
    void dump(); // dump prints the contents of the level. TODO: remove this function
    void compactLevel(double errorRate, State state, bool isLastLevel); 
    void compactSegment(double errorRate, size_t segStartIdx, size_t segEndIdx, bool isLastLevel);
    std::pair<size_t, size_t> findBestSegmentToCompact(); 
    long getLevelSize(int levelNum); 
    std::string getDiskName() const;
    int getDiskPenaltyMultiplier() const;
    bool willLowerLevelFit(); // true if there is enough space in the level to add a run with maxKvPairs
    bool willBufferFit(); // true if there is enough space in the level to flush the memtable
    int addUpKVPairsInLevel(); // Iterates over the runs to calculate the total number of kvPairs in the level
    int getLevelNum() const;
    Policy getLevelPolicy() const;
    long getKvPairs() const;  // Get the number of kvPairs in the level
    void setKvPairs(long kvPairs); // Set the number of kvPairs in the level
    long getMaxKvPairs() const; // Get the max number of kvPairs in the level
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
    void deserialize(const json& j);

    // copy constructor
    Level(Level&& other) noexcept
        : levelNum(other.levelNum),
          fanout(other.fanout),
          maxKvPairs(other.maxKvPairs),
          bufferSize(other.bufferSize),
          levelPolicy(other.levelPolicy),
          kvPairs(other.kvPairs),
          lsmTree(other.lsmTree),
          diskName(other.diskName),
          diskPenaltyMultiplier(other.diskPenaltyMultiplier),
          runs(std::move(other.runs)) {
    }
    // copy assignment operator 
    Level& operator=(Level&& other) noexcept {
        levelNum = other.levelNum;
        fanout = other.fanout;
        maxKvPairs = other.maxKvPairs;
        bufferSize = other.bufferSize;
        levelPolicy = other.levelPolicy;
        kvPairs = other.kvPairs;
        lsmTree = other.lsmTree;
        diskName = other.diskName;
        diskPenaltyMultiplier = other.diskPenaltyMultiplier;
        runs = std::move(other.runs);
        return *this;
    }
private:
    int levelNum;
    long kvPairs; // the number of key-value pairs in the level    
    Policy levelPolicy; // can be TIERED, LEVELED, LAZY_LEVELED, or PARTIAL
    long maxKvPairs; // the maximum number of key-value pairs that can be in the level
    long bufferSize; // Memtable size
    int fanout;
    std::map<int, long> levelSizes; // Vector of level sizes cached for faster lookup
    LSMTree* lsmTree;
    std::string diskName;
    int diskPenaltyMultiplier;
};

#endif /* LEVEL_HPP */