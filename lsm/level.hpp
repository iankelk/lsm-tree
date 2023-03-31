#ifndef LEVEL_HPP
#define LEVEL_HPP
#include <queue>
#include <cmath>
#include "run.hpp"

class LSMTree;
class Run;

class Level {
public:
    enum Policy {
        TIERED,
        LEVELED,
        LAZY_LEVELED
    };
    enum State {
        FULL,
        TWO_RUNS,
        UNKNOWN
    };
    // constructor
    Level(long bs, int f, Policy l, int ln, LSMTree* lsm_tree) :
    buffer_size(bs), fanout(f), level_policy(l), level_num(ln),
    is_last_level(false), kv_pairs(0), max_kv_pairs(pow(f, ln) * bs), lsm_tree(lsm_tree) {};
    
    Level() = default; // default constructor
    ~Level() {}; // destructor
    std::deque<std::unique_ptr<Run>> runs; // std::deque of std::unique_ptr pointing to runs in the level
    void put(std::unique_ptr<Run> run_ptr); // adds a std::unique_ptr to the runs queue
    void dump(); // dump prints the contents of the level. TODO: remove this function
    void compactLevel(double error_rate, State state);
    long getLevelSize(int level_num); 
    bool willLowerLevelFit(); // true if there is enough space in the level to add a run with max_kv_pairs
    bool willBufferFit(); // true if there is enough space in the level to flush the memtable
    int addUpKVPairsInLevel(); // Iterates over the runs to calculate the total number of kv_pairs in the level
    int getLevelNum() const;
    Policy getLevelPolicy() const;
    long getKvPairs() const;  // Get the number of kv_pairs in the level
    void setKvPairs(long kv_pairs); // Set the number of kv_pairs in the level
    long getMaxKvPairs() const; // Get the max number of kv_pairs in the level
    static std::string policyToString(Policy policy) {
        switch (policy) {
            case Policy::TIERED: return "TIERED";
            case Policy::LEVELED: return "LEVELED";
            case Policy::LAZY_LEVELED: return "LAZY_LEVELED";
            default: return "ERROR";
        }
    }
    // Used for deserialization
    static Policy stringToPolicy(std::string policy) {
        switch (policy[0]) {
            case 'T': return Policy::TIERED;
            case 'L': return Policy::LEVELED;
            case 'Z': return Policy::LAZY_LEVELED;
            default: return Policy::TIERED;
        }
    }

    json serialize() const;
    void deserialize(const json& j);

    // copy constructor
    Level(Level&& other) noexcept
        : level_num(other.level_num),
          fanout(other.fanout),
          max_kv_pairs(other.max_kv_pairs),
          buffer_size(other.buffer_size),
          level_policy(other.level_policy),
          kv_pairs(other.kv_pairs),
          lsm_tree(other.lsm_tree),
          runs(std::move(other.runs)) {
    }
    // copy assignment operator 
    Level& operator=(Level&& other) noexcept {
        level_num = other.level_num;
        fanout = other.fanout;
        max_kv_pairs = other.max_kv_pairs;
        buffer_size = other.buffer_size;
        level_policy = other.level_policy;
        kv_pairs = other.kv_pairs;
        lsm_tree = other.lsm_tree;
        runs = std::move(other.runs);
        return *this;
    }
private:
    int level_num;
    bool is_last_level;
    long kv_pairs; // the number of key-value pairs in the level    
    Policy level_policy; // can be TIERED, LEVELED, or LAZY_LEVELED
    long max_kv_pairs; // the maximum number of key-value pairs that can be in the level
    long buffer_size; // Memtable size
    int fanout;
    std::map<int, long> level_sizes; // Vector of level sizes cached for faster lookup
    LSMTree* lsm_tree;
};

#endif /* LEVEL_HPP */