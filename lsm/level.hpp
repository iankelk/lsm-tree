#ifndef LEVEL_HPP
#define LEVEL_HPP
#include <queue>
#include <cmath>
#include "run.hpp"

class Level {
public:
    enum Policy {
        TIERED,
        LEVELED,
        LAZY_LEVELED
    };
    // constructor
    Level(long bs, int f, Policy l, int ln) :
    buffer_size(bs), fanout(f), level_policy(l), level_num(ln),
    is_last_level(false), kv_pairs(0), max_kv_pairs(pow(f, ln) * bs) {};
    // destructor
    ~Level() {
        //cout << "LEVEL DESTRUCTOR\n";
    };
    // runs is a std::deque of std::unique_ptr pointing to runs in the level
    std::deque<std::unique_ptr<Run>> runs;
    // put takes a pointer to a Run as a parameter and adds a std::unique_ptr to the runs queue
    void put(std::unique_ptr<Run> run_ptr);
    // dump prints the contents of the level
    void dump();
    // compactLevel compacts the level
    void compactLevel(double error_rate, int bitset_size);
    // getLevelSize returns the size of the level
    long getLevelSize(int level_num); 
    // Returns true if there is enough space in the level to add a run with max_kv_pairs
    bool willLowerLevelFit();
    // Returns true if there is enough space in the level to flush the memtable
    bool willBufferFit();
    // Returns the number of kv_pairs in the level
    int numKVPairs();
    // Returns the level number
    int getLevelNum() const;
    // Sets the last level 
    void setLastLevel(bool is_last_level);
    // Gets the last level
    bool isLastLevel() const;
    // Returns the level policy
    Policy getLevelPolicy() const;
    // Get the number of kv_pairs in the level
    long getKvPairs() const;
    // Set the number of kv_pairs in the level
    void setKvPairs(long kv_pairs);

    static std::string policyToString(Policy policy) {
        switch (policy) {
            case Policy::TIERED: return "TIERED";
            case Policy::LEVELED: return "LEVELED";
            case Policy::LAZY_LEVELED: return "LAZY_LEVELED";
            default: return "ERROR";
        }
    }

    json serialize() const;
    // copy constructor
    Level(Level&& other) noexcept
        : level_num(other.level_num),
          fanout(other.fanout),
          max_kv_pairs(other.max_kv_pairs),
          buffer_size(other.buffer_size),
          level_policy(other.level_policy),
          runs(std::move(other.runs)) {
    }
    // copy assignment operator 
    Level& operator=(Level&& other) noexcept {
        level_num = other.level_num;
        fanout = other.fanout;
        max_kv_pairs = other.max_kv_pairs;
        buffer_size = other.buffer_size;
        level_policy = other.level_policy;
        runs = std::move(other.runs);
        return *this;
    }
private:
    int level_num;
    bool is_last_level;
    // kv_pairs is the number of key-value pairs in the level
    long kv_pairs;
    // Level policy can be TIERED, LEVELED, or LAZY_LEVELED
    Policy level_policy;
    // max_kv_pairs is the maximum number of key-value pairs that can be in the level
    long max_kv_pairs;
    // Memtable size
    long buffer_size;
    // Fanout
    int fanout;
    // Vector of level sizes cached
    std::map<int, long> level_sizes;
};

#endif /* LEVEL_HPP */