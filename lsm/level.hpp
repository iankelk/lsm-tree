#ifndef LEVEL_HPP
#define LEVEL_HPP

#include <queue>
#include <iostream>

#include "run.hpp"

using namespace std;

class Level {
public:
    // level_num is the level number
    int level_num;
    // boolean if the level is the last level.
    bool is_last_level = false;
    // num_runs is the number of runs that are currently in a level
    int num_runs = 0;
    // leveling is true if the level is leveled, and false if it is tiered
    bool level_policy;
    // kv_pairs is the number of key-value pairs in the level
    long kv_pairs = 0;
    // max_kv_pairs is the maximum number of key-value pairs that can be in the level
    long max_kv_pairs;
    // Memtable size
    long buffer_size;
    // Fanout
    int fanout;
    // Vector of level sizes cached
    map<int, long> level_sizes;
    // runs is a std::deque of std::unique_ptr pointing to runs in the level
    deque<unique_ptr<Run>> runs;
    // constructor
    Level(long bs, int f, bool l, int ln) : buffer_size(bs), fanout(f), level_policy(l), level_num(ln), num_runs(0),
    max_kv_pairs(pow(f, ln) * bs) {}
    // destructor
    ~Level() {
        //cout << "LEVEL DESTRUCTOR\n";
    };
    // put takes a pointer to a Run as a parameter and adds a std::unique_ptr to the runs queue
    void put(unique_ptr<Run> run_ptr);
    // dump prints the contents of the level
    void dump();
    // compactLevel compacts the level
    void compactLevel(int capacity, double error_rate, int bitset_size);
    // sumMaxKvPairs returns the sum of the max_kv_pairs of the runs in the level
    long sumMaxKvPairs();
    // getLevelSize returns the size of the level
    long getLevelSize(int level_num); 
    // Returns true if there is enough space in the level to add a run with max_kv_pairs
    bool willLowerLevelFit();
    // Returns true if there is enough space in the level flush the memtable
    bool willBufferFit();
    // Returns the number of kv_pairs in the level
    int numKVPairs();


    // copy constructor
    Level(Level&& other) noexcept
        : level_num(other.level_num),
          num_runs(other.num_runs),
          fanout(other.fanout),
          max_kv_pairs(other.max_kv_pairs),
          buffer_size(other.buffer_size),
          level_policy(other.level_policy),
          runs(std::move(other.runs)) {
    }
    // copy assignment operator 
    Level& operator=(Level&& other) noexcept {
        level_num = other.level_num;
        num_runs = other.num_runs;
        fanout = other.fanout;
        max_kv_pairs = other.max_kv_pairs;
        buffer_size = other.buffer_size;
        level_policy = other.level_policy;
        runs = std::move(other.runs);
        return *this;
    }

};

#endif /* LEVEL_HPP */