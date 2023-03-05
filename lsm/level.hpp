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
    // boolean if the level is the last level
    bool is_last_level = false;
    // num_runs is the number of runs that are currently in a level
    int num_runs;
    // max_runs is the maximum number of runs that can be in a level
    int max_runs;
    // leveling is true if the level is leveled, and false if it is tiered
    bool level_policy;
    // runs is a std::deque of std::unique_ptr pointing to runs in the level
    deque<unique_ptr<Run>> runs;
    // constructor
    Level(int n, bool l, int ln) : max_runs(n), level_policy(l), level_num(ln), num_runs(0) {
        // Print the size of the deque
        cout << "Size of deque in constructor: " << runs.size() << "\n";
    }
    // destructor
    ~Level() {
        //cout << "LEVEL DESTRUCTOR\n";
    };
    // runs_remaining returns the number of runs that can be added to the level
    bool runs_remaining(void) const {return max_runs - num_runs;}
    // put takes a pointer to a Run as a parameter and adds a std::unique_ptr to the runs queue
    void put(unique_ptr<Run> run_ptr);
    // dump prints the contents of the level
    void dump();
    // compactLevel compacts the level
    void compactLevel(long max_kv_pairs, int capacity, double error_rate, int bitset_size);

    // copy constructor
    Level(Level&& other) noexcept
        : level_num(other.level_num),
          num_runs(other.num_runs),
          max_runs(other.max_runs),
          level_policy(other.level_policy),
          runs(std::move(other.runs)) {
    }
    // copy assignment operator 
    Level& operator=(Level&& other) noexcept {
        level_num = other.level_num;
        num_runs = other.num_runs;
        max_runs = other.max_runs;
        level_policy = other.level_policy;
        runs = std::move(other.runs);
        return *this;
    }

};

#endif /* LEVEL_HPP */