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
    // num_runs is the number of runs that are currently in a level
    int num_runs;
    // max_runs is the maximum number of runs that can be in a level
    int max_runs;
    // leveling is true if the level is leveled, and false if it is tiered
    bool leveling = false;
    // runs is a std::deque of std::unique_ptr pointing to runs in the level
    deque<unique_ptr<Run>> runs;
    // constructor
    Level(int n, bool l, int ln) : max_runs(n), leveling(l), level_num(ln), num_runs(0) {}
    // destructor
    ~Level() {
        //cout << "LEVEL DESTRUCTOR\n";
    };
    // runs_remaining returns the number of runs that can be added to the level
    bool runs_remaining(void) const {return max_runs - num_runs;}
    // put takes a pointer to a Run as a parameter and adds a std::unique_ptr to the runs queue
    void put(unique_ptr<Run>&& run_ptr);
    // dump prints the contents of the level
   void dump();
    // compactLevel compacts the level
    void compactLevel(long max_kv_pairs, int capacity, double error_rate, int bitset_size);

    // copy constructor
    Level(Level&& other) noexcept
        : level_num(other.level_num),
          num_runs(other.num_runs),
          max_runs(other.max_runs),
          leveling(other.leveling),
          runs(std::move(other.runs)) {
    }
    // copy assignment operator 
    Level& operator=(Level&& other) noexcept {
        level_num = other.level_num;
        num_runs = other.num_runs;
        max_runs = other.max_runs;
        leveling = other.leveling;
        runs = std::move(other.runs);
        return *this;
    }

};

#endif /* LEVEL_HPP */