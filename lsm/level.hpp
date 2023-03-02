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
    // max_run_size is the maximum size of a run in a level
    long max_run_size;
    // leveling is true if the level is leveled, and false if it is tiered
    bool leveling = false;
    // runs is a std::deque of std::unique_ptr pointing to runs in the level
    deque<unique_ptr<Run>> runs;
    // constructor
    Level(int n, long s, bool l, int ln) : max_runs(n), max_run_size(s), leveling(l), level_num(ln), num_runs(0) {}
    // destructor
    ~Level() {
        cout << "LEVEL DESTRUCTOR\n";
    };
    // runs_remaining returns the number of runs that can be added to the level
    bool runs_remaining(void) const {return max_runs - num_runs;}
    // put takes a pointer to a Run as a parameter and adds a std::unique_ptr to the runs queue
    void put(unique_ptr<Run> run_ptr);
    // dump prints the contents of the level
    void dump();
    // compactLevel compacts the level
    // void compactLevel();
};
