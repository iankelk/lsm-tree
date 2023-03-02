#include <queue>

#include "run.hpp"

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
    // runs is a queue of runs in the level
    std::deque<Run> runs;
    // constructor
    Level(int n, long s, bool l) : max_runs(n), max_run_size(s), leveling(l), num_runs(0) {}
    // runs_remaining returns the number of runs that can be added to the level
    bool runs_remaining(void) const {return max_runs - num_runs;}
    // add_run adds a run to the level
    void put(Run run);
    // dump prints the contents of the level
    void dump();
    // compactLevel compacts the level
    void compactLevel();
};
