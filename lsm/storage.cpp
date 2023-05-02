#include "storage.hpp"
#include "utils.hpp"
#include "data_types.hpp"
#include <iostream>

const std::array<std::pair<std::string, int>, NUM_DISK_TYPES> Storage::disks {{
    {DISK1_NAME, DISK1_PENALTY_MULTIPLIER},
    {DISK2_NAME, DISK2_PENALTY_MULTIPLIER},
    {DISK3_NAME, DISK3_PENALTY_MULTIPLIER},
    {DISK4_NAME, DISK4_PENALTY_MULTIPLIER},
    {DISK5_NAME, DISK5_PENALTY_MULTIPLIER}
}};

// Return the name of the disk at the given level. If the level is at the slowest disk, return the name of the slowest disk
std::string Storage::getDiskName(int level) {
    if (level < 1) {
        die("Storage::getDiskName: Level numbers start at 1 and the level provided is " + std::to_string(level));
    } else if (level > NUM_DISK_TYPES) {
        level = NUM_DISK_TYPES;
    }
    return disks[level-1].first;
}

// Return the penalty multiplier of the disk at the given level. If the level is at the slowest disk, return the penalty multiplier of the slowest disk
int Storage::getDiskPenaltyMultiplier(int level) {
    if (level < 1) {
        die("Storage::getDiskPenaltyMultiplier: Level numbers start at 1 and the level provided is " + std::to_string(level));
    } else if (level > NUM_DISK_TYPES) {
        level = NUM_DISK_TYPES;
    }
    return disks[level-1].second;
}
