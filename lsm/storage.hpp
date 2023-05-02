#pragma once
#include <array>
#include "data_types.hpp"

class Storage {
public:
    Storage() = delete;  // disable the default constructor
    ~Storage() = delete; // disable the destructor

    static std::string getDiskName(int level);
    static int getDiskPenaltyMultiplier(int level);

private:
    static const std::array<std::pair<std::string, int>, NUM_DISK_TYPES> disks;
};
