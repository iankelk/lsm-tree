#include <iostream>
#include <iomanip>
#include <sstream>
#include <execinfo.h>
#include <stdio.h>
#include <stdlib.h>
#include <map>
#include "utils.hpp"

std::mutex SyncedCout::_coutMutex;
std::mutex SyncedCerr::_cerrMutex;

// Given a message, print it to stderr and exit with an error code. This is used for unrecoverable errors.
void die(const std::string& message) {
    std::cerr << "Error: " << message << std::endl;
    std::cerr << "Exiting..." << std::endl;
    std::exit(EXIT_FAILURE);
};

// Given a number of microseconds, return a string with the hours, minutes, and seconds to 2 significant digits.
// If there are no hours, the hours are omitted. If there are no minutes, the minutes are omitted.
std::string formatMicroseconds(size_t microseconds) {
    size_t totalSeconds = microseconds / 1000000;
    size_t hours = totalSeconds / 3600;
    totalSeconds %= 3600;
    size_t minutes = totalSeconds / 60;
    totalSeconds %= 60;
    double seconds = static_cast<double>(totalSeconds) + static_cast<double>(microseconds % 1000000) / 1000000;

    std::stringstream ss;
    ss << std::fixed << std::setprecision(2) << seconds;
    std::string secondsStr = ss.str();

    std::string result = "";

    if (hours > 0) {
        result += std::to_string(hours) + " hours, ";
    }
    if (minutes > 0) {
        result += std::to_string(minutes) + " minutes, ";
    }
    result += secondsStr + " seconds";
    return result;
};


// Given a number as a string, return a string with commas every three digits, starting from the end.
std::string addCommas(std::string s) {
    int len = s.length();

    // Add commas every three digits, starting from the end
    for (int i = len - 3; i > 0; i -= 3) {
        s.insert(i, ",");
    }
    return s;
}

void printTrace() {
    void* trace[16];
    int trace_size = backtrace(trace, 16);
    char** messages = backtrace_symbols(trace, trace_size);
    printf("[bt] Execution path:\n");
    for (int i = 0; i < trace_size; i++)
        printf("[bt] %s\n", messages[i]);
    printf("\n");
    free(messages);
}

// Helper function to get the length of the longest string in a vector
int getLongestStringLength(const std::vector<std::string>& strings) {
    return max_element(strings.begin(), strings.end(), [](const std::string &a, const std::string &b) { return a.length() < b.length(); })->length();
}

// Function to get the length of the longest vector in a nested map (vector<vector<map<string, string>>>)
size_t getLongestVectorLength(const std::vector<std::vector<std::map<std::string, std::string>>>& maps) {
    size_t maxLength = 0;
    for (const auto& level : maps) {
        if (level.size() > maxLength) {
            maxLength = level.size();
        }
    }
    return maxLength;
}

// Helper function to return a vector of values by key from a nested map (vector<vector<map<string, string>>>)
std::vector<std::string> getMapValuesByKey(const std::vector<std::vector<std::map<std::string, std::string>>>& maps, const std::string& key) {
    std::vector<std::string> values;
    for (const auto& level : maps) {
        for (const auto& run : level) {
            auto it = run.find(key);
            if (it != run.end()) {
                values.push_back(it->second);
            }
        }
    }
    return values;
}