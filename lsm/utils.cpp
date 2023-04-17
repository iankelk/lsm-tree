#include <iostream>
#include <iomanip>
#include <sstream>
#include <execinfo.h>
#include <stdio.h>
#include <stdlib.h>

// Given a message, print it to stderr and exit with an error code. This is used for unrecoverable errors.
void die(const std::string& message) {
    std::cerr << "Error: " << message << std::endl;
    std::cerr << "Exiting..." << std::endl;
    std::exit(EXIT_FAILURE);
};

// Given a number of microseconds, return a string with the minutes and seconds to 2 significant digits.
std::string formatMicroseconds(size_t microseconds) {
    size_t totalSeconds = microseconds / 1000000;
    size_t minutes = totalSeconds / 60;
    microseconds %= 1000000;
    double seconds = static_cast<double>(totalSeconds % 60) + static_cast<double>(microseconds) / 1000000;

    std::stringstream ss;
    ss << std::fixed << std::setprecision(2) << seconds;
    std::string secondsStr = ss.str();

    return std::to_string(minutes) + " minutes, " + secondsStr + " seconds";
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