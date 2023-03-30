#include <iostream>
#include <iomanip>
#include <sstream>

void die(const std::string& message) {
    std::cerr << "\nUsage:\n" << message << std::endl;
    std::exit(EXIT_FAILURE);
}

// Given a number of microseconds, return a string with the minutes and seconds to 2 significant digits.
std::string formatMicroseconds(long long microseconds) {
    long long total_seconds = microseconds / 1000000;
    long long minutes = total_seconds / 60;
    microseconds %= 1000000;
    double seconds = static_cast<double>(total_seconds % 60) + static_cast<double>(microseconds) / 1000000;

    std::stringstream ss;
    ss << std::fixed << std::setprecision(2) << seconds;
    std::string seconds_str = ss.str();

    return std::to_string(minutes) + " minutes, " + seconds_str + " seconds";
};
