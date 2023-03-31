#include <iostream>
#include <iomanip>
#include <sstream>

// Given a message, print it to stderr and exit with an error code. This is used for unrecoverable errors.
void die(const std::string& message) {
    std::cerr << "Error: " << message << std::endl;
    std::cerr << "Exiting..." << std::endl;
    std::exit(EXIT_FAILURE);
};

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

// Given a number as a string, return a string with commas every three digits, starting from the end.
std::string addCommas(std::string s) {
    int len = s.length();

    // Add commas every three digits, starting from the end
    for (int i = len - 3; i > 0; i -= 3) {
        s.insert(i, ",");
    }
    return s;
}