#ifndef UTILS_HPP
#define UTILS_HPP
#include <string>

// Declarations of error and string handling functions
void die(const std::string& message);
std::string formatMicroseconds(size_t microseconds);
std::string addCommas(std::string s);

#endif /* UTILS_HPP */