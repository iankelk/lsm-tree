#ifndef UTILS_HPP
#define UTILS_HPP
#include <string>

// Declarations of error and string handling functions
void die(const std::string& message);
std::string formatMicroseconds(long long microseconds);
std::string addCommas(std::string s);

#endif /* UTILS_HPP */