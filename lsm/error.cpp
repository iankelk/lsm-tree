#include <iostream>
//#include <cstdlib>

void die(const std::string& message) {
    std::cerr << "\nUsage:\n" << message << std::endl;
    std::exit(EXIT_FAILURE);
}