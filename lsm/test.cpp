#include <cstdlib>

#include "data_types.hpp"
#include "error.hpp"
#include "unistd.h"

using namespace std;

#include <iostream>
#include "bloom_filter.hpp"

int main2() {
    BloomFilter bf(1000, 0.01, 10000);
    bf.add(12345);
    bf.add(123456);
    std::cout << bf.contains(12345) << std::endl; // prints 1
    std::cout << bf.contains(54321) << std::endl; // prints 0
    return 0;
}