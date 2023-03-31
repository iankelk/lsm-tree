#include <cstdlib>

#include "data_types.hpp"
#include "utils.hpp"
#include "unistd.h"

/*
int main(int argc, char *argv[]) {
    int bf_bits_per_key, fanout, memtable_size;
    // Parse command line arguments
    // -f: for fanout (ratio of memtable) to lower levels
    // -b: bits per key value pair of bloom filter
    // -m: size of memtable
    int opt;
    while ((opt = getopt(argc, argv, "f:b:m:")) != -1) {
        switch (opt) {
            case 'f':
                fanout = atoi(optarg);
                break;
            case 'b':
                bf_bits_per_key = atoi(optarg);
                break;
            case 'm':
                memtable_size = atoi(optarg);
                break;
            default:
                die("-f: fanout (ratio of memtable) to lower levels\n"
                    "-b: bits per key value pair of bloom filter\n"
                    "-m: size of memtable\n");
        }
    }


}
*/

// void server(LSMTree &tree) {

//     std::string line;
//     std::string command;
//     KEY_t key_a, key_b;
//     VAL_t val;
//     std::string file_path;
//     while (getline(cin, line)) {
//         std::stringstream ss(line);
//         ss >> command;
//         if (command == "p") {
//             ss >> key_a >> val;
//             if (val < VAL_MIN || val > VAL_MAX) {
//                 die("Could not insert value " + to_string(val) + ": out of range.");
//             } else {
//                 tree.put(key_a, val);
//             }
//         } else if (command == "g") {
//             ss >> key_a;
//             tree.get(key_a);
//         } else if (command == "r") {
//             ss >> key_a >> key_b;
//             tree.range(key_a, key_b);
//         } else if (command == "d") {
//             ss >> key_a;
//             tree.del(key_a);
//         } else if (command == "l") {
//             ss >> file_path;
//             // Trim quotes
//             file_path = file_path.substr(1, file_path.size() - 2);
//             tree.load(file_path);
//         } else {
//             die("Invalid command.");
//         }
//     }
// }

/*
#include <iostream>
#include "dynamic_bitset.hpp"

int main() {
    DynamicBitset bits(10);
    bits.set(3);
    bits.set(5);
    bits.set(9);
    bits.reset(5);

    for (size_t i = 0; i < bits.size(); ++i) {
        std::cout << bits.test(i) << " ";
    }

    std::cout << "\n\n";

    bits.resize(20);

    for (size_t i = 0; i < bits.size(); ++i) {
        std::cout << bits.test(i) << " ";
    }

    std::cout << std::endl;
    return 0;
}
*/

// #include <iostream>
// #include "bloom_filter.hpp"

// int main() {
//     BloomFilter bf(1000, 0.01);
//     bf.add(12345);
//     bf.add(123456);
//     std::cout << bf.contains(12345) << std::endl; // prints 1
//     std::cout << bf.contains(54321) << std::endl; // prints 0
//     return 0;
// }

#include <iostream>
#include <vector>

int main() {
    std::vector<bool> test_vec(100000000); // 100 million bools

    // Measure memory usage in bytes
    size_t memory_usage = sizeof(test_vec) + sizeof(bool) * test_vec.capacity();
    std::cout << "Memory usage: " << memory_usage << " bytes" << std::endl;

    return 0;
}