#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>

#include "run.hpp"

using namespace std;

Run::Run(long max_kv_pairs, int capacity, double error_rate, int bitset_size) :
    max_kv_pairs(max_kv_pairs),
    capacity(capacity),
    error_rate(error_rate),
    bitset_size(bitset_size),
    bloom_filter(capacity, error_rate, bitset_size),
    fence_pointers(max_kv_pairs / getpagesize()),
    tmp_file(""),
    size(0),
    max_key(0),
    fd(FILE_DESCRIPTOR_UNINITIALIZED)
{
    char tmp_fn[] = SSTABLE_FILE_TEMPLATE;
    fd = mkstemp(tmp_fn);
    if (fd == FILE_DESCRIPTOR_UNINITIALIZED) {
        throw runtime_error("Failed to create temporary file for Run");
    }
    tmp_file = tmp_fn;
}

Run::~Run() {
    cout << "RUN DESTRUCTOR\n";
    closeFile();
    //cout << "FD after destruct: " + to_string(fd) + "\n"; 
    assert(fd == FILE_DESCRIPTOR_UNINITIALIZED);
    remove(tmp_file.c_str());
}

// Close the file descriptor for the temporary file for when we are performing point or range queries
void Run::closeFile() {
    close(fd);
    fd = FILE_DESCRIPTOR_UNINITIALIZED;
}

void Run::put(KEY_t key, VAL_t val) {
    int result;
    if (size >= max_kv_pairs) {
        throw runtime_error("Attempting to add to full Run");
    }
    kv_pair kv = {key, val};
    bloom_filter.add(key);
    // Add the key to the fence pointers vector if it is a multiple of the page size. 
    // We can assume it is sorted because the buffer is sorted
    if (size % getpagesize() == 0) {
        fence_pointers.push_back(key);
    }
    // If the key is greater than the max key, update the max key
    if (key > max_key) {
        max_key = key;
    }

    // Write the key-value pair to the temporary file
    result = write(fd, &kv, sizeof(kv_pair));
    assert(result != -1);
    size++;
}

VAL_t * Run::get(KEY_t key) {
    VAL_t *val;

    // Check if the run is empty
    if (size == 0) {
        return nullptr;
    }

    // Check if the key is in the bloom filter and if it is in the range of the fence pointers
    if (key < fence_pointers.front() || key > max_key || !bloom_filter.contains(key)) {
        return nullptr;
    }
    // Binary search for the page containing the key in the fence pointers vector
    auto upper_bound_iter = std::upper_bound(fence_pointers.begin(),  fence_pointers.end(), key);
    auto page_index = static_cast<long>(std::distance(fence_pointers.begin(), upper_bound_iter)) - 1;

    if (page_index < 0) {
        throw runtime_error("Negative index from fence pointer");
    }

    // Open the file descriptor for the temporary file
    fd = open(tmp_file.c_str(), O_RDONLY);
    if (fd == FILE_DESCRIPTOR_UNINITIALIZED) {
        throw runtime_error("Failed to open temporary file for Run");
    }

    // Search the page for the key
    long offset = page_index * getpagesize();
    lseek(fd, offset, SEEK_SET);
    kv_pair kv;
    // Search the page and keep the most recently added value 
    // TODO: This is a linear search. Could be better with a binary search?
    while (read(fd, &kv, sizeof(kv_pair)) > 0) {
        if (kv.key == key) {
            val = new VAL_t;
            *val = kv.value;
        }
    }
    closeFile();
    return val;
}

// Return a map of all the key-value pairs in the range [start, end]
map<KEY_t, VAL_t> Run::range(KEY_t start, KEY_t end) {
    // Initialize an empty map
    map<KEY_t, VAL_t> range_map;

    // Check if the run is empty
    if (size == 0) {
        return range_map;
    }

    // Check if the range is in the range of the fence pointers
    if (end < fence_pointers.front() || start > max_key) {
        return range_map;
    }
    if (start < fence_pointers.front()) {
        start = fence_pointers.front();
    }
    if (end > max_key) {
        end = fence_pointers.back();
    }
    // Binary search for the page containing the start key in the fence pointers vector.
    // Since the first page that contains data in the requested range is included in the subrange,
    // the index needs to be shifted back by one page, so that it points to the start of the first
    // page containing data in the requested range.
    auto upper_bound_iter = std::upper_bound(fence_pointers.begin(),  fence_pointers.end(), start);
    auto start_page_index = static_cast<long>(std::distance(fence_pointers.begin(), upper_bound_iter)) - 1;
    // Binary search for the page containing the end key in the fence pointers vector. 
    // Since the page immediately following the last page that contains data in the requested range
    // is not included in the subrange, subrange_page_end does not need to be shifted back by one page.
    upper_bound_iter = std::upper_bound(fence_pointers.begin(),  fence_pointers.end(), end);
    auto end_page_index = static_cast<long>(std::distance(fence_pointers.begin(), upper_bound_iter));
    // Check that the start and end page indices are valid
    if (start_page_index < 0 || end_page_index < 0) {
        throw runtime_error("Negative index from fence pointer");
    }
    // Check that the start page index is less than the end page index
    if (start_page_index >= end_page_index) {
        throw runtime_error("Start page index is greater than or equal to end page index");
    }
    // Open the file descriptor for the temporary file
    fd = open(tmp_file.c_str(), O_RDONLY);
    if (fd == FILE_DESCRIPTOR_UNINITIALIZED) {
        throw runtime_error("Failed to open temporary file for Run");
    }
    // Search the pages for the keys in the range
    for (long page_index = start_page_index; page_index < end_page_index; page_index++) {
        long offset = page_index * getpagesize();
        lseek(fd, offset, SEEK_SET);
        kv_pair kv;
        // Search the page and keep the most recently added value'
        while (read(fd, &kv, sizeof(kv_pair)) > 0) {
            if (kv.key >= start && kv.key <= end) {
                range_map[kv.key] = kv.value;
            }
        }
    }
    closeFile();
    return range_map;
}

 map<KEY_t, VAL_t> Run::getMap() {
    map<KEY_t, VAL_t> map;
    // Open the file descriptor for the temporary file
    fd = open(tmp_file.c_str(), O_RDONLY);
    if (fd == FILE_DESCRIPTOR_UNINITIALIZED) {
        throw runtime_error("Failed to open temporary file for Run");
    }
    // Read all the key-value pairs from the temporary file
    kv_pair kv;
    while (read(fd, &kv, sizeof(kv_pair)) > 0) {
        map[kv.key] = kv.value;
    }
    closeFile();
    return map;
 }

long Run::getMaxKvPairs() {
    return max_kv_pairs;
}
int Run::getCapacity() {
    return capacity;
}
double Run::getErrorRate() {
    return error_rate;
}
int Run::getBitsetSize() {
    return bitset_size;
}