#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include "run.hpp"
#include "lsm_tree.hpp"

void getNumOpenFiles();

Run::Run(long max_kv_pairs, double bf_error_rate, int bf_bitset_size, bool createFile, LSMTree* lsm_tree = nullptr) :
    max_kv_pairs(max_kv_pairs),
    bf_error_rate(bf_error_rate),
    bf_bitset_size(bf_bitset_size),
    bloom_filter(max_kv_pairs, bf_error_rate),
    tmp_file(""),
    size(0),
    max_key(0),
    fd(FILE_DESCRIPTOR_UNINITIALIZED),
    lsm_tree(lsm_tree)

{
    if (createFile) {
        char tmp_fn[] = SSTABLE_FILE_TEMPLATE;
        fd = mkstemp(tmp_fn);
        if (fd == FILE_DESCRIPTOR_UNINITIALIZED) {
            throw std::runtime_error("Failed to create temporary file for Run");
        }
        tmp_file = tmp_fn;
        fence_pointers.reserve(max_kv_pairs / getpagesize());
    }    
}

Run::~Run() {
    closeFile();
}

void Run::deleteFile() {
    closeFile();
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
        throw std::runtime_error("Attempting to add to full Run");
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

std::unique_ptr<VAL_t> Run::get(KEY_t key) {
    std::unique_ptr<VAL_t> val;

    // Check if the run is empty
    if (size == 0) {
        return nullptr;
    }

    // Check if the key is in the bloom filter and if it is in the range of the fence pointers
    if (key < fence_pointers.front() || key > max_key || !bloom_filter.contains(key)) {
        return nullptr;
    }
    // Binary search for the page containing the key in the fence pointers vector
    auto fence_pointers_iter = std::upper_bound(fence_pointers.begin(),  fence_pointers.end(), key);
    auto page_index = static_cast<long>(std::distance(fence_pointers.begin(), fence_pointers_iter)) - 1;

    if (page_index < 0) {
        throw std::runtime_error("Negative index from fence pointer");
    }

    // Open the file descriptor for the temporary file
    fd = open(tmp_file.c_str(), O_RDONLY);
    if (fd == FILE_DESCRIPTOR_UNINITIALIZED) {
        throw std::runtime_error("Run::get: Failed to open temporary file for Run");
    }

    // Search the page for the key
    long offset = page_index * getpagesize() * sizeof(kv_pair);
    long end = offset + getpagesize() * sizeof(kv_pair);

    lseek(fd, offset, SEEK_SET);
    kv_pair kv;
    // Read the key-value pairs from the temporary file starting at the offset and ending at the end of the page
    // TODO: This is a linear search. Could be better with a binary search?
    // while (read(fd, &kv, sizeof(kv_pair)) > 0 && offset < end) {
    while (read(fd, &kv, sizeof(kv_pair)) > 0 && offset < end) {
        if (kv.key == key) {
            val = std::make_unique<VAL_t>(kv.value);
            lsm_tree->incrementBfTruePositives();
            break;
        }
        offset += sizeof(kv_pair);
    }
    if (val == nullptr) {
        // If the key was not found, increment the false positive count
        lsm_tree->incrementBfFalsePositives();
    }
    closeFile();
    return val;
}

// Return a map of all the key-value pairs in the range [start, end]
std::map<KEY_t, VAL_t> Run::range(KEY_t start, KEY_t end) {
    long searchPageStart, searchPageEnd;

    // Initialize an empty map
    std::map<KEY_t, VAL_t> range_map;

    // Check if the run is empty
    if (size == 0) {
        return range_map;
    }

    // Check if the range is in the range of the fence pointers
    if (end < fence_pointers.front() || start > max_key) {
        return range_map;
    }
    // Check if the start of the range is less than the first fence pointer
    if (start < fence_pointers.front()) {
        searchPageStart = 0;
    } else {
        // Binary search for the page containing the start key in the fence pointers vector
        auto fence_pointers_iter = std::upper_bound(fence_pointers.begin(),  fence_pointers.end(), start);
        searchPageStart = static_cast<long>(std::distance(fence_pointers.begin(), fence_pointers_iter)) - 1;
    }
    // Check if the end of the range is greater than the max key
    if (end > max_key) {
        searchPageEnd = fence_pointers.size();
    } else {
        // Binary search for the page containing the end key in the fence pointers vector
        auto fence_pointers_iter = std::upper_bound(fence_pointers.begin(),  fence_pointers.end(), end);
        searchPageEnd = static_cast<long>(std::distance(fence_pointers.begin(), fence_pointers_iter));
    }

    // Check that the start and end page indices are valid
    if (searchPageStart < 0 || searchPageEnd < 0) {
        throw std::runtime_error("Negative index from fence pointer");
    }
    // Check that the start page index is less than the end page index
    if (searchPageStart >= searchPageEnd) {
        throw std::runtime_error("Start page index is greater than or equal to end page index");
    }
    // Open the file descriptor for the temporary file
    fd = open(tmp_file.c_str(), O_RDONLY);
    if (fd == FILE_DESCRIPTOR_UNINITIALIZED) {
        throw std::runtime_error("Run::range: Failed to open temporary file for Run");
    }
    bool stopSearch = false;
    // Search the pages for the keys in the range
    for (long page_index = searchPageStart; page_index < searchPageEnd; page_index++) {
        long offset = page_index * getpagesize() * sizeof(kv_pair);
        long offset_end = searchPageEnd * getpagesize() * sizeof(kv_pair);
        lseek(fd, offset, SEEK_SET);
        kv_pair kv;
        // Search the page and keep the most recently added value'
        while (read(fd, &kv, sizeof(kv_pair)) > 0 && offset < offset_end) {
            if (kv.key >= start && kv.key <= end) {
                range_map[kv.key] = kv.value;
            } else if (kv.key > end) {
                stopSearch = true;
                break;
            }
            offset += sizeof(kv_pair);
        }
        if (stopSearch) {
            break;
        }  
    }
    closeFile();

    // If the last key in the range is the end key, remove it since end is not inclusive
    if (range_map.size() > 0 && range_map.rbegin()->first == end) {
        range_map.erase(range_map.rbegin()->first);
    }
    return range_map;
}

 std::map<KEY_t, VAL_t> Run::getMap() {
    std::map<KEY_t, VAL_t> map;

    // Open the file descriptor for the temporary file
    fd = open(tmp_file.c_str(), O_RDONLY);
    if (fd == FILE_DESCRIPTOR_UNINITIALIZED) {
        throw std::runtime_error("Run::getMap: Failed to open temporary file for Run");

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

json Run::serialize() const {
    nlohmann::json j;
    j["max_kv_pairs"] = max_kv_pairs;
    j["bf_error_rate"] = bf_error_rate;
    j["bf_bitset_size"] = bf_bitset_size;
    j["bloom_filter"] = bloom_filter.serialize();
    j["fence_pointers"] = fence_pointers;
    j["tmp_file"] = tmp_file;
    j["size"] = size;
    j["max_key"] = max_key;
    return j;
}


// Get number of open files function
void getNumOpenFiles() {
    struct rlimit rlim;
    if (getrlimit(RLIMIT_NOFILE, &rlim) == 0) {
        int fd_count = 0;
        for (int fd = 0; fd < rlim.rlim_cur; fd++) {
            if (fcntl(fd, F_GETFD) != -1) {
                fd_count++;
            }
        }
        std::cout << "Number of open file descriptors: " << fd_count << std::endl;
    } else {
        std::cerr << "Error getting RLIMIT_NOFILE: " << strerror(errno) << std::endl;
    }
}

void Run::deserialize(const json& j) {
    max_kv_pairs = j["max_kv_pairs"];
    bf_error_rate = j["bf_error_rate"];
    bf_bitset_size = j["bf_bitset_size"];

    bloom_filter.deserialize(j["bloom_filter"]);
    fence_pointers = j["fence_pointers"].get<std::vector<KEY_t>>();
    tmp_file = j["tmp_file"];
    size = j["size"];
    max_key = j["max_key"];
}