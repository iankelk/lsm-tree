#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <string>
#include <filesystem>
#include <algorithm>
#include "run.hpp"
#include "lsm_tree.hpp"
#include "utils.hpp"

Run::Run(size_t maxKvPairs, double bfErrorRate, bool createFile, size_t levelOfRun, LSMTree* lsmTree = nullptr) :
    maxKvPairs(maxKvPairs),
    bfErrorRate(bfErrorRate),
    bloomFilter(maxKvPairs, bfErrorRate),
    tmpFile(""),
    size(0),
    maxKey(0),
    fd(FILE_DESCRIPTOR_UNINITIALIZED),
    levelOfRun(levelOfRun),
    lsmTree(lsmTree) 
{

    if (createFile) {
        std::string dataDir = DATA_DIRECTORY;
        std::filesystem::create_directory(dataDir); // Create the directory if it does not exist

        std::string sstableFileTemplate = dataDir + SSTABLE_FILE_TEMPLATE;
        char tmpFn[sstableFileTemplate.size() + 1];
        std::strcpy(tmpFn, sstableFileTemplate.c_str());
        
        int suffixLength = 4; // Length of ".bin" suffix
        fd = mkstemps(tmpFn, suffixLength);
        if (fd == FILE_DESCRIPTOR_UNINITIALIZED) {
            die("Run::Constructor: Failed to create temporary file for Run");
        }
        tmpFile = tmpFn;
        fencePointers.reserve(maxKvPairs / getpagesize());
    }
}

Run::~Run() {
    closeFile();
}

void Run::deleteFile() {
    closeFile();
    remove(tmpFile.c_str());
}

// Close the file descriptor for the temporary file for when we are performing point or range queries
void Run::closeFile() {
    close(fd);
    fd = FILE_DESCRIPTOR_UNINITIALIZED;
}

void Run::openFile(std::string originatingFunctionError, int flags) {
    if (fd == FILE_DESCRIPTOR_UNINITIALIZED) {
        fd = open(tmpFile.c_str(), flags);
        if (fd == FILE_DESCRIPTOR_UNINITIALIZED) {
            die(originatingFunctionError);
        }
    }
}

void Run::put(KEY_t key, VAL_t val) {
    int result;
    if (size >= maxKvPairs) {
        die("Run::put: Attempting to add to full Run");
    }

    kvPair kv = {key, val};
    bloomFilter.add(key);
    // Add the key to the fence pointers vector if it is a multiple of the page size. 
    // We can assume it is sorted because the buffer is sorted before it is written to the run
    if (size % getpagesize() == 0) {
        fencePointers.push_back(key);
    }

    // If the key is greater than the max key, update the max key
    if (key > maxKey) {
        maxKey = key;
    }

    // Write the key-value pair to the temporary file
    result = write(fd, &kv, sizeof(kvPair));
    if (result == -1) {
        die("Run::put: Failed to write to temporary file");
    }
    size++;
}

std::unique_ptr<VAL_t> Run::get(KEY_t key) {
    // Check if the run is empty
    if (size == 0) {
        return nullptr;
    }

    // Check if the key is in the bloom filter and if it is in the range of the fence pointers
    if (key < fencePointers.front() || key > maxKey || !bloomFilter.contains(key)) {
        return nullptr;
    }

    // Perform a binary search on the fence pointers to find the page that may contain the key
    auto iter = std::upper_bound(fencePointers.begin(), fencePointers.end(), key);
    auto pageIndex = std::distance(fencePointers.begin(), iter) - 1;

    // Calculate the start and end position of the range to search based on the page index
    size_t start = pageIndex * getpagesize();
    size_t end = (pageIndex + 1 == fencePointers.size()) ? size : (pageIndex + 1) * getpagesize();
    
    // Start the timer for the query
    auto start_time = std::chrono::high_resolution_clock::now();

    // Open the file descriptor
    openFile("Run::get: Failed to open temporary file for Run", O_RDONLY);

    // Perform binary search within the identified range to find the key
    auto[keyPos, val] = binarySearchInRange(fd, start, end, key);

    if (val == nullptr) {
        // If the key was not found, increment the false positive count
        lsmTree->incrementBfFalsePositives();
        falsePositives++;
    }

    closeFile();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    lsmTree->incrementIoCount();
    lsmTree->incrementLevelIoCountAndTime(levelOfRun, duration);

    return std::move(val);
}

std::pair<size_t, std::unique_ptr<VAL_t>> Run::binarySearchInRange(int fd, size_t start, size_t end, KEY_t key) {
    std::unique_ptr<VAL_t> val;

    while (start <= end) {
        size_t mid = start + (end - start) / 2;

        // Read the key-value pair at the mid index
        kvPair kv;
        pread(fd, &kv, sizeof(kvPair), mid * sizeof(kvPair));

        if (kv.key == key) {
            val = std::make_unique<VAL_t>(kv.value);
            return std::make_pair(mid, std::move(val));
        } else if (kv.key < key) {
            start = mid + 1;
        } else {
            end = mid - 1;
        }
    }

    return std::make_pair(0, nullptr);
}

// Return a map of all the key-value pairs in the range [start, end]
std::map<KEY_t, VAL_t> Run::range(KEY_t start, KEY_t end) {
    size_t searchPageStart, searchPageEnd;

    // Initialize an empty map
    std::map<KEY_t, VAL_t> rangeMap;

    // Check if the run is empty. If so, return an empty result set.
    if (size == 0) {
        return rangeMap;
    }

    // Check if the specified range is outside the range of keys in the run. If so, return an empty result set.
    if (end < fencePointers.front() || start > maxKey) {
        return rangeMap;
    }

    // Use binary search to identify the starting fence pointer index where the start key might be located.
    auto iterStart = std::lower_bound(fencePointers.begin(), fencePointers.end(), start);
    searchPageStart = std::distance(fencePointers.begin(), iterStart);

    // Find the ending fence pointer index for the end key using binary search.
    auto iterEnd = std::lower_bound(fencePointers.begin(), fencePointers.end(), end);
    searchPageEnd = std::distance(fencePointers.begin(), iterEnd);

    // Start the timer for the query
    auto start_time = std::chrono::high_resolution_clock::now();

    // Open the file descriptor
    openFile("Run::range: Failed to open temporary file for Run", O_RDONLY);

    // Iterate through the fence pointers between the starting and ending fence pointer indices (inclusive),
    // and perform binary searches within the corresponding pages for keys within the specified range.
    for (size_t pageIndex = searchPageStart; pageIndex <= searchPageEnd; pageIndex++) {
        size_t pageStart = pageIndex * getpagesize();
        size_t pageEnd = (pageIndex + 1 == fencePointers.size()) ? size : (pageIndex + 1) * getpagesize();
        auto startPosPair = binarySearchInRange(fd, pageStart, pageEnd, start);

        if (startPosPair.second != nullptr) {
            rangeMap[startPosPair.first] = *startPosPair.second;
        }

        for (size_t i = startPosPair.first + 1; i < pageEnd; i++) {
            kvPair kv;
            pread(fd, &kv, sizeof(kvPair), i * sizeof(kvPair));

            if (kv.key >= start && kv.key < end) {
                rangeMap[kv.key] = kv.value;
            } else if (kv.key >= end) {
                break;
            }
        }
    }

    // Close the file descriptor.
    closeFile();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    lsmTree->incrementIoCount();
    lsmTree->incrementLevelIoCountAndTime(levelOfRun, duration);

    // If the last key in the range is the end key, remove it since the RANGE query is not inclusive for the end key.
    if (rangeMap.size() > 0 && rangeMap.rbegin()->first == end) {
        rangeMap.erase(rangeMap.rbegin()->first);
    }

    // Return the data structure containing the key-value pairs within the specified range.
    return rangeMap;
}

 std::map<KEY_t, VAL_t> Run::getMap() {
    std::map<KEY_t, VAL_t> map;

    if (lsmTree == nullptr) {
        die("Run::getMap: LSM tree is null");
    }

    // Start the timer for the query
    auto start_time = std::chrono::high_resolution_clock::now();

    // Open the file descriptor
    openFile("Run::getMap: Failed to open temporary file for Run", O_RDONLY);

    kvPair kv;
    while (read(fd, &kv, sizeof(kvPair)) > 0) {
        map[kv.key] = kv.value;
    }
    closeFile();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    // Read all the key-value pairs from the temporary file
    lsmTree->incrementIoCount();
    lsmTree->incrementLevelIoCountAndTime(levelOfRun, duration);

    return map;
 }

size_t Run::getMaxKvPairs() {
    return maxKvPairs;
}

json Run::serialize() const {
    nlohmann::json j;
    j["maxKvPairs"] = maxKvPairs;
    j["bfErrorRate"] = bfErrorRate;
    j["bloomFilter"] = bloomFilter.serialize();
    j["fencePointers"] = fencePointers;
    j["tmpFile"] = tmpFile;
    j["size"] = size;
    j["maxKey"] = maxKey;
    j["truePositives"] = truePositives;
    j["falsePositives"] = falsePositives;
    return j;
}

void Run::deserialize(const json& j) {
    maxKvPairs = j["maxKvPairs"];
    bfErrorRate = j["bfErrorRate"];

    bloomFilter.deserialize(j["bloomFilter"]);
    fencePointers = j["fencePointers"].get<std::vector<KEY_t>>();
    tmpFile = j["tmpFile"];
    size = j["size"];
    maxKey = j["maxKey"];
    truePositives = j["truePositives"];
    falsePositives = j["falsePositives"];
}

float Run::getBfFalsePositiveRate() {
    int total = truePositives + falsePositives;
    if (total > 0) {
        return (float)falsePositives / total;
    } else {
        return BLOOM_FILTER_UNUSED; // No false positives or true positives
    }
}

void Run::setLSMTree(LSMTree* lsmTree) {
    this->lsmTree = lsmTree;
}

// Example:
// Run 0: Bloom Filter Size: 25,126,656, Num Hash Functions: 16, FPR: 0.046875, TP: 183, FP: 9, Max Keys: 1,048,576, Number of Keys: 1,048,576
std::string Run::getBloomFilterSummary() {
    // If the bloom filter has not been used, don't print the false positive rate and just print "Unused"
    std::string bfStatus = getBfFalsePositiveRate() == BLOOM_FILTER_UNUSED ? "Unused" : std::to_string(getBfFalsePositiveRate());
    std::stringstream ss;
    ss << "Bloom Filter Size: " << addCommas(std::to_string(getBloomFilterNumBits())) << ", Num Hash Functions: " << bloomFilter.getNumHashes() << 
    ", FPR: " << bfStatus << ", TP: " << addCommas(std::to_string(truePositives)) << ", FP: " << addCommas(std::to_string(falsePositives))
    << ", Max Keys: " << addCommas(std::to_string(maxKvPairs)) <<  ", Number of Keys: " << addCommas(std::to_string(size)) 
    << ", Theoretical FPR: " << std::to_string(bloomFilter.theoreticalErrorRate());
    return ss.str();
}

void Run::resizeBloomFilterBitset(size_t numBits) {
    bloomFilter.resize(bloomFilter.getNumBits());
}

// Populate the bloom filter. This will typically be called after MONKEY resizes them.
void Run::populateBloomFilter() {
    if (size == 0) {
        return;
    }
    // Open the file descriptor for the temporary file
    fd = open(tmpFile.c_str(), O_RDONLY);
    if (fd == FILE_DESCRIPTOR_UNINITIALIZED) {
        die("Run::populateBloomFilter: Failed to open temporary file for Run");
    }
    // Read all the key-value pairs from the temporary file and add the keys to the bloom filter
    kvPair kv;
    while (read(fd, &kv, sizeof(kvPair)) > 0) {
        bloomFilter.add(kv.key);
    }
    // Close the file descriptor
    closeFile();
}
