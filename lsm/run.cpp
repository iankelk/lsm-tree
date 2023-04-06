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

Run::Run(long maxKvPairs, double bfErrorRate, bool createFile, LSMTree* lsmTree = nullptr) :
    maxKvPairs(maxKvPairs),
    bfErrorRate(bfErrorRate),
    bloomFilter(maxKvPairs, bfErrorRate),
    tmpFile(""),
    size(0),
    maxKey(0),
    fd(FILE_DESCRIPTOR_UNINITIALIZED),
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

void Run::put(KEY_t key, VAL_t val) {
    int result;
    if (size >= maxKvPairs) {
        die("Run::put: Attempting to add to full Run");
    }

    kvPair kv = {key, val};
    bloomFilter.add(key);
    // Add the key to the fence pointers vector if it is a multiple of the page size. 
    // We can assume it is sorted because the buffer is sorted
    if (size % getpagesize() == 0) {
        fencePointers.push_back(key);
    }
    // If the key is greater than the max key, update the max key
    if (key > maxKey) {
        maxKey = key;
    }

    // Write the key-value pair to the temporary file
    result = write(fd, &kv, sizeof(kvPair));
    assert(result != -1);
    size++;
    lsmTree->incrementIoCount();
}

std::unique_ptr<VAL_t> Run::get(KEY_t key) {
    std::unique_ptr<VAL_t> val;

    // Check if the run is empty
    if (size == 0) {
        return nullptr;
    }

    // Check if the key is in the bloom filter and if it is in the range of the fence pointers
    if (key < fencePointers.front() || key > maxKey || !bloomFilter.contains(key)) {
        return nullptr;
    }
    // Binary search for the page containing the key in the fence pointers vector
    auto fencePointersIter = std::upper_bound(fencePointers.begin(),  fencePointers.end(), key);
    auto pageIndex = static_cast<long>(std::distance(fencePointers.begin(), fencePointersIter)) - 1;

    if (pageIndex < 0) {
        die("Run::get: Negative index from fence pointer");
    }

    // Open the file descriptor for the temporary file
    fd = open(tmpFile.c_str(), O_RDONLY);
    if (fd == FILE_DESCRIPTOR_UNINITIALIZED) {
        die("Run::get: Failed to open temporary file for Run");
    }

    // Search the page for the key
    size_t offset = pageIndex * getpagesize() * sizeof(kvPair);
    size_t numPairsInPage = std::min<long>(maxKvPairs - pageIndex * getpagesize(), getpagesize());

    lseek(fd, offset, SEEK_SET);
    kvPair kv;
    lsmTree->incrementIoCount();
    
    // Binary search for the key-value pair in the page
    size_t left = 0, right = numPairsInPage - 1;
    while (left <= right) {
        size_t mid = left + (right - left) / 2;
        size_t midOffset = offset + mid * sizeof(kvPair);

        lseek(fd, midOffset, SEEK_SET);
        if (read(fd, &kv, sizeof(kvPair)) > 0) {
            if (kv.key == key) {
                val = std::make_unique<VAL_t>(kv.value);
                lsmTree->incrementBfTruePositives();
                truePositives++;
                break;
            } else if (kv.key < key) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        } else {
            break;
        }
    }
    if (val == nullptr) {
        // If the key was not found, increment the false positive count
        lsmTree->incrementBfFalsePositives();
        falsePositives++;
    }
    closeFile();
    return val;
}

// Return a map of all the key-value pairs in the range [start, end]
std::map<KEY_t, VAL_t> Run::range(KEY_t start, KEY_t end) {
    size_t searchPageStart, searchPageEnd;

    // Initialize an empty map
    std::map<KEY_t, VAL_t> rangeMap;

    // Check if the run is empty
    if (size == 0) {
        return rangeMap;
    }

    // Check if the range is in the range of the fence pointers
    if (end < fencePointers.front() || start > maxKey) {
        return rangeMap;
    }
    // Check if the start of the range is less than the first fence pointer
    if (start < fencePointers.front()) {
        searchPageStart = 0;
    } else {
        // Binary search for the page containing the start key in the fence pointers vector
        auto fencePointersIter = std::upper_bound(fencePointers.begin(),  fencePointers.end(), start);
        searchPageStart = static_cast<long>(std::distance(fencePointers.begin(), fencePointersIter)) - 1;
    }
    // Check if the end of the range is greater than the max key
    if (end > maxKey) {
        searchPageEnd = fencePointers.size();
    } else {
        // Binary search for the page containing the end key in the fence pointers vector
        auto fencePointersIter = std::upper_bound(fencePointers.begin(),  fencePointers.end(), end);
        searchPageEnd = static_cast<long>(std::distance(fencePointers.begin(), fencePointersIter));
    }

    // Check that the start and end page indices are valid
    if (searchPageStart < 0 || searchPageEnd < 0) {
        die("Run::range: Negative index from fence pointer");
    }
    // Check that the start page index is less than the end page index
    if (searchPageStart >= searchPageEnd) {
        die("Run::range: Start page index is greater than or equal to end page index");
    }
    // Open the file descriptor for the temporary file
    fd = open(tmpFile.c_str(), O_RDONLY);
    if (fd == FILE_DESCRIPTOR_UNINITIALIZED) {
        die("Run::range: Failed to open temporary file for Run");
    }
    lsmTree->incrementIoCount();
    bool stopSearch = false;
    // Search the pages for the keys in the range
    for (size_t pageIndex = searchPageStart; pageIndex < searchPageEnd; pageIndex++) {
        size_t offset = pageIndex * getpagesize() * sizeof(kvPair);
        size_t offset_end = searchPageEnd * getpagesize() * sizeof(kvPair);
        lseek(fd, offset, SEEK_SET);
        kvPair kv;
        // Search the page and keep the most recently added value'
        while (read(fd, &kv, sizeof(kvPair)) > 0 && offset < offset_end) {
            if (kv.key >= start && kv.key <= end) {
                rangeMap[kv.key] = kv.value;
            } else if (kv.key > end) {
                stopSearch = true;
                break;
            }
            offset += sizeof(kvPair);
        }
        if (stopSearch) {
            break;
        }  
    }
    closeFile();

    // If the last key in the range is the end key, remove it since end is not inclusive
    if (rangeMap.size() > 0 && rangeMap.rbegin()->first == end) {
        rangeMap.erase(rangeMap.rbegin()->first);
    }
    return rangeMap;
}

 std::map<KEY_t, VAL_t> Run::getMap() {
    std::map<KEY_t, VAL_t> map;

    if (lsmTree == nullptr) {
        die("Run::getMap: LSM tree is null");
    }
    // Open the file descriptor for the temporary file
    fd = open(tmpFile.c_str(), O_RDONLY);
    if (fd == FILE_DESCRIPTOR_UNINITIALIZED) {
        die("Run::getMap: Failed to open temporary file for Run");
    }
    // Read all the key-value pairs from the temporary file
    lsmTree->incrementIoCount();
    kvPair kv;
    while (read(fd, &kv, sizeof(kvPair)) > 0) {
        map[kv.key] = kv.value;
    }
    closeFile();
    return map;
 }

long Run::getMaxKvPairs() {
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
    ss << "Bloom Filter Size: " << addCommas(std::to_string(bloomFilter.getNumBits())) << ", Num Hash Functions: " << bloomFilter.getNumHashes() << 
    ", FPR: " << bfStatus << ", TP: " << addCommas(std::to_string(truePositives)) << ", FP: " << addCommas(std::to_string(falsePositives))
    << ", Max Keys: " << addCommas(std::to_string(maxKvPairs)) <<  ", Number of Keys: " << addCommas(std::to_string(size));
    return ss.str();
}

void Run::clearBloomFilter() {
    bloomFilter.clear();
}
void Run::resizeBloomFilter(long long numBits) {
    bloomFilter.resize(numBits);
}
void Run::addKeyToBloomFilter(KEY_t key) {
    bloomFilter.add(key);
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
