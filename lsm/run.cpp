#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <string>
#include <filesystem>
#include <algorithm>
#include <mutex>
#include <queue>
#include <vector>
#include "../lib/binary_search.hpp"
#include "run.hpp"
#include "lsm_tree.hpp"
#include "utils.hpp"


thread_local int Run::localFd = FILE_DESCRIPTOR_UNINITIALIZED;

Run::Run(size_t maxKvPairs, double bfErrorRate, bool createFile, size_t levelOfRun, LSMTree* lsmTree = nullptr) :
    maxKvPairs(maxKvPairs),
    bfErrorRate(bfErrorRate),
    bloomFilter(maxKvPairs, bfErrorRate),
    runFilePath(""),
    size(0),
    maxKey(KEY_MIN),
    levelOfRun(levelOfRun),
    lsmTree(lsmTree) 
{

    if (createFile) {
        std::string dataDir = lsmTree->getDataDirectory();
        std::filesystem::create_directory(dataDir); // Create the directory if it does not exist

        std::string sstableFileTemplate = dataDir + "/" + SSTABLE_FILE_TEMPLATE;
        char tmpFn[sstableFileTemplate.size() + 1];
        std::strcpy(tmpFn, sstableFileTemplate.c_str());
        
        int suffixLength = 4; // Length of ".bin" suffix
        localFd = mkstemps(tmpFn, suffixLength);
        if (localFd == FILE_DESCRIPTOR_UNINITIALIZED) {
            die("Run::Constructor: Failed to create file for Run: " + runFilePath);
        }
        runFilePath = tmpFn;
        fencePointers.reserve(maxKvPairs / getpagesize());
    }
}

Run::~Run() {
    closeFile();
}

void Run::deleteFile() {
    closeFile();
    remove(runFilePath.c_str());
}

void Run::openFileReadOnly(const std::string& originatingFunctionError) {
    if (localFd == FILE_DESCRIPTOR_UNINITIALIZED) {
        localFd = open(runFilePath.c_str(), O_RDONLY);
        if (localFd == FILE_DESCRIPTOR_UNINITIALIZED) {
            die(originatingFunctionError + ": " + runFilePath);
        }
    }
}


void Run::closeFile() {
    if (localFd != FILE_DESCRIPTOR_UNINITIALIZED) {
        close(localFd);
        localFd = FILE_DESCRIPTOR_UNINITIALIZED;
    }
}

void Run::put(KEY_t key, VAL_t val) {
    int result, runSize;
    {
        std::shared_lock<std::shared_mutex> lock(sizeMutex);
        runSize = size;
    }
    if (runSize >= maxKvPairs) {
            die("Run::put: Attempting to add to full Run: " + runFilePath);
    }
    kvPair kv = {key, val};
    addToBloomFilter(key);
    // Add the key to the fence pointers vector if it is a multiple of the page size. 
    // We can assume it is sorted because the buffer is sorted before it is written to the run
    if (runSize % getpagesize() == 0) {
        addFencePointer(key);
    }

    // If the key is greater than the max key, update the max key
    if (key > getMaxKey()) {
        setMaxKey(key);
    }

    // Write the key-value pair to the Run file
    result = write(localFd, &kv, sizeof(kvPair));
    if (result == -1) {
        die("Run::put: Failed to write to Run file: " + runFilePath);
    }
    incrementSize();
}

void Run::setFirstAndLastKeys(KEY_t first, KEY_t last) {
    firstKey = first;
    lastKey = last;
}

std::unique_ptr<VAL_t> Run::get(KEY_t key) {
    size_t runSize;
    {
        std::shared_lock<std::shared_mutex> lock(sizeMutex);
        runSize = size;
    }
    // Check if the run is empty
    if (runSize == 0) {
        return nullptr;
    }
    auto fencePointersCopy = getFencePointers();
    {
        std::shared_lock<std::shared_mutex> lock(bloomFilterMutex);
        // Check if the key is in the bloom filter and if it is in the range of the fence pointers
        if (key < fencePointersCopy.front() || key > getMaxKey() || !bloomFilter.contains(key)) {
            return nullptr;
        }
    }
    // Perform a binary search on the fence pointers to find the page that may contain the key
    auto iter = branchless_lower_bound(fencePointersCopy.begin(), fencePointersCopy.end(), key);
    auto pageIndex = std::distance(fencePointersCopy.begin(), iter) - 1;

    // Calculate the start and end position of the range to search based on the page index
    size_t start = pageIndex * getpagesize();
    size_t end = (pageIndex + 1 == fencePointersCopy.size()) ? runSize : (pageIndex + 1) * getpagesize();
    
    // Start the timer for the query
    auto start_time = std::chrono::high_resolution_clock::now();

    std::unique_ptr<kvPair> kv;
    {
        // Open the file descriptor
        openFileReadOnly("Run::get: Failed to open file for Run");
        std::size_t keyPos;
        // Perform binary search within the identified range to find the key
        std::tie(keyPos, kv) = binarySearchInRange(localFd, start, end, key);
        // Close the file descriptor
        closeFile();
    }
    if (kv == nullptr) {
        // If the key was not found, increment the false positive count
        lsmTree->incrementBfFalsePositives();
        incrementFalsePositives();
    } else {
        // If the key was found, increment the true positive count
        lsmTree->incrementBfTruePositives();
        incrementTruePositives();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    lsmTree->incrementLevelIoCountAndTime(levelOfRun, duration);

    return (kv == nullptr) ? nullptr : std::make_unique<VAL_t>(kv->value);
}

// Return a pair of the position of a KvPair, and a pointer to the KvPair
std::pair<size_t, std::unique_ptr<kvPair>> Run::binarySearchInRange(int fd, size_t start, size_t end, KEY_t key) {
    while (start <= end) {
        size_t mid = start + (end - start) / 2;

        // Read the key-value pair at the mid index
        kvPair kv;
        ssize_t bytes_read = pread(fd, &kv, sizeof(kvPair), mid * sizeof(kvPair));
        if (bytes_read != sizeof(kvPair)) {
            SyncedCerr() << "ERROR: Read only " << bytes_read << " bytes from file" << std::endl;
        }
        if (kv.key == key) {
            std::unique_ptr<kvPair> found_kv = std::make_unique<kvPair>(kv);
            return std::make_pair(mid, std::move(found_kv));
        } else if (kv.key < key) {
            start = mid + 1;
        } else {
            end = mid - 1;
        }
    }
    return std::make_pair(start, nullptr);
}

// Return a map of all the key-value pairs in the range [start, end)
std::map<KEY_t, VAL_t> Run::range(KEY_t start, KEY_t end) {
    size_t searchPageStart, runSize;
    std::map<KEY_t, VAL_t> rangeMap;

    // Check if the run is empty
    {
        std::shared_lock<std::shared_mutex> lock(sizeMutex);
        runSize = size;
    }

    // Check if the run is empty. If so, return an empty result set.
    if (runSize == 0) {
        return rangeMap;
    }

    auto fencePointersCopy = getFencePointers();

    // Check if the specified range is outside the range of keys in the run. If so, return an empty result set.
    if (end <= fencePointersCopy.front() || start > getMaxKey()) {
        return rangeMap;
    }

    // Use binary search to identify the starting fence pointer index where the start key might be located.
    auto iterStart = branchless_lower_bound(fencePointersCopy.begin(), fencePointersCopy.end(), start);
    searchPageStart = std::distance(fencePointersCopy.begin(), iterStart) - 1;

    // Start the timer for the query
    auto start_time = std::chrono::high_resolution_clock::now();

    size_t pageStart = searchPageStart * getpagesize();
    size_t pageEnd = (searchPageStart + 1 == fencePointersCopy.size()) ? runSize : (searchPageStart + 1) * getpagesize();

    openFileReadOnly("Run::get: Failed to open file for Run");
    std::pair<size_t, std::unique_ptr<kvPair>> startPosResult = binarySearchInRange(localFd, pageStart, pageEnd, start);
    std::unique_ptr<kvPair> startPosKvPair = std::move(startPosResult.second);

    size_t rangeStartIndex;
    if (startPosKvPair != nullptr) {
        rangeMap[startPosKvPair->key] = startPosKvPair->value;
        // Start key was already found so don't need to read again
        rangeStartIndex = startPosResult.first + 1;
    } else {
        // Start key was not found so read from beginning
        rangeStartIndex = startPosResult.first;
    }

    for (size_t i = rangeStartIndex; i < runSize; i++) {
        kvPair kv;
        ssize_t bytes_read = pread(localFd, &kv, sizeof(kvPair), i * sizeof(kvPair));
        if (bytes_read != sizeof(kvPair)) {
            SyncedCerr() << "ERROR: Read only " << bytes_read << " bytes from file" << std::endl;
        }

        if (kv.key >= start && kv.key < end) {
            rangeMap[kv.key] = kv.value;
        } else if (kv.key >= end) {
            break;
        }
    }
    closeFile();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    lsmTree->incrementLevelIoCountAndTime(levelOfRun, duration);

    // Return the data structure containing the key-value pairs within the specified range.
    return rangeMap;
}

std::vector<kvPair> Run::getVector() {
    std::vector<kvPair> vec;
    vec.reserve(size);

    if (lsmTree == nullptr) {
        die("Run::getVector: LSM tree is null");
    }

    // Start the timer for the query
    auto start_time = std::chrono::high_resolution_clock::now();

    // Open the file descriptor
    openFileReadOnly("Run::getVector: Failed to open file for Run");

    kvPair kv;
    while (read(localFd, &kv, sizeof(kvPair)) > 0) {
        vec.push_back(kv);
    }
    closeFile();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    lsmTree->incrementLevelIoCountAndTime(levelOfRun, duration);
    return vec;
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
    j["runFilePath"] = runFilePath;
    j["size"] = size;
    j["maxKey"] = maxKey;
    j["truePositives"] = truePositives;
    j["falsePositives"] = falsePositives;
    j["firstKey"] = firstKey;
    j["lastKey"] = lastKey;
    return j;
}

void Run::deserialize(const json& j) {
    maxKvPairs = j["maxKvPairs"];
    bfErrorRate = j["bfErrorRate"];

    bloomFilter.deserialize(j["bloomFilter"]);
    fencePointers = j["fencePointers"].get<std::vector<KEY_t>>();
    runFilePath = j["runFilePath"];
    size = j["size"];
    maxKey = j["maxKey"];
    truePositives = j["truePositives"];
    falsePositives = j["falsePositives"];
    firstKey = j["firstKey"];
    lastKey = j["lastKey"];
}

float Run::getBfFalsePositiveRate() {
    std::shared_lock<std::shared_mutex> lockFP(falsePositivesMutex, std::defer_lock);
    std::shared_lock<std::shared_mutex> lockTP(truePositivesMutex, std::defer_lock);

    std::lock(lockFP, lockTP);

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

// Get the summary for a Run's bloom filter and return a map of variable names and their values.
std::map<std::string, std::string> Run::getBloomFilterSummary() {
    std::map<std::string, std::string> summary;

    // If the bloom filter has not been used, don't print the false positive rate and just print "Unused"
    std::string bfStatus = getBfFalsePositiveRate() == BLOOM_FILTER_UNUSED ? "Unused" : std::to_string(getBfFalsePositiveRate());

    summary["bloomFilterSize"] = addCommas(std::to_string(getBloomFilterNumBits()));
    summary["hashFunctions"] = std::to_string(bloomFilter.getNumHashes());
    summary["keys"] = addCommas(std::to_string(size)) + " (Max " + addCommas(std::to_string(maxKvPairs)) + ")";
    summary["theoreticalFPR"] = std::to_string(bloomFilter.theoreticalErrorRate());
    summary["truePositives"] = addCommas(std::to_string(truePositives));
    summary["falsePositives"] = addCommas(std::to_string(getFalsePositives()));
    summary["measuredFPR"] = bfStatus;

    return summary;
}

void Run::resizeBloomFilterBitset(size_t numBits) {
    bloomFilter.resize(bloomFilter.getNumBits());
}

// Populate the bloom filter. This will typically be called after MONKEY resizes them.
void Run::populateBloomFilter() {
    if (size == 0) {
        return;
    }
    openFileReadOnly("Run::populateBloomFilter: Failed to open file for Run");
    // Read all the key-value pairs from the Run file and add the keys to the bloom filter
    kvPair kv;
    while (read(localFd, &kv, sizeof(kvPair)) > 0) {
        bloomFilter.add(kv.key);
    }
    closeFile();
}

void Run::incrementFalsePositives() { 
    std::unique_lock<std::shared_mutex> lock(falsePositivesMutex);
    falsePositives++;
}
void Run::incrementTruePositives() {
    std::unique_lock<std::shared_mutex> lock(truePositivesMutex);
    truePositives++; 
}

size_t Run::getFalsePositives() {
    std::shared_lock<std::shared_mutex> lock(falsePositivesMutex);
    return falsePositives;
}

void Run::incrementSize() {
    std::unique_lock<std::shared_mutex> lock(sizeMutex);
    size++;
}

void Run::setMaxKey(KEY_t key) {
    std::unique_lock<std::shared_mutex> lock(maxKeyMutex);
    maxKey = key;
}

KEY_t Run::getMaxKey() {
    std::shared_lock<std::shared_mutex> lock(maxKeyMutex);
    return maxKey;
}

void Run::addFencePointer(KEY_t key) {
    std::unique_lock<std::shared_mutex> lock(fencePointersMutex);
    fencePointers.push_back(key);
}

std::vector<KEY_t> Run::getFencePointers() {
    std::shared_lock<std::shared_mutex> lock(fencePointersMutex);
    return fencePointers;
}

void Run::addToBloomFilter(KEY_t key) {
    std::unique_lock<std::shared_mutex> lock(bloomFilterMutex);
    bloomFilter.add(key);
}


