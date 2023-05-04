#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <filesystem>
#include <memory>
#include <algorithm>
#include <mutex>
#include <queue>
#include <vector>
#include <random>
#include "../lib/binary_search.hpp"
#include "run.hpp"
#include "lsm_tree.hpp"
#include "memtable.hpp"
#include "utils.hpp"

Run::Run(size_t maxKvPairs, double bfErrorRate, bool createFile, size_t levelOfRun, LSMTree* lsmTree = nullptr) :
    maxKvPairs(maxKvPairs),
    bfErrorRate(bfErrorRate),
    levelOfRun(levelOfRun),
    lsmTree(lsmTree),
    bloomFilter(maxKvPairs, bfErrorRate),
    runFilePath(""),
    size(0),
    maxKey(KEY_MIN)
{
    if (createFile) {
        std::string dataDir = lsmTree->getDataDirectory();
        std::filesystem::create_directory(dataDir); // Create the directory if it does not exist

        std::string sstableFileTemplate = dataDir + "/" + SSTABLE_FILE_TEMPLATE;
        
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, std::numeric_limits<int>::max());
        std::string tmpFn;
        std::ofstream tmpOfs;

        do {
            std::string uniqueId = std::to_string(dis(gen));
            tmpFn = sstableFileTemplate + uniqueId + ".bin";
            tmpOfs.open(tmpFn, std::ios::out | std::ios::binary);
        } while(!tmpOfs.is_open());

        tmpOfs.close();
        runFilePath = tmpFn;
        fencePointers.reserve(maxKvPairs / getpagesize());
    }
}


Run::~Run() {}

void Run::deleteFile() {
    remove(runFilePath.c_str());
}

// New function to open the output file stream
void Run::openOutputFileStream(std::ofstream& ofs, const std::string& originatingFunctionError) {
    if (!ofs.is_open()) {
        ofs.open(runFilePath, std::ios::out | std::ios::binary);
        if (!ofs.is_open()) {
            die(originatingFunctionError + ": " + runFilePath);
        }
    }
}

// New function to open the input file stream
void Run::openInputFileStream(std::ifstream& ifs, const std::string& originatingFunctionError) {
    if (!ifs.is_open()) {
        ifs.open(runFilePath, std::ios::in | std::ios::binary);
        if (!ifs.is_open()) {
            die(originatingFunctionError + ": " + runFilePath);
        }
    }
}

// New function to close the output file stream
void Run::closeOutputFileStream(std::ofstream& ofs) {
    if (ofs.is_open()) {
        ofs.close();
    }
}

// New function to close the input file stream
void Run::closeInputFileStream(std::ifstream& ifs) {
    if (ifs.is_open()) {
        ifs.close();
    }
}

void Run::flush(std::unique_ptr<std::vector<kvPair>> kvPairs) {
    std::ofstream ofs;
    {
        std::shared_lock<std::shared_mutex> lock(sizeMutex);
        if (size >= maxKvPairs) {
            die("Run::flush: Attempting to add to full Run: " + runFilePath);
        }
    }
    // First pass: Add Bloom filters and fence pointers
    size_t idx = 0;

    for (const auto &kv : *kvPairs) {
        addToBloomFilter(kv.key);

        if (idx % getpagesize() == 0) {
            addFencePointer(kv.key);
        }
        if (kv.key > getMaxKey()) {
            setMaxKey(kv.key);
        }
        ++idx;
    }
    // Second pass: Write the data to the Run file
    openOutputFileStream(ofs, "Run::flush: Failed to open file for Run");
    ofs.write(reinterpret_cast<const char*>(kvPairs->data()), sizeof(kvPair) * kvPairs->size());
    ofs.flush();
    closeOutputFileStream(ofs);
    setSize(kvPairs->size());
}


void Run::setFirstAndLastKeys(KEY_t first, KEY_t last) {
    firstKey = first;
    lastKey = last;
}

std::unique_ptr<VAL_t> Run::get(KEY_t key) {
    size_t runSize;
    std::ifstream ifs;
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
    auto iter = std::upper_bound(fencePointersCopy.begin(), fencePointersCopy.end(), key);
    size_t pageIndex = std::distance(fencePointersCopy.begin(), iter) - 1;

    // Calculate the start and end position of the range to search based on the page index
    size_t start = pageIndex * getpagesize();
    size_t end = (pageIndex + 1 == fencePointersCopy.size()) ? runSize : (pageIndex + 1) * getpagesize();
    
    // Start the timer for the query
    auto start_time = std::chrono::high_resolution_clock::now();

    std::unique_ptr<kvPair> kv;
    {
        openInputFileStream(ifs, "Run::get: Failed to open file for Run");
        std::size_t keyPos;
        std::tie(keyPos, kv) = binarySearchInRange(ifs, start, end, key);
        closeInputFileStream(ifs);
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
std::pair<size_t, std::unique_ptr<kvPair>> Run::binarySearchInRange(std::ifstream &ifs, size_t start, size_t end, KEY_t key) {
    while (start <= end) {
        size_t mid = start + (end - start) / 2;

        // Read the key-value pair at the mid index
        kvPair kv;
        ifs.seekg(mid * sizeof(kvPair), std::ios::beg);
        ifs.read(reinterpret_cast<char*>(&kv), sizeof(kvPair));

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
std::vector<kvPair> Run::range(KEY_t start, KEY_t end) {
    std::ifstream ifs;
    size_t searchPageStart, runSize;
    std::vector<kvPair> rangeVec;

    // Check if the run is empty
    {
        std::shared_lock<std::shared_mutex> lock(sizeMutex);
        runSize = size;
    }
    // Check if the run is empty. If so, return an empty result set.
    if (runSize == 0) {
        return rangeVec;
    }

    auto fencePointersCopy = getFencePointers();

    // Check if the specified range is outside the range of keys in the run. If so, return an empty result set.
    if (end <= fencePointersCopy.front() || start > getMaxKey()) {
        return rangeVec;
    }

    // Use binary search to identify the starting fence pointer index where the start key might be located.
    auto iterStart = std::upper_bound(fencePointersCopy.begin(), fencePointersCopy.end(), start);
    searchPageStart = std::distance(fencePointersCopy.begin(), iterStart) - 1;

    // Start the timer for the query
    auto start_time = std::chrono::high_resolution_clock::now();

    size_t pageStart = searchPageStart * getpagesize();
    size_t pageEnd = (searchPageStart + 1 == fencePointersCopy.size()) ? runSize : (searchPageStart + 1) * getpagesize();

    openInputFileStream(ifs, "Run::range: Failed to open file for Run");
    std::pair<size_t, std::unique_ptr<kvPair>> startPosResult = binarySearchInRange(ifs, pageStart, pageEnd, start);
    std::unique_ptr<kvPair> startPosKvPair = std::move(startPosResult.second);

    size_t rangeStartIndex;
    if (startPosKvPair != nullptr) {
        rangeVec.push_back(*startPosKvPair);
        // Start key was already found so don't need to read again
        rangeStartIndex = startPosResult.first + 1;
    } else {
        // Start key was not found so read from beginning
        rangeStartIndex = startPosResult.first;
    }

    for (size_t i = rangeStartIndex; i < runSize; i++) {
        kvPair kv;
        // Read the key-value pair at index i
        ifs.seekg(i * sizeof(kvPair), std::ios::beg);
        ifs.read(reinterpret_cast<char*>(&kv), sizeof(kvPair));

        if (kv.key >= start && kv.key < end) {
            rangeVec.push_back(kv);
        } else if (kv.key >= end) {
            break;
        }
    }
    closeInputFileStream(ifs);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    lsmTree->incrementLevelIoCountAndTime(levelOfRun, duration);
    return rangeVec;
}

std::vector<kvPair> Run::getVector() {
    std::ifstream ifs;
    std::vector<kvPair> vec;
    vec.reserve(size);

    if (lsmTree == nullptr) {
        die("Run::getVector: LSM tree is null");
    }

    // Start the timer for the query
    auto start_time = std::chrono::high_resolution_clock::now();

    // Open the file descriptor
    openInputFileStream(ifs, "Run::getVector: Failed to open file for Run");

    kvPair kv;
    while (ifs.read(reinterpret_cast<char*>(&kv), sizeof(kvPair))) {
        vec.push_back(kv);
    }
    closeInputFileStream(ifs);

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
    bloomFilter.resize(numBits);
}

// Populate the bloom filter. This will typically be called after MONKEY resizes them.
void Run::populateBloomFilter() {
    if (size == 0) {
        return;
    }
    std::ifstream ifs;
    openInputFileStream(ifs, "Run::populateBloomFilter: Failed to open file for Run");
    // Read all the key-value pairs from the Run file and add the keys to the bloom filter
    kvPair kv;
    while (ifs.read(reinterpret_cast<char*>(&kv), sizeof(kvPair))) {
        bloomFilter.add(kv.key);
    }
    closeInputFileStream(ifs);
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

void Run::setSize(size_t newSize) {
    std::unique_lock<std::shared_mutex> lock(sizeMutex);
    size = newSize;
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


