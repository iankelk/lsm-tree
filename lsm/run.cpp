#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <string>
#include <filesystem>
#include <algorithm>
#include <mutex>
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
    maxKey(0),
    levelOfRun(levelOfRun),
    lsmTree(lsmTree) 
{

    if (createFile) {
        // Ensure that only one thread is creating a file at a time and no other thread is reading 
        // TODO: just in case
        //std::unique_lock<std::shared_mutex> lock(fileMutex);

        std::string dataDir = DATA_DIRECTORY;
        std::filesystem::create_directory(dataDir); // Create the directory if it does not exist

        std::string sstableFileTemplate = dataDir + SSTABLE_FILE_TEMPLATE;
        char tmpFn[sstableFileTemplate.size() + 1];
        std::strcpy(tmpFn, sstableFileTemplate.c_str());
        
        int suffixLength = 4; // Length of ".bin" suffix
        localFd = mkstemps(tmpFn, suffixLength);
        if (localFd == FILE_DESCRIPTOR_UNINITIALIZED) {
            die("Run::Constructor: Failed to create file for Run");
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

int Run::openFile(std::string originatingFunctionError, int flags) {
    if (localFd == FILE_DESCRIPTOR_UNINITIALIZED) {
        localFd = open(runFilePath.c_str(), flags);
        if (localFd == FILE_DESCRIPTOR_UNINITIALIZED) {
            die(originatingFunctionError);
        }
    }
    return localFd;
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
            die("Run::put: Attempting to add to full Run");
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
        die("Run::put: Failed to write to Run file");
    }
    incrementSize();
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
    auto iter = std::upper_bound(fencePointersCopy.begin(), fencePointersCopy.end(), key);
    auto pageIndex = std::distance(fencePointersCopy.begin(), iter) - 1;

    // Calculate the start and end position of the range to search based on the page index
    size_t start = pageIndex * getpagesize();
    size_t end = (pageIndex + 1 == fencePointersCopy.size()) ? runSize : (pageIndex + 1) * getpagesize();

    // {   
        // Ensure that the run file is not being written to while we are reading from it
        // std::shared_lock<std::shared_mutex> lock(fileMutex);
    // }
    
    // Start the timer for the query
    auto start_time = std::chrono::high_resolution_clock::now();

    std::unique_ptr<VAL_t> val;
    std::size_t keyPos;
    {
        // std::shared_lock<std::shared_mutex> lock(fileMutex); // Keep the lock here for localFileDescriptors protection
        // Open the file descriptor
        int localFd = openFile("Run::get: Failed to open file for Run", O_RDONLY);
        // Perform binary search within the identified range to find the key
        std::tie(keyPos, val) = binarySearchInRange(localFd, start, end, key);
        // Close the file descriptor
        closeFile();
    }
    if (val == nullptr) {
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

// Return a map of all the key-value pairs in the range [start, end)
std::map<KEY_t, VAL_t> Run::range(KEY_t start, KEY_t end) {
    size_t searchPageStart, runSize;
    std::map<KEY_t, VAL_t> rangeMap;

    // Check if the run is empty
    {
        // SyncedCout() << "Run::range: Getting sizeMutex lock" << std::this_thread::get_id() << std::endl;
        std::shared_lock<std::shared_mutex> lock(sizeMutex);
        // SyncedCout() << "Run::range: Got sizeMutex lock" << std::this_thread::get_id() << std::endl;
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
    auto iterStart = std::lower_bound(fencePointersCopy.begin(), fencePointersCopy.end(), start);
    searchPageStart = std::distance(fencePointersCopy.begin(), iterStart);

    {   
        // SyncedCout() << "Run::range: Getting fileWrite lock" << std::this_thread::get_id() << std::endl;
        // // Ensure that the run file is not being written to while we are reading from it
        // std::shared_lock<std::shared_mutex> lock(fileMutex);
        // SyncedCout() << "Run::range: Got fileWrite lock" << std::this_thread::get_id() << std::endl;
    }

    // Start the timer for the query
    auto start_time = std::chrono::high_resolution_clock::now();

    size_t pageStart = searchPageStart * getpagesize();
    size_t pageEnd = (searchPageStart + 1 == fencePointersCopy.size()) ? runSize : (searchPageStart + 1) * getpagesize();

    {
        // SyncedCout() << "Run::range: Getting fileRead lock" << std::this_thread::get_id() << std::endl;
        // std::shared_lock<std::shared_mutex> lock(fileMutex);
        int localFd = openFile("Run::get: Failed to open file for Run", O_RDONLY);
        auto startPosPair = binarySearchInRange(localFd, pageStart, pageEnd, start);
        if (startPosPair.second != nullptr) {
            rangeMap[startPosPair.first] = *startPosPair.second;
        }

        for (size_t i = startPosPair.first + 1; i < runSize; i++) {
            kvPair kv;
            pread(localFd, &kv, sizeof(kvPair), i * sizeof(kvPair));

            if (kv.key >= start && kv.key < end) {
                rangeMap[kv.key] = kv.value;
            } else if (kv.key >= end) {
                break;
            }
        }

        closeFile();
        // SyncedCout() << "Run::range: Released fileRead lock" << std::this_thread::get_id() << std::endl;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    lsmTree->incrementLevelIoCountAndTime(levelOfRun, duration);

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

    {
        // std::shared_lock<std::shared_mutex> lock(fileMutex);
        // Open the file descriptor
        int localFd = openFile("Run::get: Failed to open file for Run", O_RDONLY);
        kvPair kv;
        while (read(localFd, &kv, sizeof(kvPair)) > 0) {
            map[kv.key] = kv.value;
        }
        closeFile();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
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
    j["runFilePath"] = runFilePath;
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
    runFilePath = j["runFilePath"];
    size = j["size"];
    maxKey = j["maxKey"];
    truePositives = j["truePositives"];
    falsePositives = j["falsePositives"];
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

// Example:
// Run 0: Bloom Filter Size: 25,126,656, Num Hash Functions: 16, FPR: 0.046875, TP: 183, FP: 9, Max Keys: 1,048,576, Number of Keys: 1,048,576
std::string Run::getBloomFilterSummary() {
    // If the bloom filter has not been used, don't print the false positive rate and just print "Unused"
    std::string bfStatus = getBfFalsePositiveRate() == BLOOM_FILTER_UNUSED ? "Unused" : std::to_string(getBfFalsePositiveRate());
    std::stringstream ss;
    ss << "Bloom Filter Size: " << addCommas(std::to_string(getBloomFilterNumBits())) << ", Num Hash Functions: " << bloomFilter.getNumHashes() << 
    ", FPR: " << bfStatus << ", TP: " << addCommas(std::to_string(truePositives)) << ", FP: " << addCommas(std::to_string(getFalsePositives()))
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
    {
        // std::shared_lock<std::shared_mutex> lock(fileMutex);
        int localFd = openFile("Run::populateBloomFilter: Failed to open file for Run", O_RDONLY);
        // Read all the key-value pairs from the Run file and add the keys to the bloom filter
        kvPair kv;
        while (read(localFd, &kv, sizeof(kvPair)) > 0) {
            bloomFilter.add(kv.key);
        }
        closeFile();
    }
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

size_t Run::getTruePositives() {
    std::shared_lock<std::shared_mutex> lock(truePositivesMutex);
    return truePositives;
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


