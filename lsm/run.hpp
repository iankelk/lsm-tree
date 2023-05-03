#pragma once
#include <vector>
#include <map>
#include <string>
#include <shared_mutex>
#include <thread>
#include "memtable.hpp"
#include "bloom_filter.hpp"

class LSMTree;

class Run {
public:
    Run(size_t maxKvPairs, double bfErrorRate, bool createFile, size_t levelOfRun, LSMTree* lsmTree);
    ~Run();
    std::unique_ptr<VAL_t> get(KEY_t key);
    std::vector<kvPair> range(KEY_t start, KEY_t end);
    void put2(KEY_t key, VAL_t val);
    void flush(std::unique_ptr<std::vector<kvPair>> kvPairs);
    std::vector<kvPair> getVector();
    size_t getMaxKvPairs();
    std::map<std::string, std::string> getBloomFilterSummary();
    void openOutputFileStream(std::ofstream& ofs, const std::string& originatingFunctionError);
    void openInputFileStream(std::ifstream& ifs, const std::string& originatingFunctionError);
    void closeOutputFileStream(std::ofstream& ofs);
    void closeInputFileStream(std::ifstream& ifs);

    json serialize() const;
    void deserialize(const json& j);
    void deleteFile();
    void setLSMTree(LSMTree* lsmTree);
    size_t getBloomFilterNumBits() { return bloomFilter.getNumBits(); }
    void setBloomFilterNumBits(size_t numBits) { bloomFilter.setNumBits(numBits); }
    size_t getSize() { return size; }
    void resizeBloomFilterBitset(size_t numBits);
    void populateBloomFilter();
    std::string getRunFilePath() { return runFilePath; }
    void setFirstAndLastKeys(KEY_t first, KEY_t last);
    KEY_t getFirstKey() { return firstKey; }
    KEY_t getLastKey() { return lastKey; }

private:
    std::pair<size_t, std::unique_ptr<kvPair>> binarySearchInRange(std::ifstream &ifs, size_t start, size_t end, KEY_t key);
    size_t maxKvPairs;
    double bfErrorRate;
    std::vector<KEY_t> fencePointers;
    float getBfFalsePositiveRate();
    size_t falsePositives = 0;
    size_t truePositives = 0;
    size_t levelOfRun;
    LSMTree* lsmTree;
    BloomFilter bloomFilter;
    std::string runFilePath;
    size_t size;
    KEY_t maxKey;
    mutable std::shared_mutex falsePositivesMutex;
    mutable std::shared_mutex truePositivesMutex;
    mutable std::shared_mutex sizeMutex;
    mutable std::shared_mutex maxKeyMutex;
    mutable std::shared_mutex fencePointersMutex;
    mutable std::shared_mutex bloomFilterMutex;
    void setSize(size_t newSize);
    void setMaxKey(KEY_t key);
    KEY_t getMaxKey();
    void addFencePointer(KEY_t key);
    std::vector<KEY_t> getFencePointers();
    void addToBloomFilter(KEY_t key);

    void incrementFalsePositives();
    size_t getFalsePositives();
    void incrementTruePositives();
    KEY_t firstKey;
    KEY_t lastKey;

};
