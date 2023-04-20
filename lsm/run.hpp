#ifndef LSM_RUN_HPP
#define LSM_RUN_HPP
#include <vector>
#include <map>
#include <string>
#include <shared_mutex>
#include <thread>
#include "bloom_filter.hpp"

class LSMTree;

class Run {
public:
    Run(size_t maxKvPairs, double bfErrorRate, bool createFile, size_t levelOfRun, LSMTree* lsmTree);
    ~Run();
    std::unique_ptr<VAL_t> get(KEY_t key);
    std::map<KEY_t, VAL_t> range(KEY_t start, KEY_t end);
    void put(KEY_t key, VAL_t val);
    std::map<KEY_t, VAL_t> getMap();
    size_t getMaxKvPairs();
    std::string getBloomFilterSummary();
    int openFile(std::string originatingFunctionError, int flags);
    void closeFile();
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
    mutable std::shared_mutex fileReadMutex;

private:
    static thread_local int localFd;
    std::pair<size_t, std::unique_ptr<VAL_t>> binarySearchInRange(int fd, size_t start, size_t end, KEY_t key);

    KEY_t maxKey;
    size_t maxKvPairs;
    double bfErrorRate;
    BloomFilter bloomFilter;
    std::vector<KEY_t> fencePointers;
    std::string runFilePath;
    size_t size;
    LSMTree* lsmTree;
    float getBfFalsePositiveRate();
    size_t falsePositives = 0;
    size_t truePositives = 0;
    size_t levelOfRun;
    mutable std::shared_mutex falsePositivesMutex;
    mutable std::shared_mutex truePositivesMutex;
    mutable std::shared_mutex sizeMutex;
    mutable std::shared_mutex maxKeyMutex;
    mutable std::shared_mutex fencePointersMutex;
    mutable std::shared_mutex bloomFilterMutex;
    void incrementSize();
    void setMaxKey(KEY_t key);
    KEY_t getMaxKey();
    void addFencePointer(KEY_t key);
    std::vector<KEY_t> getFencePointers();
    void addToBloomFilter(KEY_t key);

    void incrementFalsePositives();
    size_t getFalsePositives();
    void incrementTruePositives();
    size_t getTruePositives();

};

#endif /* LSM_RUN_HPP */
