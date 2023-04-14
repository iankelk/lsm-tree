#ifndef LSM_RUN_HPP
#define LSM_RUN_HPP
#include <vector>
#include <map>
#include <string>
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
    void openFile(std::string originatingFunctionError);
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
private:
    std::unique_ptr<VAL_t> binarySearchInRange(int fd, size_t start, size_t end, KEY_t key);
    KEY_t maxKey;
    size_t maxKvPairs;
    double bfErrorRate;
    BloomFilter bloomFilter;
    std::vector<KEY_t> fencePointers;
    std::string tmpFile;
    size_t size;
    int fd;
    LSMTree* lsmTree;
    float getBfFalsePositiveRate();
    size_t falsePositives = 0;
    size_t truePositives = 0;
    size_t levelOfRun;
};

#endif /* LSM_RUN_HPP */
