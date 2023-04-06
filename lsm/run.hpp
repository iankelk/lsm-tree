#ifndef LSM_RUN_HPP
#define LSM_RUN_HPP
#include <vector>
#include <map>
#include <string>
#include "bloom_filter.hpp"

class LSMTree;

class Run {
public:
    Run(long maxKvPairs, double bfErrorRate, bool createFile, LSMTree* lsmTree);
    ~Run();
    std::unique_ptr<VAL_t> get(KEY_t key);
    std::map<KEY_t, VAL_t> range(KEY_t start, KEY_t end);
    void put(KEY_t key, VAL_t val);
    std::map<KEY_t, VAL_t> getMap();
    long getMaxKvPairs();
    std::string getBloomFilterSummary();
    void closeFile();
    json serialize() const;
    void deserialize(const json& j);
    void deleteFile();
    void setLSMTree(LSMTree* lsmTree);
    size_t getBloomFilterNumBits() { return bloomFilter.getNumBits(); }
    void setBloomFilterNumBits(size_t numBits) { bloomFilter.setNumBits(numBits); }
    size_t getSize() { return size; }
    void resetBloomFilterBitset();
    void resizeBloomFilterBitset(size_t numBits);
    void addKeyToBloomFilter(KEY_t key);
    void populateBloomFilter();
private:
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
    long long falsePositives = 0;
    long long truePositives = 0;
};

#endif /* LSM_RUN_HPP */
