#ifndef LSM_TREE_HPP
#define LSM_TREE_HPP
#include "memtable.hpp"
#include "level.hpp"
#include "run.hpp"

class Run;

class LSMTree {
public:
    LSMTree(float, int, int, Level::Policy);
    void put(KEY_t, VAL_t);
    std::unique_ptr<VAL_t> get(KEY_t key);
    std::unique_ptr<std::map<KEY_t, VAL_t>> range(KEY_t start, KEY_t end);
    void del(KEY_t key);
    void benchmark(const std::string& filename, bool verbose);
    void load(const std::string& filename);
    bool isLastLevel(std::vector<Level>::iterator it);
    std::string printStats();
    std::string printTree();
    json serialize() const;
    void serializeLSMTreeToFile(const std::string& filename);
    void deserialize(const std::string& filename);
    float getBfErrorRate() const { return bfErrorRate; }
    int getBufferNumPages() { return buffer.getMaxKvPairs(); }
    int getFanout() const { return fanout; }
    Level::Policy getLevelPolicy() const { return levelPolicy; }
    void incrementBfFalsePositives() { bfFalsePositives++; }
    void incrementBfTruePositives() { bfTruePositives++; }
    void incrementIoCount() { ioCount++; }
    long getIoCount() { return ioCount; }
    float getBfFalsePositiveRate();
    std::string getBloomFilterSummary();
    void monkeyOptimizeBloomFilters();
    int getTotalRuns() const;

private:
    Memtable buffer;
    double bfErrorRate;
    int fanout;
    Level::Policy levelPolicy;
    int countLogicalPairs();
    void removeTombstones(std::unique_ptr<std::map<KEY_t, VAL_t>> &rangeMap);
    std::vector<Level> levels;
    void mergeLevels(int currentLevelNum);
    long long bfFalsePositives = 0;
    long long bfTruePositives = 0;
    long long ioCount = 0;
    long long getTotalBits() const;
    double TrySwitch(Run& run1, Run& run2, long long delta, double R) const;
    double eval(long long bits, long long entries) const;
    double AutotuneFilters(long long mFilters) const;
};

#endif /* LSM_TREE_HPP */