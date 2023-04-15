#ifndef LSM_TREE_HPP
#define LSM_TREE_HPP
#include <shared_mutex>
#include "memtable.hpp"
#include "level.hpp"
#include "run.hpp"
#include "threadpool.hpp"

class Run;

class LSMTree {
public:
    LSMTree(float, int, int, Level::Policy, size_t);
    void put(KEY_t, VAL_t);
    std::unique_ptr<VAL_t> get(KEY_t key);
    std::unique_ptr<std::map<KEY_t, VAL_t>> range(KEY_t start, KEY_t end);
    std::unique_ptr<std::map<KEY_t, VAL_t>> cRange(KEY_t start, KEY_t end);
    void del(KEY_t key);
    void benchmark(const std::string& filename, bool verbose, bool concurrent);
    void load(const std::string& filename);
    bool isLastLevel(std::vector<Level>::iterator it);
    bool isLastLevel(int levelNum);
    std::string printStats(size_t numToPrintFromEachLevel);
    std::string printTree();
    std::string printLevelIoCount();
    json serialize() const;
    void serializeLSMTreeToFile(const std::string& filename);
    void deserialize(const std::string& filename);
    float getBfErrorRate() const { return bfErrorRate; }
    size_t getBufferNumPages() { return buffer.getMaxKvPairs(); }
    int getFanout() const { return fanout; }
    Level::Policy getLevelPolicy() const { return levelPolicy; }
    void incrementBfFalsePositives() { bfFalsePositives++; }
    void incrementBfTruePositives() { bfTruePositives++; }
    void incrementIoCount();
    float getBfFalsePositiveRate();
    std::string getBloomFilterSummary();
    void monkeyOptimizeBloomFilters();
    void printHitsMissesStats();
    size_t getNumThreads() { return threadPool.getNumThreads(); }
    void incrementLevelIoCountAndTime(int levelNum, std::chrono::microseconds duration);
    size_t getIoCount();
    size_t getLevelIoCount(int levelNum);
    std::chrono::microseconds getLevelIoTime(int levelNum);

private:
    Memtable buffer;
    ThreadPool threadPool;
    double bfErrorRate;
    int fanout;
    Level::Policy levelPolicy;
    int countLogicalPairs();
    void removeTombstones(std::unique_ptr<std::map<KEY_t, VAL_t>> &rangeMap);
    std::vector<Level> levels;
    std::vector<std::pair<size_t, std::chrono::microseconds>> levelIoCountAndTime;
    std::map<int, std::pair<int, int>> compactionPlan;

    void mergeLevels(int currentLevelNum);
    void moveRuns(int currentLevelNum);
    void executeCompactionPlan();

    size_t bfFalsePositives = 0;
    size_t bfTruePositives = 0;
    size_t ioCount = 0;
    size_t getTotalBits() const;
    double TrySwitch(Run* run1, Run* run2, size_t delta, double R) const;
    double eval(size_t bits, size_t entries) const;
    double AutotuneFilters(size_t mFilters);
    size_t getMisses = 0;
    size_t getHits = 0;
    size_t rangeMisses = 0;
    size_t rangeHits = 0;

    std::shared_mutex ioCountMutex;
};

#endif /* LSM_TREE_HPP */