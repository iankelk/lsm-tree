#ifndef LSM_TREE_HPP
#define LSM_TREE_HPP
#include <shared_mutex>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/lock_algorithms.hpp>
#include "memtable.hpp"
#include "level.hpp"
#include "run.hpp"
#include "threadpool.hpp"

class Run;

class LSMTree {
public:
    LSMTree(float bfErrorRate, int buffer_num_pages, int fanout, Level::Policy levelPolicy, size_t numThreads);
    void put(KEY_t, VAL_t);
    std::unique_ptr<VAL_t> get(KEY_t key);
    std::unique_ptr<std::map<KEY_t, VAL_t>> range(KEY_t start, KEY_t end);
    void del(KEY_t key);
    void benchmark(const std::string& filename, bool verbose, size_t verboseFrequency);
    void load(const std::string& filename);
    bool isLastLevel(std::vector<std::shared_ptr<Level>>::iterator it);
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
    void incrementBfFalsePositives();
    void incrementBfTruePositives();
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
    std::tuple<size_t, std::map<KEY_t, VAL_t>, std::vector<Level*>> countLogicalPairs();
    void removeTombstones(std::unique_ptr<std::map<KEY_t, VAL_t>> &rangeMap);
    std::vector<Level*> getLocalLevelsCopy();
    std::vector<std::shared_ptr<Level>> levels;
    std::vector<std::pair<size_t, std::chrono::microseconds>> levelIoCountAndTime;
    std::map<int, std::pair<int, int>> compactionPlan;

    void incrementGetMisses();
    void incrementGetHits();

    void incrementRangeMisses();
    void incrementRangeHits();

    size_t getGetHits() const;
    size_t getGetMisses() const;
    size_t getRangeHits() const;
    size_t getRangeMisses() const;

    void mergeLevels(int currentLevelNum);
    void moveRuns(int currentLevelNum);
    void executeCompactionPlan();
    size_t getCompactionPlanSize();
    void clearCompactionPlan();

    size_t bfFalsePositives = 0;
    size_t bfTruePositives = 0;
    size_t getTotalBits() const;
    double TrySwitch(Run* run1, Run* run2, size_t delta, double R) const;
    double eval(size_t bits, size_t entries) const;
    double AutotuneFilters(size_t mFilters);
    size_t getMisses = 0;
    size_t getHits = 0;
    size_t rangeMisses = 0;
    size_t rangeHits = 0;

    mutable boost::upgrade_mutex numLogicalPairsMutex;
    ssize_t numLogicalPairs = NUM_LOGICAL_PAIRS_NOT_CACHED;

    mutable std::shared_mutex getHitsMutex;
    mutable std::shared_mutex getMissesMutex;
    mutable std::shared_mutex rangeHitsMutex;
    mutable std::shared_mutex rangeMissesMutex;
    mutable std::shared_mutex bfFalsePositivesMutex;
    mutable std::shared_mutex bfTruePositivesMutex;
    mutable std::shared_mutex levelIoCountAndTimeMutex;

    // Tricky ones
    mutable std::shared_mutex compactionPlanMutex;
    mutable std::shared_mutex bufferMutex;
    mutable std::shared_mutex moveRunsMutex;
    mutable boost::upgrade_mutex levelsMutex;

};

#endif /* LSM_TREE_HPP */