#pragma once
#include <shared_mutex>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/lock_algorithms.hpp>
#include <optional>
#include "memtable.hpp"
#include "level.hpp"
#include "run.hpp"
#include "threadpool.hpp"

class Run;

class LSMTree {
public:
    // Constructor
    LSMTree(float bfErrorRate, int buffer_num_pages, int fanout, Level::Policy levelPolicy, size_t numThreads, 
            float compactionPercentage, const std::string& dataDirectory, bool throughputPrinting, size_t throughputFrequency);

    // DSL commands
    void put(KEY_t, VAL_t);
    std::unique_ptr<VAL_t> get(KEY_t key);
    std::unique_ptr<std::vector<kvPair>> range(KEY_t start, KEY_t end);
    void del(KEY_t key);
    void load(const std::string& filename);
    std::string printStats(ssize_t numToPrintFromEachLevel);
    void benchmark(const std::string& filename, bool verbose, size_t verboseFrequency);

    // Public compaction functions
    bool isLastLevel(std::vector<std::shared_ptr<Level>>::iterator it);
    bool isLastLevel(unsigned int levelNum);

    // Printing output
    void printHitsMissesStats();
    std::string printInfo();
    std::string printLevelIoCount();
    std::string getBloomFilterSummary();

    // Getters
    size_t getBufferMaxKvPairs() { return buffer.getMaxKvPairs(); }
    int getFanout() const { return fanout; }
    Level::Policy getLevelPolicy() const { return levelPolicy; }
    size_t getNumThreads() { return threadPool.getNumThreads(); }
    float getBfFalsePositiveRate();
    float getBfErrorRate() const { return bfErrorRate; }
    size_t getIoCount();
    size_t getLevelIoCount(int levelNum);
    std::chrono::microseconds getLevelIoTime(int levelNum);
    float getCompactionPercentage() const { return compactionPercentage; }
    std::string getDataDirectory() const { return dataDirectory; }
    bool getThroughputPrinting() const { return throughputPrinting; }
    size_t getThroughputFrequency() const { return throughputFrequency; }

    // Incrementers
    void incrementBfFalsePositives();
    void incrementBfTruePositives();
    void incrementLevelIoCountAndTime(int levelNum, std::chrono::microseconds duration);

    // MONKEY Bloom filter optimization
    void monkeyOptimizeBloomFilters();

    // Serialization
    json serialize() const;
    void serializeLSMTreeToFile(const std::string& filename);
    void deserialize(const std::string& filename);

private:
    // LSM tree components
    double bfErrorRate;
    unsigned int fanout;
    Level::Policy levelPolicy;
    size_t bfFalsePositives = 0;
    size_t bfTruePositives = 0;
    Memtable buffer;
    ThreadPool threadPool;
    float compactionPercentage;
    std::string dataDirectory;
    std::vector<std::shared_ptr<Level>> levels;
    bool throughputPrinting;
    size_t throughputFrequency;

    // Timer and IO count
    std::vector<std::pair<size_t, std::chrono::microseconds>> levelIoCountAndTime;

    // Compaction planning
    std::map<int, std::pair<int, int>> compactionPlan;

    // Private compaction functions
    void removeTombstones(std::unique_ptr<std::vector<kvPair>> &rangeResult);
    void moveRuns(int currentLevelNum);
    void executeCompactionPlan();
    size_t getCompactionPlanSize();
    void clearCompactionPlan();

    // Counting number of logical pairs
    std::pair<std::map<KEY_t, VAL_t>, std::vector<Level*>> setNumLogicalPairs();
    mutable boost::upgrade_mutex numLogicalPairsMutex;
    ssize_t numLogicalPairs = NUM_LOGICAL_PAIRS_NOT_CACHED;

    // Tracking hits and misses for get and range
    size_t getMisses = 0;
    size_t getHits = 0;
    size_t rangeMisses = 0;
    size_t rangeHits = 0;

    // Private functions for getting and incrementing with mutex locks
    std::vector<Level*> getLocalLevelsCopy();
    void incrementGetMisses();
    void incrementGetHits();
    void incrementRangeMisses();
    void incrementRangeHits();
    size_t getGetHits() const;
    size_t getGetMisses() const;
    size_t getRangeHits() const;
    size_t getRangeMisses() const;

    // Private functions for MONKEY bloom filter optimization
    size_t getTotalBits() const;
    double TrySwitch(Run* run1, Run* run2, size_t delta, double R) const;
    double eval(size_t bits, size_t entries) const;
    double AutotuneFilters(size_t mFilters);

    // Mutexes used in getters and incrementers
    mutable std::shared_mutex getHitsMutex;
    mutable std::shared_mutex getMissesMutex;
    mutable std::shared_mutex rangeHitsMutex;
    mutable std::shared_mutex rangeMissesMutex;
    mutable std::shared_mutex bfFalsePositivesMutex;
    mutable std::shared_mutex bfTruePositivesMutex;
    mutable std::shared_mutex levelIoCountAndTimeMutex;

    // Mutexes used for buffer locking, compaction, and level locking
    mutable std::shared_mutex compactionPlanMutex;
    mutable std::shared_mutex bufferMutex;
    mutable std::shared_mutex moveRunsMutex;  // Blocks the moveRuns function to only a single thread
    mutable boost::upgrade_mutex levelsVectorMutex;

    // Throughput tracking
    mutable boost::upgrade_mutex throughputMutex;
    void calculateAndPrintThroughput();
    std::atomic<uint64_t> commandCounter{0};
    std::chrono::steady_clock::time_point startTime;
    std::chrono::steady_clock::time_point lastReportTime;
    uint64_t lastReportIoCount = 0;
    bool timerStarted = false;
};