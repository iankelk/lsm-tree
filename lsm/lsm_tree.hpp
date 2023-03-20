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
    void benchmark(const std::string& filename);
    void load(const std::string& filename);
    bool isLastLevel(std::vector<Level>::iterator it);
    std::string printStats();
    std::string printTree();
    json serialize() const;
    void serializeLSMTreeToFile(const std::string& filename);
    void deserialize(const std::string& filename);
    float getBfErrorRate() const { return bf_error_rate; }
    int getBufferNumPages() { return buffer.getMaxKvPairs(); }
    int getFanout() const { return fanout; }
    Level::Policy getLevelPolicy() const { return level_policy; }
    void incrementBfFalsePositives() { bfFalsePositives++; }
    void incrementBfTruePositives() { bfTruePositives++; }
    float getBfFalsePositiveRate() { return (float)bfFalsePositives / (bfFalsePositives + bfTruePositives); }
private:
    Memtable buffer;
    double bf_error_rate;
    int fanout;
    Level::Policy level_policy;
    int countLogicalPairs();
    void removeTombstones(std::unique_ptr<std::map<KEY_t, VAL_t>> &range_map);
    std::vector<Level> levels;
    void merge_levels(int currentLevelNum);
    int bfFalsePositives = 0;
    int bfTruePositives = 0;
};

#endif /* LSM_TREE_HPP */