#ifndef LSM_TREE_HPP
#define LSM_TREE_HPP
#include "memtable.hpp"
#include "level.hpp"

class LSMTree {
public:
    LSMTree(int, float, int, int, int, Level::Policy);
    void put(KEY_t, VAL_t);
    std::unique_ptr<VAL_t> get(KEY_t key);
    std::unique_ptr<std::map<KEY_t, VAL_t>> range(KEY_t start, KEY_t end);
    void del(KEY_t key);
    void load(const std::string& filename);
    std::string printStats();
    std::string printTree();
private:
    Memtable buffer;
    int bf_capacity;
    double bf_error_rate;
    int bf_bitset_size;
    int fanout;
    Level::Policy level_policy;
    int countLogicalPairs();
    void removeTombstones(std::unique_ptr<std::map<KEY_t, VAL_t>> &range_map);
    std::vector<Level> levels;
    void merge_levels(int currentLevelNum);
};

#endif /* LSM_TREE_HPP */