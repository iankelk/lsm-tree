#ifndef LSM_RUN_HPP
#define LSM_RUN_HPP
#include <vector>
#include <map>
#include <string>
#include "bloom_filter.hpp"
#include "lsm_tree.hpp"

class Run {
public:
    Run(long max_kv_pairs, double bf_error_rate, int bf_bitset_size, LSMTree& lsm_tree);
    ~Run();
    std::unique_ptr<VAL_t> get(KEY_t key);
    std::map<KEY_t, VAL_t> range(KEY_t start, KEY_t end);
    void put(KEY_t key, VAL_t val);
    std::map<KEY_t, VAL_t> getMap();
    long getMaxKvPairs();
    void closeFile();
    json serialize() const;
private:
    LSMTree& lsm_tree;
    KEY_t max_key;
    long max_kv_pairs;
    double bf_error_rate;
    int bf_bitset_size;
    BloomFilter bloom_filter;
    std::vector<KEY_t> fence_pointers;
    std::string tmp_file;
    long size;
    int fd;
};

#endif /* LSM_RUN_HPP */
