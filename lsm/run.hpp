#ifndef LSM_RUN_H
#define LSM_RUN_H

#include <vector>
#include <map>
#include "data_types.hpp"
#include "bloom_filter.hpp"

using namespace std;

class Run {
public:
    Run(long max_kv_pairs, int capacity, double error_rate, int bitset_size);
    ~Run();
    VAL_t * get(KEY_t key);
    map<KEY_t, VAL_t> range(KEY_t start, KEY_t end);
    void put(KEY_t key, VAL_t val);
    map<KEY_t, VAL_t> getMap();
    long getMaxKvPairs();
    int getCapacity();
    double getErrorRate();
    int getBitsetSize();
//private:
    KEY_t max_key;
    long max_kv_pairs;
    int capacity;
    double error_rate;
    int bitset_size;
    BloomFilter bloom_filter;
    vector<KEY_t> fence_pointers;
    string tmp_file;
    long size;
    int fd;
    void closeFile();
};

#endif
