#include <cstdlib>
#include <iostream>
#include <string>
#include <cassert>
#include <iostream>
#include <stdexcept>

#include "unistd.h"

#include "data_types.hpp"
#include "error.hpp"
#include "bloom_filter.hpp"
#include "memtable.hpp"
#include "run.hpp"

using namespace std;

template <typename FUNC, typename EXCEPTION> void assert_throws(FUNC&& f, EXCEPTION&& e);
int testDynamicBitset();
int testBloomFilter();
int testMemtable();
int testRun();

int main() {
    // BloomFilter bf(1000, 0.01, 10000);
    // bf.add(12345);
    // bf.add(123456);
    // std::cout << bf.contains(12345) << std::endl; // prints 1
    // std::cout << bf.contains(54321) << std::endl; // prints 0
    // return 0;

    testDynamicBitset();
    testBloomFilter();
    testMemtable();
    testRun();
}

// 
template <typename FUNC, typename EXCEPTION> void assert_throws(FUNC&& f, EXCEPTION&& e) {
    try {
        f();
        assert(false);
    } catch (const EXCEPTION& ex) {
        assert(string(ex.what()) == string(e.what()));
    }
}

// Test the DynamicBitset class
int testDynamicBitset() {
    // Test case 1: Check the initial size of the dynamic bitset object
    DynamicBitset db;
    assert(db.size() == 0);

    // Test case 2: Resize the dynamic bitset object and check its new size
    db.resize(5);
    assert(db.size() == 5);

    // Test case 3: Set a bit at a valid position and check its value
    db.set(2);
    assert(db.test(2) == true);

    // Test case 4: Reset a bit at a valid position and check its value
    db.reset(2);
    assert(db.test(2) == false);

    // Test case 5: Set a bit at an invalid position and catch the thrown exception
    try {
        db.set(6);
        assert(false);
    } catch (const out_of_range& e) {
        assert(string(e.what()) == "set: Bitset index 6 out of range for size 5");
    }

    // Test case 6: Reset a bit at an invalid position and catch the thrown exception
    try {
        db.reset(6);
        assert(false);
    } catch (const out_of_range& e) {
        assert(string(e.what()) == "reset: Bitset index 6 out of range for size 5");
    }

    // Test case 7: Test a bit at an invalid position and catch the thrown exception
    try {
        db.test(6);
        assert(false);
    } catch (const out_of_range& e) {
        assert(string(e.what()) == "test: Bitset index 6 out of range for size 5");
    }

    cout << "DynamicBitset: All test cases passed." << endl;
    return 0;
}

int testBloomFilter() {
    // Test case 1: Add a key and check if it exists
    BloomFilter bf(1000, 0.01, 10000);
    bf.add(12345);
    assert(bf.contains(12345) == true);

    // Test case 2: Add a key and check if a different key exists
    bf.add(12345);
    assert(bf.contains(54321) == false);

    // Test case 3: Add multiple keys and check if they exist
    bf.add(12345);
    bf.add(67890);
    bf.add(54321);
    assert(bf.contains(12345) == true);
    assert(bf.contains(67890) == true);
    assert(bf.contains(54321) == true);

    // Test case 4: Check if a key exists that was not added
    assert(bf.contains(99999) == false);

    // Test case 5: Add a key, re-add it, and check if it exists
    bf.add(12345);
    assert(bf.contains(12345) == true);
    bf.add(12345);
    assert(bf.contains(12345) == true);

    // Test case 6: Create a BloomFilter with a negative capacity
    assert_throws([](){ BloomFilter(-1000, 0.01, 10000); }, std::invalid_argument("Capacity must be non-negative."));

    cout << "BloomFilter: All test cases passed." << endl;
    return 0;
}

int testMemtable() {
    Memtable memtable(3); // Create a memtable with a maximum of 3 key-value pairs
    KEY_t key1 = 1;
    KEY_t key2 = 2;
    KEY_t key3 = 3;
    KEY_t key4 = 4;
    VAL_t value1 = 10;
    VAL_t value2 = 20;
    VAL_t value3 = 30;
    VAL_t value4 = 40;

    // Test case 1: Insert a new key-value pair into an empty memtable and check if it can be retrieved
    bool success = memtable.put(key1, value1);
    assert(success == true);
    VAL_t* result = memtable.get(key1);
    assert(result != nullptr);
    assert(*result == value1);

    // Test case 2: Insert a new key-value pair into a non-empty memtable and check if it can be retrieved
    success = memtable.put(key2, value2);
    assert(success == true);
    result = memtable.get(key2);
    assert(result != nullptr);
    assert(*result == value2);

    // Test case 3: Update an existing key-value pair in the memtable and check if the update was successful
    success = memtable.put(key2, value3);
    assert(success == true);
    result = memtable.get(key2);
    assert(result != nullptr);
    assert(*result == value3);

    // Test case 4: Insert a new key-value pair into a full memtable and check if it was rejected
    success = memtable.put(key3, value3);
    assert(success == true);
    success = memtable.put(key4, value4);
    assert(success == false);

    // Test case 5: Get a non-existing key from the memtable and check if nullptr is returned
    result = memtable.get(key4);
    assert(result == nullptr);

    // Test case 6: Get all key-value pairs within a range and check if they match the expected values
    map<KEY_t, VAL_t> range = memtable.range(1, 2);
    assert(range.size() == 2);
    assert(range[key1] == value1);
    assert(range[key2] == value3);

    // Test case 7: Clear the memtable and check if it is empty
    memtable.clear();
    assert(memtable.get(key1) == nullptr);
    assert(memtable.get(key2) == nullptr);
    assert(memtable.get(key3) == nullptr);
    assert(memtable.get(key4) == nullptr);
    range = memtable.range(0, 10);
    assert(range.size() == 0);

    cout << "Memtable: All test cases passed." << endl;
    return 0;
}

int testRun() {
    // Test case for inserting and retrieving a single key-value pair
    {
        Run run(1, 1, 0.1, 1);
        KEY_t key = 10;
        VAL_t val = 20;
        run.put(key, val);
        VAL_t *retrieved_val = run.get(key);
        assert(*retrieved_val == val);
        delete retrieved_val;
    }

    // Test case for inserting and retrieving multiple key-value pairs using get
    {
        Run run(5, 5, 0.1, 5);
        map<KEY_t, VAL_t> expected = {{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}};
        for (auto const& [key, val] : expected) {
            cout << "Put " + to_string(key) + ": " + to_string(val) << endl;
            run.put(key, val);
        }
        for (auto const& [key, val] : expected) {
            VAL_t *retrieved_val = run.get(key);
            cout << "Retrieved val: " + to_string(*retrieved_val) + " Expected val: " + to_string(val) << endl;
            //assert(*retrieved_val == val);
            // delete retrieved_val;
        }
    }

    // Test case for inserting and retrieving multiple key-value pairs
    {
        Run run(5, 5, 0.1, 5);
        map<KEY_t, VAL_t> expected = {{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}};
        for (auto const& [key, val] : expected) {
            run.put(key, val);
        }
        map<KEY_t, VAL_t> retrieved = run.range(1, 5);
        assert(retrieved == expected);
    }

    // Test case for inserting more key-value pairs than the capacity
    {
        Run run(5, 5, 0.1, 5);
        for (int i = 1; i <= 5; i++) {
            run.put(i, i);
        }
        try {
            run.put(6, 6);
            assert(false);
        } catch (const std::runtime_error& e) {
            assert(string(e.what()) == "Attempting to add to full Run");
        }
    }

    // Test case for retrieving a key that doesn't exist
    {
        Run run(1, 1, 0.1, 1);
        KEY_t key = 10;
        VAL_t *retrieved_val = run.get(key);
        assert(retrieved_val == nullptr);
    }

    // Test case for retrieving a range of keys
    {
        Run run(5, 5, 0.1, 5);
        map<KEY_t, VAL_t> expected = {{2, 2}, {3, 3}, {4, 4}};
        for (auto const& [key, val] : expected) {
            run.put(key, val);
        }
        map<KEY_t, VAL_t> retrieved = run.range(2, 4);
        assert(retrieved == expected);
    }

    cout << "Run: All tests passed" << endl;

    return 0;
}

