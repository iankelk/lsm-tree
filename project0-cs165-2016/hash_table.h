#ifndef CS165_HASH_TABLE // This is a header guard. It prevents the header from being included more than once.
#define CS165_HASH_TABLE  
#define NODE_SIZE 4
#define Q_TUNING 1
#define CAPACITY 49999

#include <stdio.h>
#include <stdlib.h>

typedef int keyType;
typedef int valType;

// Define the key value pair here
typedef struct kv_pair {
    keyType key;
    valType value;
} kv_pair;

// Define the Hash Node item here. Count is the number of key-value pairs in the node, 
// kv_pairs is an array of key-value pairs, and next is a pointer to the next node in the linked list 
typedef struct hash_node {
    int count;
    kv_pair* kv_pairs;
    struct hash_node* next;
} hash_node;

typedef struct hashtable {
// define the components of the hash table here (e.g. the array, bookkeeping for number of elements, etc)
    hash_node** items;
    int size; // Size of the hash table
    int count; // Number of elements in the hash table
    int node_size; // Size of the kv_pair array in each hash_node
    int q_tuning; // Tuning parameter for resizing
} hashtable;

int allocate(hashtable** ht, int size);
int put(hashtable* ht, keyType key, valType value);
int insert(hashtable* ht, keyType key, valType value);
int get(hashtable* ht, keyType key, valType *values, int num_values, int* num_results);
int erase(hashtable* ht, keyType key);
int deallocate(hashtable* ht);
int resize(hashtable** ht);


#endif
