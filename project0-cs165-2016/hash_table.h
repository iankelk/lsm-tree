#ifndef CS165_HASH_TABLE // This is a header guard. It prevents the header from being included more than once.
#define CS165_HASH_TABLE  

#include <stdio.h>
#include <stdlib.h>

typedef int keyType;
typedef int valType;

// Define the Hash Table Item here
typedef struct hash_node {
    int key;
    int value;
    struct hash_node* next;
} hash_node;

typedef struct hashtable {
// define the components of the hash table here (e.g. the array, bookkeeping for number of elements, etc)
    hash_node** items;
    int size;
    int count;
} hashtable;

int allocate(hashtable** ht, int size);
int put(hashtable* ht, keyType key, valType value);
int get(hashtable* ht, keyType key, valType *values, int num_values, int* num_results);
int erase(hashtable* ht, keyType key);
int deallocate(hashtable* ht);

#endif
