#include "hash_table.h"

unsigned long hash_function(keyType key, int size) {
    return key % size;
}

hash_node* create_hash_node(keyType key, valType value) {
    // Creates a pointer to a new hash table item
    hash_node* item = (hash_node*) malloc (sizeof(hash_node));
    item->key = key;
    item->value = value;
    item->next = NULL;
    return item;
}

// Initialize the components of a hashtable.
// The size parameter is the expected number of elements to be inserted.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the parameter passed to the method is not null, if malloc fails, etc).
int allocate(hashtable** ht, int size) {
    // The next line tells the compiler that we know we haven't used the variable
    // yet so don't issue a warning. You should remove this line once you use
    // the parameter.
    // (void) ht;
    // (void) size;
    // return 0;
    if (*ht != NULL) {
        return -1;
    }
    *ht = (hashtable*) malloc (sizeof(hashtable));
    (*ht)->size = size;
    (*ht)->count = 0;
    (*ht)->items = (hash_node**) calloc ((*ht)->size, sizeof(hash_node*));
    for (int i = 0; i < (*ht)->size; i++)
        (*ht)->items[i] = NULL;
    return 0;
}

// This method inserts a key-value pair into the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if malloc is called and fails).
// Allow the existence of multiple key-value pairs with the same key
int put(hashtable* ht, keyType key, valType value) {
    // (void) ht;
    // (void) key;
    // (void) value;
    // return 0;
    if (ht == NULL) {
        return -1;
    }
    hash_node* item = create_hash_node(key, value);
    int index = hash_function(key, ht->size);
    hash_node* cur_item = ht->items[index];
    hash_node* prev_item = NULL;
    
    while (cur_item != NULL) {
        prev_item = cur_item;
        cur_item = cur_item->next;
    }
    if (prev_item == NULL) {
        ht->items[index] = item;
    } else {
        prev_item->next = item;
    }
    ht->count++;
    return 0;
}

// This method retrieves entries with a matching key and stores the corresponding values in the
// values array. The size of the values array is given by the parameter
// num_values. If there are more matching entries than num_values, they are not
// stored in the values array to avoid a buffer overflow. The function returns
// the number of matching entries using the num_results pointer. If the value of num_results is greater than
// num_values, the caller can invoke this function again (with a larger buffer)
// to get values that it missed during the first call. 
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int get(hashtable* ht, keyType key, valType *values, int num_values, int* num_results) {
    if (ht == NULL) {
        return -1;
    }
    int index = hash_function(key, ht->size);
    hash_node* cur_item = ht->items[index];
    int count = 0;
    while (cur_item != NULL) {
        if (cur_item->key == key) {
            if (count < num_values) {
                values[count] = cur_item->value;
            }
            count++;
        }
        cur_item = cur_item->next;
    }
    *num_results = count;
    return 0;

}

// This method erases all key-value pairs with a given key from the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int erase(hashtable* ht, keyType key) {
    (void) ht;
    (void) key;
    return 0;
}

int free_hash_node(hash_node* item) {
    free(item);
    return 0;
}

// This method frees all memory occupied by the hash table.
// It returns an error code, 0 for success and -1 otherwise.
int deallocate(hashtable* ht) {
    // This line tells the compiler that we know we haven't used the variable
    // yet so don't issue a warning. You should remove this line once you use
    // the parameter.
    for (int i = 0; i < ht->size; i++) {
        if (ht->items[i] != NULL) {
            free_hash_node(ht->items[i]);
        }
    }
    free(ht->items);
    free(ht);    
    return 0;
}
