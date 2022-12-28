#include "hash_table.h"

unsigned long hash_function(keyType key, int size) {
    return key % size;
}

// Initialize the components of a hashtable.
// The size parameter is the expected number of elements to be inserted.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the parameter passed to the method is not null, if malloc fails, etc).
int allocate(hashtable** ht, int size) {
    if (*ht != NULL) {
        return -1;
    }
    *ht = (hashtable*) malloc (sizeof(hashtable));
    if (*ht == NULL) {
        return -1;
    }
    (*ht)->size = size;
    (*ht)->count = 0;
    (*ht)->node_size = NODE_SIZE;
    
    (*ht)->items = (hash_node**) calloc ((*ht)->size, sizeof(hash_node*));
    if ((*ht)->items == NULL) {
        return -1;
    }
    for (int i = 0; i < (*ht)->size; i++)
        (*ht)->items[i] = NULL;
    return 0;
}

hash_node* create_hash_node(keyType key, valType value, int node_size) {
    // Creates a pointer to a new hash table item
    hash_node* item = (hash_node*) malloc (sizeof(hash_node));
    if (item == NULL) {
        return NULL;
    }
    // Create a new kv_pair array of size NODE_SIZE
    item->kv_pairs = (kv_pair*) malloc (node_size * sizeof(kv_pair));  
    if (item->kv_pairs == NULL) {
        return NULL;
    }
    // Initialize the first kv_pair in the array
    item->kv_pairs[0].key = key;
    item->kv_pairs[0].value = value;
    item->next = NULL;
    item->count = 1;
    return item;
}

// This method inserts a key-value pair into the hash table. Each node of the linked list contains an array of key-value pairs. 
// It first checks to see there is room available in any of the nodes in the linked list. If there is, it inserts the key-value 
// pair into the array of the first available node. If the node is full, it creates a new node and inserts the key-value pair
// into the array of the new node.
// It returns an error code, 0 for success and -1 otherwise (e.g., if malloc is called and fails).
// Allow the existence of multiple key-value pairs with the same key
int put(hashtable* ht, keyType key, valType value) {
    if (ht == NULL) {
        return -1;
    }
    int index = hash_function(key, ht->size);
    hash_node* cur_item = ht->items[index];
    hash_node* prev_item = NULL;
    while (cur_item != NULL) {
        if (cur_item->count < NODE_SIZE) {
            cur_item->kv_pairs[cur_item->count].key = key;
            cur_item->kv_pairs[cur_item->count].value = value;
            cur_item->count++;
            return 0;
        }
        prev_item = cur_item;
        cur_item = cur_item->next;
    }
    hash_node* new_item = create_hash_node(key, value, ht->node_size);
    if (new_item == NULL) {
        return -1;
    }
    if (prev_item == NULL) {
        ht->items[index] = new_item;
    } else {
        prev_item->next = new_item;
    }
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
        for (int i = 0; i < cur_item->count; i++) {
            if (cur_item->kv_pairs[i].key == key) {
                if (count < num_values) {
                    values[count] = cur_item->kv_pairs[i].value;
                }
                count++;
            }
        }
        cur_item = cur_item->next;
    }
    *num_results = count;
    return 0;
}

// Free all the components of a hash_node
int free_hash_node(hash_node* item) {
    if (item == NULL) {
        return -1;
    }
    free(item->kv_pairs);
    free(item);
    return 0;
}

// This method erases all key-value pairs with a given key from the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int erase(hashtable* ht, keyType key) {
    if (ht == NULL) {
        return -1;
    }
    int index = hash_function(key, ht->size);
    hash_node* cur_item = ht->items[index];
    hash_node* prev_item = NULL;
    while (cur_item != NULL) {
        for (int i = 0; i < cur_item->count; i++) {
            if (cur_item->kv_pairs[i].key == key) {
                for (int j = i; j < cur_item->count - 1; j++) {
                    cur_item->kv_pairs[j].key = cur_item->kv_pairs[j + 1].key;
                    cur_item->kv_pairs[j].value = cur_item->kv_pairs[j + 1].value;
                }
                cur_item->count--;
                if (cur_item->count == 0) {
                    if (prev_item == NULL) {
                        ht->items[index] = cur_item->next;
                    } else {
                        prev_item->next = cur_item->next;
                    }
                    free_hash_node(cur_item);
                    cur_item = prev_item;
                }
                break;
            }
        }
        prev_item = cur_item;
        // Check for NULL to see if the last node was deleted
        if (cur_item != NULL) {
            cur_item = cur_item->next;
        }
    }
    return 0;
}

// This method frees all memory occupied by the hash table.
// It returns an error code, 0 for success and -1 otherwise.
int deallocate(hashtable* ht) {
    if (ht == NULL) {
        return -1;
    }
    for (int i = 0; i < ht->size; i++) {
        hash_node* cur_item = ht->items[i];
        hash_node* next_item = NULL;
        while (cur_item != NULL) {
            next_item = cur_item->next;
            free_hash_node(cur_item);
            cur_item = next_item;
        }
    }
    free(ht->items);
    free(ht);
    return 0;
}
