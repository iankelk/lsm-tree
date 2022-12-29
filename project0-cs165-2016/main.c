#include <stdlib.h>
#include <stdio.h>

#include "hash_table.h"

// This is where you can implement your own tests for the hash table
// implementation. 
int main(void) {

  hashtable *ht = NULL;
  int size = CAPACITY;
  allocate(&ht, size);


  int key = 0;
  int value = -1;

  printf("Initial size of hash array is %d\n", ht->size);

  put(ht, key, value);
  resize(&ht);

  printf("Final size of hash array is %d\n", ht->size);

  int num_values = 1;

  valType* values = malloc(1 * sizeof(valType));

  int* num_results = (int *) malloc(sizeof(int)); 

  printf("made it here 1 \n");
  get(ht, key, values, num_values, num_results);
  printf("made it here 2\n");
  if ((*num_results) > num_values) {
    values = realloc(values, (*num_results) * sizeof(valType));
    get(ht, 0, values, num_values, num_results);
  }
  printf("made it here 3\n");
  for (int i = 0; i < (*num_results); i++) {
    printf("value of %d is %d \n", i, values[i]);
  }
  free(values);
  free(num_results);


  erase(ht, 0);

  deallocate(ht);
  return 0;
}
