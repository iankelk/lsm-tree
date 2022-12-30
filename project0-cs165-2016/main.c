#include <stdlib.h>
#include <stdio.h>

#include "hash_table.h"

// This is where you can implement your own tests for the hash table
// implementation. 
int main( int argc, char *argv[] ) {

  int size = CAPACITY, node_size = NODE_SIZE, q_tuning = Q_TUNING;

  // Parse command line arguments
  // -s: size of hash table
  // -q: q tuning parameter
  // -n: node size
  // For example, to run the benchmark with a hash table size of 1000000, a q
  // tuning parameter of 2, and a node size of 4, you would type:
  // ./benchmark -s 1000000 -q 2 -n 4
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-s") == 0) {
      size = atoi(argv[i+1]);
    } else if (strcmp(argv[i], "-q") == 0) {
      q_tuning = atoi(argv[i+1]);
    } else if (strcmp(argv[i], "-n") == 0) {
      node_size = atoi(argv[i+1]);
    }
  }

  hashtable *ht = NULL;
  allocate(&ht, size);

  ht->node_size = node_size;
  ht->q_tuning = q_tuning;

  printf("Parameters: size: %d, node_size: %d, Q: %d\n", size, node_size, q_tuning);

  int key = 0;
  int value = -1;

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
