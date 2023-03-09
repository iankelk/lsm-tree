#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <chrono>


#include "hash_table.h"

// This code is designed to stress test your hash table implementation. You do
// not need to significantly change it, but you may want to vary the value of
// num_tests to control the amount of time and memory that benchmarking takes
// up. Compile and run it in the command line by typing:
// make benchmark; ./benchmark

int main(int argc, char *argv[]) {
  
  int num_tests = 50000000;
  int size = num_tests, node_size = NODE_SIZE;
  float q_tuning = Q_TUNING; 

  // Parse command line arguments
  // -s: size of hash table
  // -q: q tuning parameter
  // -n: node size
  // For example, to run the benchmark with a hash table size of 1000000, a q
  // tuning parameter of 2, and a node size of 4, you would type:
  // ./benchmark -s 1000000 -q 2 -n 4
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-s") == 0) {
      size = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-q") == 0) {
      q_tuning = atof(argv[++i]);
    } else if (strcmp(argv[i], "-n") == 0) {
      node_size = atoi(argv[++i]);
    }
  }

  hashtable* ht=NULL;
  assert(allocate(&ht, size)==0);

  ht->node_size = node_size;
  ht->q_tuning = q_tuning;

  printf("Parameters: size: %d, node_size: %d, Q: %f\n", size, node_size, q_tuning);

  int seed = 2;
  srand(seed);
  printf("Performing stress test. Inserting 50 million keys.\n");

  struct timeval stop, start;
  gettimeofday(&start, NULL);

  for (int i = 0; i < num_tests; i += 1) {
    int key = rand();
    int val = rand();
    assert(put(ht, key, val)==0);
  }

  gettimeofday(&stop, NULL);
  double secs = (double)(stop.tv_usec - start.tv_usec) / 1000000 + (double)(stop.tv_sec - start.tv_sec); 
  printf("50 million insertions took <%f> seconds\n", secs);
  printf("Final size of hash array is %d\n", ht->size);

  assert(deallocate(ht)==0);

  return 0;
}
