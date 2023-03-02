SRCS = lsm/dynamic_bitset.cpp lsm/bloom_filter.cpp lsm/error.cpp lsm/memtable.cpp lsm/run.cpp lsm/level.cpp

# Ensure bin directory exists
$(shell mkdir -p bin)

all: build

build:
	g++ lsm/main.cpp $(SRCS) -o bin/lsm -std=c++17 -I./lib -I/usr/local/include -L/usr/local/lib

test:
	g++ -ggdb3 lsm/test.cpp $(SRCS) -o bin/test -std=c++17 -I./lib -I/usr/local/include -L/usr/local/lib

.PHONY: generator
generator:
	$(CC) generator/generator.c -o bin/generator -lgsl -lgslcblas

clean:
	rm bin/generator bin/lsm bin/test
