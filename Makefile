SRCS = lsm/dynamic_bitset.cpp lsm/bloom_filter.cpp lsm/utils.cpp lsm/memtable.cpp lsm/run.cpp lsm/level.cpp lsm/lsm_tree.cpp

# Ensure bin directory exists
$(shell mkdir -p bin)

all: server client

fast: fast_server fast_client

build:
	g++ lsm/main.cpp $(SRCS) -o bin/lsm -std=c++17 -I./lib -I/usr/local/include -L/usr/local/lib

server:
	g++ -ggdb3 -g -O0 lsm/server.cpp $(SRCS) -o bin/server -std=c++17 -I./lib -I/usr/local/include -L/usr/local/lib

client:
	g++ -ggdb3 -g -O0 lsm/client.cpp $(SRCS) -o bin/client -std=c++17 -I./lib -I/usr/local/include -L/usr/local/lib	

fast_server:
	g++ -O3 lsm/server.cpp $(SRCS) -o bin/server -std=c++17 -I./lib -I/usr/local/include -L/usr/local/lib

fast_client:
	g++ -O3 lsm/client.cpp $(SRCS) -o bin/client -std=c++17 -I./lib -I/usr/local/include -L/usr/local/lib	

test:
	g++ -ggdb3 -g -O0 lsm/test.cpp $(SRCS) -o bin/test -std=c++17 -I./lib -I/usr/local/include -L/usr/local/lib

genm1:
	$(CC) generator/generator.c -I/opt/homebrew/include -L/opt/homebrew/lib -o bin/generator -lgsl -lgslcblas -lm

generator:
	$(CC) generator/generator.c -o bin/generator -lgsl -lgslcblas

clean:
	rm bin/generator bin/lsm bin/test
