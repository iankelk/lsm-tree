SRCS = lsm/bloom_filter.cpp lsm/utils.cpp lsm/memtable.cpp lsm/run.cpp lsm/level.cpp lsm/lsm_tree.cpp lsm/storage.cpp lsm/threadpool.cpp lib/xxhash.cpp

# Ensure bin directory exists
$(shell mkdir -p bin)

# Link the Boost library
OS_NAME := $(shell uname)

ifeq ($(OS_NAME), Darwin)
  LDLIBS = -lpthread -lboost_thread-mt
else
  LDLIBS = -lpthread -lboost_thread
endif

CXXFLAGS = -std=c++20 -I./lib -I/usr/local/include -I/opt/homebrew/include -I/opt/homebrew/Cellar/boost/1.81.0_1/include
LDFLAGS = -L/usr/local/lib -L/opt/homebrew/lib -L/opt/homebrew/Cellar/boost/1.81.0_1/lib $(LDLIBS)

all: server client

fast: fast_server fast_client

build:
	g++ lsm/main.cpp $(SRCS) -o bin/lsm $(CXXFLAGS) $(LDFLAGS)

server:
	g++ -ggdb3 -g -O0 lsm/server.cpp $(SRCS) -o bin/server $(CXXFLAGS) $(LDFLAGS) -fno-omit-frame-pointer -fsanitize=thread

client:
	g++ -ggdb3 -g -O0 lsm/client.cpp $(SRCS) -o bin/client $(CXXFLAGS) $(LDFLAGS) -fno-omit-frame-pointer -fsanitize=thread

fast_server:
	g++ -O3 lsm/server.cpp $(SRCS) -o bin/server $(CXXFLAGS) $(LDFLAGS)

fast_client:
	g++ -O3 lsm/client.cpp $(SRCS) -o bin/client $(CXXFLAGS) $(LDFLAGS)

test:
	g++ -ggdb3 -g -O0 lsm/test.cpp $(SRCS) -o bin/test $(CXXFLAGS) $(LDFLAGS) -fsanitize=address -fno-omit-frame-pointer

.PHONY: genm1
genm1:
	$(CC) generator/generator.c -I/opt/homebrew/include -L/opt/homebrew/lib -o bin/generator -lgsl -lgslcblas -lm

.PHONY: generator
generator:
	$(CC) generator/generator.c -o bin/generator -lgsl -lgslcblas

.PHONY: clean
	rm bin/generator bin/lsm bin/test


# server:
# 	g++ -ggdb3 -g -O0 lsm/server.cpp $(SRCS) -o bin/server $(CXXFLAGS) $(LDFLAGS) -fsanitize=address -fno-omit-frame-pointer -fsanitize=thread

# client:
# 	g++ -ggdb3 -g -O0 lsm/client.cpp $(SRCS) -o bin/client $(CXXFLAGS) $(LDFLAGS) -fsanitize=address -fno-omit-frame-pointer -Wall -Wextra

# server:
# 	g++ -ggdb3 -g -O0 lsm/server.cpp $(SRCS) -o bin/server -std=c++17 -I./lib -I/usr/local/include -L/usr/local/lib

# client:
# 	g++ -ggdb3 -g -O0 lsm/client.cpp $(SRCS) -o bin/client -std=c++17 -I./lib -I/usr/local/include -L/usr/local/lib