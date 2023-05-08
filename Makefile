SRCS = lsm/bloom_filter.cpp lsm/utils.cpp lsm/memtable.cpp lsm/run.cpp lsm/level.cpp lsm/lsm_tree.cpp lsm/storage.cpp lsm/threadpool.cpp lib/xxhash.cpp

# Ensure bin directory exists
$(shell mkdir -p bin)

# Link the Boost library
OS_NAME := $(shell uname)
ARCH := $(shell uname -m)

ifeq ($(OS_NAME), Darwin)
  LDLIBS = -lpthread -lboost_thread-mt
else
  LDLIBS = -lpthread -lboost_thread
endif

# Set library paths based on architecture
ifeq ($(ARCH), arm64)
  INCLUDE_PATH = -I/opt/homebrew/include -I/opt/homebrew/Cellar/boost/1.81.0_1/include
  LIBRARY_PATH = -L/opt/homebrew/lib -L/opt/homebrew/Cellar/boost/1.81.0_1/lib
else
  INCLUDE_PATH = -I/usr/local/include
  LIBRARY_PATH = -L/usr/local/lib
endif

CXXFLAGS = -std=c++20 -I./lib $(INCLUDE_PATH)
LDFLAGS = $(LIBRARY_PATH) $(LDLIBS)

all: server client generator

fast: fast_server fast_client

server:
	g++ -ggdb3 -g -O0 lsm/server.cpp $(SRCS) -o bin/server $(CXXFLAGS) $(LDFLAGS) -fno-omit-frame-pointer -Wall -Wextra -lprofiler

client:
	g++ -ggdb3 -g -O0 lsm/client.cpp $(SRCS) -o bin/client $(CXXFLAGS) $(LDFLAGS) -fno-omit-frame-pointer -Wall -Wextra -lprofiler

fast_server:
	g++ -O3 lsm/server.cpp $(SRCS) -o bin/server $(CXXFLAGS) $(LDFLAGS)

fast_client:
	g++ -O3 lsm/client.cpp $(SRCS) -o bin/client $(CXXFLAGS) $(LDFLAGS)

.PHONY: generator
generator:
ifeq ($(ARCH), arm64)
	$(CC) generator/generator.c -I/opt/homebrew/include -L/opt/homebrew/lib -o bin/generator -lgsl -lgslcblas -lm
else
	$(CC) generator/generator.c -o bin/generator -lgsl -lgslcblas
endif

.PHONY: clean
clean:
	rm bin/generator bin/client bin/server


# server:
# 	g++ -ggdb3 -g -O0 lsm/server.cpp $(SRCS) -o bin/server $(CXXFLAGS) $(LDFLAGS) -fsanitize=address -fno-omit-frame-pointer -fsanitize=thread

# client:
# 	g++ -ggdb3 -g -O0 lsm/client.cpp $(SRCS) -o bin/client $(CXXFLAGS) $(LDFLAGS) -fsanitize=address -fno-omit-frame-pointer -Wall -Wextra

# server:
# 	g++ -ggdb3 -g -O0 lsm/server.cpp $(SRCS) -o bin/server -std=c++17 -I./lib -I/usr/local/include -L/usr/local/lib

# client:
# 	g++ -ggdb3 -g -O0 lsm/client.cpp $(SRCS) -o bin/client -std=c++17 -I./lib -I/usr/local/include -L/usr/local/lib