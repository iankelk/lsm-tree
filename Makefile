# Ensure bin directory exists
$(shell mkdir -p bin)

all: build 

build:
	g++ lsm/*.cpp -o bin/lsm -std=c++11 -I./lib -I/usr/local/include -L/usr/local/lib

.PHONY: generator
generator:
	$(CC) generator/generator.c -o bin/generator -lgsl -lgslcblas

clean:
	rm bin/generator
