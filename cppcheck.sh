#!/bin/bash

# Run cppcheck on the "lsm" directory
cppcheck --enable=all --std=c++17 --suppress=missingIncludeSystem lsm/