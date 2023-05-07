# Harvard CS265 - Big Data Systems #
---
*This repository contains the code of a workload generator for an LSM tree.
It follows the DSL specified for the systems project of CS265.*

*More information can be found [here](http://daslab.seas.harvard.edu/classes/cs265/project.html).*

## LSM Tree Key Value Store ##
---

# LSMTree Database

## Getting Started

This document contains instructions for launching and using the server and client of the LSMTree Database. Please follow the steps below to get started. In order to install the required libraries and tools for the LSMTree, follow the instructions below.

## Prerequisites

Install g++, make, GSL, Boost Thread, and nlohmann/json.hpp on Linux and macOS.

### Linux

For Linux systems, you can use the `apt` package manager. Open a terminal and enter the following commands:

```sh
sudo apt update
sudo apt install g++ make libgsl-dev libboost-thread-dev
```

To install `nlohmann/json.hpp`, use the following commands:

```sh
sudo apt install nlohmann-json3-dev
```

### macOS

For macOS systems, you can use the `brew` package manager. If you haven't already installed Homebrew, you can follow the instructions [here](https://brew.sh/).

Once Homebrew is installed, open a terminal and enter the following commands:

```sh
brew update
brew install gcc make gsl boost
```

To install `nlohmann/json.hpp`, use the following commands:

```sh
brew install nlohmann-json
```

## Launching the Server

To launch the server, use the following command, with any desired options:

```
./server [OPTIONS]
```

### Server Launch Options

| Option | Default | Description |
|--------|---------|-------------|
| `-e <errorRate>` | DEFAULT_ERROR_RATE | Bloom filter error rate |
| `-n <numPages>` | DEFAULT_NUM_PAGES | Size of the buffer by number of disk pages |
| `-f <fanout>` | DEFAULT_FANOUT | LSM tree fanout |
| `-l <levelPolicy>` | DEFAULT_LEVELING_POLICY | Compaction policy |
| `-c <compactionPercentage>` | DEFAULT_COMPACTION_PERCENTAGE | Compaction % used for PARTIAL compaction only |
| `-p <port>` | DEFAULT_SERVER_PORT | Port number |
| `-t <numThreads>` | DEFAULT_NUM_THREADS | Number of threads for GET and RANGE queries |
| `-v <optional: frequency>` | DEFAULT_VERBOSE_FREQUENCY | Verbose benchmarking. Reports every "frequency" number of commands |
| `-s <optional: frequency>` | DEFAULT_THROUGHPUT_FREQUENCY | Throughput reporting. Reports every "frequency" number of commands |
| `-d <dataDirectory>` | DEFAULT_DATA_DIRECTORY | Data directory |
| `-h` | N/A | Print help message |

## Server Commands

Once the server has been launched, you can enter the following commands in the terminal:

| Command | Description |
|---------|-------------|
| `bloom` | Print Bloom Filter summary |
| `monkey` | Optimize Bloom Filters using MONKEY |
| `misses` | Print GET and RANGE hits and misses stats |
| `io` | Print level IO-specific counts and storage type time estimates |
| `quit` | Quit server |
| `qs` | Save server to disk and quit |
| `help` | Print help message |

## Launching the Client

To launch the client, use the following command, with any desired options:

```
./client [-p port] [-q <quiet mode>]
```

### Client Launch Options

| Option | Description |
|--------|-------------|
| `-p port` | Specify the port number |
| `-q <quiet mode>` | Quiet mode |

## Client Commands

Once the client has been launched, you can enter the following commands in the terminal:

| Command | Description |
|---------|-------------|
| `p [INT1] [INT2]` | Put (Insert/Update a key-value pair) |
| `g [INT1]` | Get (Retrieve the value associated with a key) |
| `r [INT1] [INT2]` | Range (Retrieve key-value pairs within a range of keys) |
| `d [INT1]` | Delete (Remove a key-value pair) |
| `l "/path/to/fileName"` | Load (Insert key-value pairs from a binary file) |
| `b "/path/to/fileName"` | Benchmark (Run commands from a text file quietly with no output) |
| `s [Optional INT1]` | Print Stats (Display information about the current state of the tree) |
| `i` | Summarized Tree Info |
| `q` | Shutdown server and save the database state to disk |

Refer to the inline help (`help` command) or documentation for detailed examples and explanations.

# LSMTree Domain Specific Language

The LSMTree provides a domain specific language (DSL) that supports six commands: put, get, range, delete, load, and print stats. Each command is explained in greater detail below.

## Put

The put command inserts a key-value pair into the LSM-Tree. Duplicate keys are not supported, so updating the value of an existing key is done using this command as well.

**Syntax:**

```
p [INT1] [INT2]
```

The 'p' indicates that this is a put command with key INT1 and value INT2.

**Example:**

```
p 10 7
p 63 222
p 10 5
```

First, the key `10` is added with value `7`. Next, key `63` is added with value `222`. The tree now holds two pairs. Finally, the key `10` is updated to have value `5`. Note that the tree logically still holds only two pairs. These instructions include only puts, so no output is expected.

## Get

The get command takes a single integer (the key) and returns the current value associated with that key.

**Syntax:**

```
g [INT1]
```

The 'g' indicates that this is a get for key INT1. The current value should be printed on a single line if the key exists and a blank line printed if the key is not in the tree.

**Example:**

```
p 10 7
p 63 222
g 10
g 15
p 15 5
g 15
```

**Output:**
```
7

5
```

## Range

The range command retrieves a sequence of keys within a range instead of a single specific key.

**Syntax:**

```
r [INT1] [INT2]
```

The 'r' indicates that this is a range request for all the keys from INT1 inclusive to INT2 exclusive. If the range is completely empty, then the output should be a blank line; otherwise, the output should be a space-delimited list of all the found pairs (in the format `key:value`); the order is irrelevant.

**Example:**

```
p 10 7
p 13 2
p 17 99
p 12 22
r 10 12
r 10 15
r 14 17
r 0 100
```

**Output:**
```
10:7
10:7 12:22 13:2

10:7 12:22 13:2 17:99
```

## Delete

The delete command removes a single key-value pair from the LSM-Tree.

**Syntax:**

```
d [INT1]
```

The 'd' indicates that this is a delete command for key INT1 and its associated value (whatever that may be). A delete of a non-existing key has no effect.

**Example:**

```
p 10 7
p 12 5
g 10
d 10
g 10
g 12
```

**Output:**
```
7

5
```

## Load

The load command inserts many values into the tree without the need to read and parse individual ASCII lines.

**Syntax:**

```
l "/path/to/file_name"
```

The 'l' command code is used for load. This command takes a single string argument - a relative or absolute path to a binary file of integers that should be loaded into the tree.

**Example:**

```
l "~/load_file.bin"
```

## Print Stats

The print stats command allows the caller to view some information about the current state of the tree. This is helpful for debugging as well as the final evaluation.

**Syntax:**

```
s [Optional INT1]`
```

The 's' command code is used for printing stats. This command has an optional integer argument to limit how many key-value pairs are printed per level, and prints out the stats for the tree. It's recommended to use the optional integer for large databases, as otherwise you may find yourself waiting for your computer to finish printing the entire dump of the database.

**Example:**

This command will print out at a minimum the data listed above. For a very small tree, one output might look like:

```
Logical Pairs: 12
LVL1: 3, LVL3: 11
45:56:L1 56:84:L1 91:45:L1

7:32:L3 19:73:L3 32:91:L3 45:64:L3 58:3:L3 61:10:L3 66:4:L3 85:15:L3 91:71:L3 95:87:L3 97:76:L3
```