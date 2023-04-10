#include <signal.h>
#include <sstream>
#include <iostream>
#include <unistd.h>
#include <shared_mutex>
#include <sys/select.h>
#include "server.hpp"

volatile sig_atomic_t terminationFlag = 0;
bool concurrent = false;

Server *server_ptr = nullptr;

void sigintHandler(int signal) {
    terminationFlag = 1;
    if (server_ptr) {
        server_ptr->close();
    }
}

void Server::listenToStdIn() {
    std::unique_lock<std::shared_mutex> exclusiveLock;
    std::shared_lock<std::shared_mutex> sharedLock;
    std::string input;

    // Prepare for select
    fd_set readfds;
    struct timeval timeout;

    while (!terminationFlag)
    {
        // Clear the set and add standard input (0) to it
        FD_ZERO(&readfds);
        FD_SET(0, &readfds);

        // Set timeout to 0, so select won't block and will allow checking terminationFlag
        timeout.tv_sec = 0;
        timeout.tv_usec = 100000; // 100ms

        // Use select to check if standard input has data
        int activity = select(1, &readfds, nullptr, nullptr, &timeout);
        if (activity > 0 && FD_ISSET(0, &readfds))
        {
            std::getline(std::cin, input);
            if (input == "bloom") {
                sharedLock = std::shared_lock<std::shared_mutex>(sharedMtx);
                std::cout << lsmTree->getBloomFilterSummary() << std::endl;
                sharedLock.unlock();
            } else if (input == "monkey") {
                exclusiveLock = std::unique_lock<std::shared_mutex>(sharedMtx);  // Lock the LSM Tree during monkey optimization
                std::cout << "\nMONKEY Bloom Filter optimization starting...\n" << std::endl;
                lsmTree->monkeyOptimizeBloomFilters();
                std::cout << "MONKEY Bloom Filter optimization complete" << std::endl;
                exclusiveLock.unlock();
            } else if (input == "misses") {
                sharedLock = std::shared_lock<std::shared_mutex>(sharedMtx);
                lsmTree->printMissesStats();
                sharedLock.unlock();
            } else if (input == "concurrent") {
                concurrent = !concurrent;
                std::cout << "Concurrent: " << concurrent << std::endl;
            } else if (input == "help") {
                std::cout << "bloom: Print Bloom Filter summary" << std::endl;
                std::cout << "monkey: Optimize Bloom Filters using MONKEY" << std::endl;
                std::cout << "misses: Print misses stats" << std::endl;
                std::cout << "concurrent: Toggle concurrent" << std::endl;
                std::cout << "help: Print this help message" << std::endl;
            } else {
                std::cout << "Invalid command" << std::endl;
            }
        }
    }
}

// Thread function to handle client connections
void Server::handleClient(int clientSocket) {
    std::cout << "New client connected" << std::endl;

    // Read commands from client
    char buffer[BUFFER_SIZE];
    // Check if client is still connected
    while (!terminationFlag)
    {
        // Receive command
        ssize_t n_read = recv(clientSocket, buffer, sizeof(buffer), 0);
        if (n_read == -1) {
            std::cerr << "Error receiving data from client" << std::endl;
            break;
        }
        else if (n_read == 0) {
            std::cout << "Client disconnected" << std::endl;
            return;
        }
        std::string command(buffer, n_read);

        // Parse command
        std::stringstream ss(command);
        handleCommand(ss, clientSocket);
    }
    // Clean up resources
    ::close(clientSocket);
    std::cout << "Client disconnected" << std::endl;
}

// Send a response to the client in chunks of BUFFER_SIZE bytes
void Server::sendResponse(int clientSocket, const std::string &response) {
    for (int i = 0; i < response.length(); i += BUFFER_SIZE) {
        char chunk[BUFFER_SIZE] = {};
        std::strncat(chunk, response.c_str() + i, BUFFER_SIZE);
        send(clientSocket, chunk, strlen(chunk), 0);
    }
    // Send the end of message indicator
    send(clientSocket, END_OF_MESSAGE, strlen(END_OF_MESSAGE), 0);
}

void Server::handleCommand(std::stringstream& ss, int clientSocket) {
    char op;
    ss >> op;
    KEY_t key, start, end;
    VAL_t value;
    
    // Pointers to store the results of get and range
    std::unique_ptr<VAL_t> valuePtr;
    std::unique_ptr<std::map<KEY_t, VAL_t>> rangePtr;
    // fileName is used for load and benchmark operations
    std::string fileName;

    // Path to data directory and JSON file for serialization
    std::string dataDir = DATA_DIRECTORY;
    std::string lsmTreeJsonFile = dataDir + LSM_TREE_JSON_FILE;

    // Response to send back to client
    std::string response;

    // Locks to ensure exclusive or shared access to the LSM tree
    std::unique_lock<std::shared_mutex> exclusiveLock;
    std::shared_lock<std::shared_mutex> sharedLock;

    // put, delete, load, benchmark, and quit operations are exclusive
    if (op == 'p' || op == 'd' || op == 'l' || op == 'b' || op == 'q') {
        exclusiveLock = std::unique_lock<std::shared_mutex>(sharedMtx);  
    // get, range, printStats, and info operations are shared  
    } else if (op == 'c' || op == 'g' || op == 'r' || op == 's' || op == 'i') {
        sharedLock = std::shared_lock<std::shared_mutex>(sharedMtx);
    } else {
        response = printDSLHelp();
        sendResponse(clientSocket, response);
        return;
    }
    switch (op) {
        // Put, delete, load, benchmark, and quit operations are exclusive
        case 'p':
            ss >> key >> value;
            // Break if key or value are not numbers
            if (ss.fail()) {
                response = printDSLHelp();
                break;
            }
            // Report error if key is less than VALUE_MIN or greater than VALUE_MAX
            if (value < VAL_MIN || value > VAL_MAX) {
                response = "ERROR: Value " + std::to_string(value) + " out of range [" + std::to_string(VAL_MIN) + ", " + std::to_string(VAL_MAX) + "]\n";
                break;
            }
            lsmTree->put(key, value);
            response = OK;
            break;
        case 'd':
            ss >> key;
            // Break if key is not a number
            if (ss.fail()) {
                response = printDSLHelp();
                break;
            }
            lsmTree->del(key);
            response = OK;
            break;
        case 'l':
            ss >> fileName;
            lsmTree->load(fileName);
            response = OK;
            break;
        case 'b':
            ss >> fileName;
            lsmTree->benchmark(fileName, verbose, concurrent);
            response = OK;
            break;
        case 'q':
            lsmTree->serializeLSMTreeToFile(lsmTreeJsonFile);
            response = OK;
            sendResponse(clientSocket, response);
            terminationFlag = 1;
            close();
            break;
        // Get, range, printStats, and info operations are shared
        case 'g':
            ss >> key;
            // Break if key is not a number
            if (ss.fail()) {
                response = printDSLHelp();
                break;
            }
            valuePtr = lsmTree->get(key);
            if (valuePtr != nullptr) {
                response = std::to_string(*valuePtr);
            }
            else {
                response = NO_VALUE;
            }
            break;
        case 'r':
            ss >> start >> end;
            // Break if start and end are not numbers
            if (ss.fail()) {
                response = printDSLHelp();
                break;
            }
            concurrent ? lsmTree->cRange(start, end) : lsmTree->range(start, end);
            if (rangePtr->size() > 0) {
                // Iterate over the map and store the key-value pairs in results
                for (const auto &p : *rangePtr) {
                    // Return key-value pairs as key:value separated by spaces
                    response += std::to_string(p.first) + ":" + std::to_string(p.second) + " ";
                }
            }
            else {
                response = NO_VALUE;
            }
            break;
        case 's':            
            response = lsmTree->printStats();
            break;
        case 'i':
            response = lsmTree->printTree();
            break;
        default:
            response = printDSLHelp();
        }
        sendResponse(clientSocket, response);
}

void Server::run() {
    // Accept incoming connections
    while (!terminationFlag) {
        sockaddr_in clientAddress;
        socklen_t clientAddressSize = sizeof(clientAddress);
        int clientSocket = accept(serverSocket, (sockaddr *)&clientAddress, &clientAddressSize);
        if (clientSocket == -1) {
            std::cerr << "Error accepting incoming connection" << std::endl;
            continue;
        }

        // Spawn thread to handle client connection
        std::thread clientThread(&Server::handleClient, this, clientSocket);
        clientThread.detach();
    }
}

Server::Server(int port, bool verbose) : port(port), verbose(verbose) {
    // Create server socket
    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == -1) {
        std::cerr << "Error creating server socket" << std::endl;
        exit(1);
    }

    // Bind socket to port
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    serverAddress.sin_port = htons(port);
    if (bind(serverSocket, (sockaddr *)&serverAddress, sizeof(serverAddress)) == -1) {
        std::cerr << "Error binding server socket" << std::endl;
        close();
        exit(1);
    }

    // Listen for incoming connections
    if (listen(serverSocket, SOMAXCONN) == -1) {
        std::cerr << "Error listening for incoming connections" << std::endl;
        close();
        exit(1);
    }
    std::cout << "\nServer started, listening on port " << port << std::endl;
}

void Server::close() {
    terminationFlag = 1;
    if (serverSocket != -1) {
        shutdown(serverSocket, SHUT_RDWR);
        ::close(serverSocket);
    }
}

void Server::createLSMTree(float bfErrorRate, int bufferNumPages, int fanout, Level::Policy levelPolicy, size_t numThreads) {
    // Path to data directory and JSON file for serialization
    std::string dataDir = DATA_DIRECTORY;
    std::string lsmTreeJsonFile = dataDir + LSM_TREE_JSON_FILE;
    // Create LSM-Tree with lsmTree unique pointer
    lsmTree = std::make_unique<LSMTree>(bfErrorRate, bufferNumPages, fanout, levelPolicy, numThreads);
    lsmTree->deserialize(lsmTreeJsonFile);
    printLSMTreeParameters(lsmTree->getBfErrorRate(), lsmTree->getBufferNumPages(), lsmTree->getFanout(), lsmTree->getLevelPolicy(), lsmTree->getNumThreads());
}

void printHelp() {
    std::cout << "Usage: ./server [OPTIONS]\n"
              << "Options:\n"
              << "  -e <errorRate>       Bloom filter error rate (default: " << DEFAULT_FANOUT << ")\n"
              << "  -n <numPages>        Number of buffer pages (default: " << DEFAULT_NUM_PAGES << ")\n"
              << "  -f <fanout>          LSM tree fanout (default: " << DEFAULT_FANOUT << ")\n"
              << "  -l <levelPolicy>     Level policy (default: " << Level::policyToString(DEFAULT_LEVELING_POLICY) << ")\n"
              << "  -p <port>            Port number (default: " << DEFAULT_SERVER_PORT << ")\n"
              << "  -t <numThreads>      Number of threads for GET and RANGE queries (default: " << DEFAULT_NUM_THREADS << ")\n"
              << "  -v                   Verbose benchmarking. Benchmark function will print out status as it processes.\n"
              << "  -h                   Print this help message\n" << std::endl
    ;
}

void Server::printLSMTreeParameters(float bfErrorRate, int bufferNumPages, int fanout, Level::Policy levelPolicy, size_t numThreads) {
    std::cout << "LSMTree parameters:" << std::endl;
    std::cout << "  Bloom filter error rate: " << bfErrorRate << std::endl;
    std::cout << "  Number of buffer pages: " << bufferNumPages << std::endl;
    std::cout << "  LSM-tree fanout: " << fanout << std::endl;
    std::cout << "  Level policy: " << Level::policyToString(levelPolicy) << std::endl;
    std::cout << "  Number of threads: " << numThreads << std::endl;
    std::cout << "  Verbosity: " << (verbose ? "on" : "off") << std::endl;
    std::cout << "\nLSM Tree ready and waiting for input" << std::endl;
}

std::string Server::printDSLHelp() {
    std::string helpText = 
        "\nLSM-Tree Domain Specific Language Help:\n\n"
        "Commands:\n"
        "1. Put (Insert/Update a key-value pair)\n"
        "   Syntax: p [INT1] [INT2]\n"
        "   Example: p 10 7\n\n"
        "2. Get (Retrieve the value associated with a key)\n"
        "   Syntax: g [INT1]\n"
        "   Example: g 10\n\n"
        "3. Range (Retrieve key-value pairs within a range of keys)\n"
        "   Syntax: r [INT1] [INT2]\n"
        "   Example: r 10 12\n\n"
        "4. Delete (Remove a key-value pair)\n"
        "   Syntax: d [INT1]\n"
        "   Example: d 10\n\n"
        "5. Load (Insert key-value pairs from a binary file)\n"
        "   Syntax: l \"/path/to/fileName\"\n"
        "   Example: l \"~/load_file.bin\"\n\n"
        "6. Benchmark (Run commands from a text file quietly with no output.)\n"
        "   NOT MULTIPLE THREAD SAFE since it bypasses the server/client blocking)\n"
        "   Syntax: b \"/path/to/fileName\"\n"
        "   Example: b \"~/workload.txt\"\n\n"
        "7. Print Stats (Display information about the current state of the tree)\n"
        "   Syntax: s\n"
        "   Example: \n"
        "     Logical Pairs: 10\n"
        "     LVL1: 3, LVL3: 9\n"
        "     45:56:L1 56:84:L1 91:45:L1\n"
        "     7:32:L3 19:73:L3 32:91:L3 45:64:L3 58:3:L3 85:15:L3 91:71:L3 95:87:L3 97:76:L3\n\n"
        "8. Summarized Tree Info\n"
        "   Syntax: i\n"
        "   Example: \n"
        "     Number of logical key-value pairs: 4,997,014\n"
        "     Bloom filter false positive rate: 0.042312\n"
        "     Number of I/O operations: 4,195,277\n"
        "     Number of entries in the buffer: 805,079\n"
        "     Maximum number of key-value pairs in the buffer: 1,048,576\n"
        "     Maximum size in bytes of the buffer: 8,388,608\n"
        "     Number of levels: 1\n"
        "     Number of SSTables in level 1: 4\n"
        "     Number of key-value pairs in level 1: 4,194,304\n"
        "     Max number of key-value pairs in level 1: 10,485,760\n"
        "     Is level 1 the last level? Yes\n"
        "9. Shutdown server and save the database state to disk\n"
        "   Syntax: q\n"
        "Refer to the documentation for detailed examples and explanations of each command.\n";

    return helpText;
}

int main(int argc, char **argv) {
    // Set signal handler for SIGINT
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigintHandler;
    sigaction(SIGINT, &sa, NULL);

    // Default values for options
    int opt, port = DEFAULT_SERVER_PORT;
    float bfErrorRate = DEFAULT_ERROR_RATE;
    int bufferNumPages = DEFAULT_NUM_PAGES;
    int fanout = DEFAULT_FANOUT;
    Level::Policy levelPolicy = DEFAULT_LEVELING_POLICY;
    bool verbose = DEFAULT_VERBOSE_LEVEL;
    int numThreads = DEFAULT_NUM_THREADS;

    // Parse command line arguments
    while ((opt = getopt(argc, argv, "e:n:f:l:p:t:hv")) != -1) {
        switch (opt) {
        case 'e':
            bfErrorRate = atof(optarg);
            break;
        case 'n':
            bufferNumPages = atoi(optarg);
            break;
        case 'f':
            fanout = atoi(optarg);
            break;
        case 'l':
            if (strcmp(optarg, "TIERED") == 0) {
                levelPolicy = Level::Policy::TIERED;
            } else if (strcmp(optarg, "LEVELED") == 0) {
                levelPolicy = Level::Policy::LEVELED;
            }
            else if (strcmp(optarg, "LAZY_LEVELED") == 0) {
                levelPolicy = Level::Policy::LAZY_LEVELED;
            }
            else if (strcmp(optarg, "PARTIAL") == 0) {
                levelPolicy = Level::Policy::PARTIAL;
            } 
            else {
                std::cerr << "Invalid value for -l option. Valid options are TIERED, LEVELED, LAZY_LEVELED, and PARTIAL" << std::endl;
                exit(1);
            }
            break;
        case 'p':
            port = atoi(optarg);
            break;
        case 't':
            numThreads = atoi(optarg);
            break;
        case 'v':
            verbose = true;
            // print "verbose is enabled"
            std::cout << "Verbose is enabled" << std::endl;
            break;
        case 'h':
            printHelp();
            exit(0);
        default:
            printHelp();
            exit(1);
        }
    }

    // Check if there are any unparsed arguments
    if (optind < argc) {
        std::cerr << "Unexpected argument: " << argv[optind] << std::endl;
        printHelp();
        exit(1);
    }

    // Create server instance with the specified port
    Server server(port, verbose);

    // Set the global server pointer
    server_ptr = &server;

    // Create LSM-Tree with the parsed options
    server.createLSMTree(bfErrorRate, bufferNumPages, fanout, levelPolicy, numThreads);
    // Create a thread for listening to standard input
    std::thread stdInThread(&Server::listenToStdIn, &server);
    server.run();

    // Clean up resources
    server.close();
    // Wait for the stdInThread to finish
    if (stdInThread.joinable()) {
        stdInThread.join();
    }
    return 0;
}
