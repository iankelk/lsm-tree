#include <signal.h>
#include <sstream>
#include <iostream>
#include <unistd.h>
#include <shared_mutex>
#include <sys/select.h>
#include <atomic>
#include <string>
#include "server.hpp"
#include "utils.hpp"

std::atomic<sig_atomic_t> terminationFlag{0};

Server *server_ptr = nullptr;

void handleSIGINT([[maybe_unused]] int signal) {
    terminationFlag.store(1, std::memory_order_release);
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

    while (!terminationFlag.load(std::memory_order_acquire))
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
                SyncedCout() << lsmTree->getBloomFilterSummary() << std::endl;
                sharedLock.unlock();
            } else if (input == "monkey") {
                exclusiveLock = std::unique_lock<std::shared_mutex>(sharedMtx);  // Lock the LSM Tree during monkey optimization
                SyncedCout() << "\nMONKEY Bloom Filter optimization starting...\n" << std::endl;
                lsmTree->monkeyOptimizeBloomFilters();
                SyncedCout() << "MONKEY Bloom Filter optimization complete" << std::endl;
                exclusiveLock.unlock();
            } else if (input == "misses") {
                sharedLock = std::shared_lock<std::shared_mutex>(sharedMtx);
                lsmTree->printHitsMissesStats();
                sharedLock.unlock();
            } else if (input == "io") {
                sharedLock = std::shared_lock<std::shared_mutex>(sharedMtx);
                SyncedCout() << lsmTree->printLevelIoCount() << std::endl;
                sharedLock.unlock();
            } else if (input == "quit") {
                terminationFlag.store(1, std::memory_order_release);
                // Send SERVER_SHUTDOWN command to all connected clients
                std::unique_lock<std::mutex> lock(connectedClientsMutex);
                for (int clientSocket : connectedClients) {
                    send(clientSocket, SERVER_SHUTDOWN.c_str(), SERVER_SHUTDOWN.length(), 0);
                }
                lock.unlock();
                close();
            } else if (input == "qs") {
                // Path to data directory and JSON file for serialization
                std::string lsmTreeJsonFile = lsmTree->getDataDirectory(); + "/" + LSM_TREE_JSON_FILE;
                lsmTree->serializeLSMTreeToFile(lsmTreeJsonFile);
                terminationFlag.store(1, std::memory_order_release);
                close();
            } else if (input == "help") {
                SyncedCout() << "bloom: Print Bloom Filter summary" << std::endl;
                SyncedCout() << "monkey: Optimize Bloom Filters using MONKEY" << std::endl;
                SyncedCout() << "misses: Print hits and misses stats" << std::endl;
                SyncedCout() << "io: Print level IO count" << std::endl;
                SyncedCout() << "quit: Quit server" << std::endl;
                SyncedCout() << "qs: Save server to disk and quit" << std::endl;
                SyncedCout() << "help: Print this help message" << std::endl;
            } else {
                SyncedCout() << "Invalid command. Use \"help\" for list of available commands" << std::endl;
            }
        }
    }
}

// Thread function to handle client connections
void Server::handleClient(int clientSocket) {
    SyncedCout() << "New client connected with Thread ID: " << std::this_thread::get_id() << std::endl;
    
    // Add client to connected clients set
    {
        std::unique_lock<std::mutex> lock(connectedClientsMutex);
        connectedClients.insert(clientSocket);
    }

    // Read commands from client
    char buffer[BUFFER_SIZE];
    // Check if client is still connected
    while (!terminationFlag.load(std::memory_order_acquire))
    {
        // Receive command
        ssize_t n_read = recv(clientSocket, buffer, sizeof(buffer), 0);
        if (n_read == -1) {
            SyncedCerr() << "Error receiving data from client" << std::endl;
            break;
        }
        else if (n_read == 0) {
            SyncedCerr() << "Client disconnected" << std::endl;
            return;
        }
        std::string command(buffer, n_read);

        // Parse command
        std::stringstream ss(command);
        handleCommand(ss, clientSocket);
    }
    // Clean up resources
    ::close(clientSocket);
    // Remove client from connected clients set
    {
        std::unique_lock<std::mutex> lock(connectedClientsMutex);
        connectedClients.erase(clientSocket);
    }
    SyncedCout() << "Client disconnected with Thread ID: " << std::this_thread::get_id() << std::endl;
}

void Server::sendResponse(int clientSocket, const std::string &response) {
    for (size_t i = 0; i < response.length(); i += BUFFER_SIZE) {
        char chunk[BUFFER_SIZE] = {};
        std::strncpy(chunk, response.c_str() + i, BUFFER_SIZE - 1); // Subtracts 1 from BUFFER_SIZE to make sure the string is null-terminated
        send(clientSocket, chunk, strlen(chunk), 0);
    }
    // Send the end of message indicator
    send(clientSocket, END_OF_MESSAGE.c_str(), strlen(END_OF_MESSAGE.c_str()), 0);

}


void Server::handleCommand(std::stringstream& ss, int clientSocket) {
    char op;
    ss >> op;
    int numToPrintFromEachLevel;
    KEY_t key, start, end;
    VAL_t value;
    
    // Pointers to store the results of get and range
    std::unique_ptr<VAL_t> valuePtr;
    std::unique_ptr<std::vector<kvPair>> rangePtr;
    // fileName is used for load and benchmark operations
    std::string fileName;

    // Path to data directory and JSON file for serialization
    std::string lsmTreeJsonFile = lsmTree->getDataDirectory() + "/" + LSM_TREE_JSON_FILE;

    // Response to send back to client
    std::string response;

    switch (op) {
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
            lsmTree->load(removeQuotes(fileName));
            response = OK;
            break;
        case 'b':
            ss >> fileName;
            lsmTree->benchmark(fileName, verbose, verboseFrequency);
            response = OK;
            break;
        case 'q':
            lsmTree->serializeLSMTreeToFile(lsmTreeJsonFile);
            response = OK;
            sendResponse(clientSocket, response);
            terminationFlag.store(1, std::memory_order_release);
            close();
            break;
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
            rangePtr = lsmTree->range(start, end);
            if (rangePtr->size() > 0) {
                // Iterate over the vector and store the key-value pairs in results
                for (const auto &p : *rangePtr) {
                    // Return key-value pairs as key:value separated by spaces
                    response += std::to_string(p.key) + ":" + std::to_string(p.value) + " ";
                }
            }
            else {
                response = NO_VALUE;
            }
            break;
        case 's':
            // Check if there's an integer after the 's' option
            if (ss >> numToPrintFromEachLevel) {
                // Check if the integer is positive
                if (numToPrintFromEachLevel > 0) {
                    // Pass the integer to the printStats function if it's provided and positive
                    response = lsmTree->printStats(numToPrintFromEachLevel);
                } else {
                    // Set the response to an error message if the integer is not positive
                    response = "For printing stats, the number of key-value pairs to print must be positive.\n";
                }
            } else {
                // Call the function without any arguments if the integer is not provided
                response = lsmTree->printStats(STATS_PRINT_EVERYTHING);
            }
            break;
        case 'i':
            response = lsmTree->printInfo();
            break;
        default:
            response = printDSLHelp();
        }
        sendResponse(clientSocket, response);
}

void Server::run() {
    // Prepare for select
    fd_set readfds;
    struct timeval timeout;

    while (!terminationFlag.load(std::memory_order_acquire)) {
        // Clear the set and add serverSocket to it
        FD_ZERO(&readfds);
        FD_SET(serverSocket, &readfds);

        // Set timeout to 0, so select won't block and will allow checking terminationFlag
        timeout.tv_sec = 0;
        timeout.tv_usec = 100000; // 100ms

        // Use select to check if serverSocket has an incoming connection
        int activity = select(serverSocket + 1, &readfds, nullptr, nullptr, &timeout);

        if (activity > 0 && FD_ISSET(serverSocket, &readfds)) {
            sockaddr_in clientAddress;
            socklen_t clientAddressSize = sizeof(clientAddress);
            int clientSocket = accept(serverSocket, (sockaddr *)&clientAddress, &clientAddressSize);
            if (clientSocket == -1) {
                std::cerr << "Error accepting incoming connection" << std::endl;
                continue;
            }

            // Spawn thread to handle client connection
            clientThreads.push_back(std::make_unique<std::thread>(&Server::handleClient, this, clientSocket));

            clientThreads.back()->detach();
        }
    }
}


Server::Server(int port, bool verbose, size_t verboseFrequency) : verbose(verbose), verboseFrequency(verboseFrequency) {
    // Create server socket
    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == -1) {
        std::cerr << "Error creating server socket" << std::endl;
        exit(1);
    }

    // Set the SO_REUSEADDR option
    int enableReuse = 1;
    if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &enableReuse, sizeof(enableReuse)) == -1) {
        std::cerr << "Error setting SO_REUSEADDR option" << std::endl;
        close();
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
    SyncedCout() << "\nServer started, listening on port " << port << std::endl;
}


void Server::close() {
    terminationFlag.store(1, std::memory_order_release);
    if (serverSocket != -1) {
        shutdown(serverSocket, SHUT_RDWR);
        ::close(serverSocket);
    }
}

void Server::createLSMTree(float bfErrorRate, int bufferNumPages, int fanout, Level::Policy levelPolicy, size_t numThreads, 
                           float compactionPercentage, std::string dataDirectory, bool throughputPrinting, size_t throughputFrequency) {
    // Path to data directory and JSON file for serialization
    std::string lsmTreeJsonFile = dataDirectory + "/" + LSM_TREE_JSON_FILE;
    // Create LSM-Tree with lsmTree unique pointer
    lsmTree = std::make_unique<LSMTree>(bfErrorRate, bufferNumPages, fanout, levelPolicy, numThreads, 
                                        compactionPercentage, dataDirectory, throughputPrinting, throughputFrequency);
    lsmTree->deserialize(lsmTreeJsonFile);
    printLSMTreeParameters(lsmTree->getBfErrorRate(), lsmTree->getBufferMaxKvPairs(), lsmTree->getFanout(), lsmTree->getLevelPolicy(), lsmTree->getNumThreads(),
                           lsmTree->getCompactionPercentage(), lsmTree->getDataDirectory(), lsmTree->getThroughputPrinting(), lsmTree->getThroughputFrequency());
}

void printHelp() {
    SyncedCout() << "Usage: ./server [OPTIONS]\n"
              << "Options:\n"
              << "  -e <errorRate>              Bloom filter error rate (default: " << DEFAULT_FANOUT << ")\n"
              << "  -n <numPages>               Size of the buffer by number of disk pages (default: " << DEFAULT_NUM_PAGES << ")\n"
              << "  -f <fanout>                 LSM tree fanout (default: " << DEFAULT_FANOUT << ")\n"
              << "  -l <levelPolicy>            Level policy (default: " << Level::policyToString(DEFAULT_LEVELING_POLICY) << ")\n"
              << "  -p <port>                   Port number (default: " << DEFAULT_SERVER_PORT << ")\n"
              << "  -t <numThreads>             Number of threads for GET and RANGE queries (default: " << DEFAULT_NUM_THREADS << ")\n"
              << "  -c <compactionPercentage>   Compaction \% used for PARTIAL compaction only (default: " << DEFAULT_COMPACTION_PERCENTAGE << ")\n"
              << "  -v <optional: frequency>    Verbose benchmarking. Reports every \"frequency\" number of commands (default: " << DEFAULT_VERBOSE_FREQUENCY << ")\n"
              << "  -s <optional: frequency>    Throughput reporting. Reports every \"frequency\" number of commands (default: " << DEFAULT_THROUGHPUT_FREQUENCY << ")\n"
              << "  -d <dataDirectory>          Data directory (default: " << DEFAULT_DATA_DIRECTORY << ")\n"
              << "  -h                          Print this help message\n" << std::endl
    ;
}

void Server::printLSMTreeParameters(float bfErrorRate, size_t bufferMaxKvPairs, int fanout, Level::Policy levelPolicy, size_t numThreads, 
                                    float compactionPercentage, const std::string& dataDirectory, bool throughputPrinting, size_t throughputFrequency) {
    std::string verboseFrequencyString = (verbose) ? " (report every " + addCommas(std::to_string(verboseFrequency)) + " commands)" : "";    
    std::string throughputFrequencyString = (throughputPrinting) ? " (report every " + addCommas(std::to_string(throughputFrequency)) + " commands)" : "";
    SyncedCout() << "LSMTree parameters:" << std::endl;
    SyncedCout() << "  Bloom filter error rate: " << bfErrorRate << std::endl;
    SyncedCout() << "  Max key-value pairs in buffer: " << addCommas(std::to_string(bufferMaxKvPairs)) << " (" << 
                       addCommas(std::to_string(bufferMaxKvPairs * sizeof(kvPair))) << " bytes) " << std::endl;
    SyncedCout() << "  LSM-tree fanout: " << fanout << std::endl;
    SyncedCout() << "  Level policy: " << Level::policyToString(levelPolicy) << std::endl;
    SyncedCout() << "  Number of threads: " << numThreads << std::endl;
    if (levelPolicy == Level::Policy::PARTIAL) {
        SyncedCout() << "  Compaction percentage: " << compactionPercentage << std::endl;
    }
    SyncedCout() << "  Verbosity: " << (verbose ? "on" : "off") << verboseFrequencyString << std::endl;
    SyncedCout() << "  Data directory: " << dataDirectory << std::endl;
    SyncedCout() << "  Throughput printing: " << (throughputPrinting ? "on" : "off") << throughputFrequencyString << std::endl;
    SyncedCout() << "\nLSM Tree ready and waiting for input" << std::endl;
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
        "   Syntax: s [INT1 (optional number of results returned per level)]\n"
        "   Example: \n"
        "     Logical Pairs: 10\n"
        "     LVL1: 3, LVL3: 9\n"
        "     45:56:L1 56:84:L1 91:45:L1\n"
        "     7:32:L3 19:73:L3 32:91:L3 45:64:L3 58:3:L3 85:15:L3 91:71:L3 95:87:L3 97:76:L3\n\n"
        "8. Summarized Tree Info\n"
        "   Syntax: i\n"
        "   Example: \n"
        "     Number of logical key-value pairs: 9,988,261\n"
        "     Bloom filter measured false positive rate: 0.000042\n"
        "     Number of I/O operations: 238,248\n"
        "     Number of entries in the buffer: 38,207 (Max 262,144 entries, or 2,097,152 bytes, 14\% full)\n"
        "     Maximum number of key-value pairs in the buffer: 1,048,576\n"
        "     Maximum size in bytes of the buffer: 8,388,608\n"
        "     Number of Levels: 2\n"
        "     Number of Runs in Level 1: 8\n"
        "     Number of key-value pairs in level 1: 2,097,152 (Max 2,621,440, 80\% full)\n"
        "     Number of Runs in Level 2: 3\n"
        "     Number of key-value pairs in level 2: 7,864,320 (Max 26,214,400, 30\% full)\n"
        "     Level 1 disk type: SSD, disk penalty multiplier: 1, is it the last level? No\n"
        "     Level 2 disk type: HDD1, disk penalty multiplier: 5, is it the last level? Yes\n\n"
        "9. Shutdown server and save the database state to disk\n"
        "   Syntax: q\n"
        "Refer to the documentation for detailed examples and explanations of each command.\n";

    return helpText;
}

int main(int argc, char **argv) {
    // Set signal handler for SIGINT
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handleSIGINT;
    sigaction(SIGINT, &sa, NULL);

    // Default values for options
    int opt, port = DEFAULT_SERVER_PORT;
    float bfErrorRate = DEFAULT_ERROR_RATE;
    int bufferNumPages = DEFAULT_NUM_PAGES;
    int fanout = DEFAULT_FANOUT;
    Level::Policy levelPolicy = DEFAULT_LEVELING_POLICY;
    bool verbose = DEFAULT_VERBOSE_PRINTING;
    int numThreads = DEFAULT_NUM_THREADS;
    size_t verboseFrequency = DEFAULT_VERBOSE_FREQUENCY;
    float compactionPercentage = DEFAULT_COMPACTION_PERCENTAGE;
    std::string dataDirectory = DEFAULT_DATA_DIRECTORY;
    bool throughputPrinting = DEFAULT_THROUGHPUT_PRINTING;
    size_t throughputFrequency = DEFAULT_THROUGHPUT_FREQUENCY;

    // Parse command line arguments
    while ((opt = getopt(argc, argv, "e:n:f:l:p:t:c:d:shv")) != -1) {
        switch (opt) {
        case 'e':
            bfErrorRate = atof(optarg);
            break;
        case 'n':
            bufferNumPages = std::stoull(optarg);
            break;
        case 'f':
            fanout = std::stoull(optarg);
            if (fanout < 2) {
                std::cerr << "Invalid value for -f option. Fanout must be greater than 1." << std::endl;
                exit(1);
            }
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
            numThreads = std::stoull(optarg);
            break;
        case 'c':
            compactionPercentage = atof(optarg);
            break;
        case 'd':
            dataDirectory = optarg;
            break;
        case 'v':
            verbose = true;
            // Check if there is an argument after -v and if it is a number
            if (optind < argc && isdigit(argv[optind][0])) {
                verboseFrequency = std::stoull(argv[optind]);
                optind++; // Move to the next argument
            }
            break;
        case 's':
            throughputPrinting = true;
            // Check if there is an argument after -s and if it is a number
            if (optind < argc && isdigit(argv[optind][0])) {
                throughputFrequency = std::stoull(argv[optind]);
                optind++; // Move to the next argument
            }
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
    Server server(port, verbose, verboseFrequency);

    // Set the global server pointer
    server_ptr = &server;

    // Create LSM-Tree with the parsed options
    server.createLSMTree(bfErrorRate, bufferNumPages, fanout, levelPolicy, numThreads, compactionPercentage, dataDirectory, throughputPrinting, throughputFrequency);
    // Create a thread for listening to standard input
    std::thread stdInThread(&Server::listenToStdIn, &server);
    server.run();

    // Clean up resources
    server.close();
    // Wait for the stdInThread to finish
    if (stdInThread.joinable()) {
        stdInThread.join();
    }

    // Wait for all client threads to finish
    for (auto& thread : server.clientThreads) {
        if (thread->joinable()) {
            thread->join();
        }
    }
    return 0;
}
