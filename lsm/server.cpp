#include <signal.h>
#include <sstream>
#include <iostream>
#include <unistd.h>
#include "server.hpp"

volatile sig_atomic_t termination_flag = 0;

Server server(SERVER_PORT);

void sigint_handler(int signal) {
    termination_flag = 1;
    server.close();
}

// Thread function to handle client connections
void Server::handle_client(int client_socket)
{
    std::cout << "New client connected" << std::endl;

    // Read commands from client
    char buffer[BUFFER_SIZE];
    // Check if client is still connected
    while (!termination_flag)
    {
        // Receive command
        ssize_t n_read = recv(client_socket, buffer, sizeof(buffer), 0);
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
        char op;
        ss >> op;
        KEY_t key, start, end;
        VAL_t value;
        
        // Pointers to store the results of get and range
        std::unique_ptr<VAL_t> value_ptr;
        std::unique_ptr<std::map<KEY_t, VAL_t>> range_ptr;
        std::string file_name;

        // Response to send back to client
        std::string response;
        
        switch (op) {
        case 'p':
            ss >> key >> value;
            // Throw error if key is less than VALUE_MIN or greater than VALUE_MAX
            if (value < VAL_MIN || value > VAL_MAX) {
                response = "ERROR: Value " + std::to_string(value) + " out of range [" + std::to_string(VAL_MIN) + ", " + std::to_string(VAL_MAX) + "]\n";
                break;
            }

            lsmTree->put(key, value);
            // TODO: Define and handle a proper confirmation message
            response = "ok";
            break;
        case 'g':
            ss >> key;
            value_ptr = lsmTree->get(key);
            if (value_ptr != nullptr) {
                response = std::to_string(*value_ptr);
            }
            else {
                response = NO_VALUE;
            }
            break;
        case 'r':
            ss >> start >> end;
            range_ptr = lsmTree->range(start, end);
            if (range_ptr != nullptr) {
                // Iterate over the map and store the key-value pairs in results
                for (const auto &p : *range_ptr) {
                    // Make response "%d:%d ", p.first, p.second
                    response += std::to_string(p.first) + ":" + std::to_string(p.second) + " ";
                }
            }
            else {
                response = NO_VALUE;
            }
            break;
        case 'd':
            ss >> key;
            lsmTree->del(key);
            response = "ok";
            break;
        case 'l':
            ss >> file_name;
            lsmTree->load(file_name);
            response = "ok";
            break;
        case 'i':
            response = lsmTree->printTree();
            break;
        case 's':            
            response = lsmTree->printStats();
            break;
        case 'q':
            lsmTree->serializeLSMTreeToFile(LSM_TREE_FILE);
            response = "ok";
            sendResponse(client_socket, response);
            termination_flag = 1;
            close();
            break;
        default:
            response = printDSLHelp();
        }
        sendResponse(client_socket, response);
    }
    // Clean up resources
    close();
    std::cout << "Client disconnected" << std::endl;
}

// Send a response to the client in chunks of BUFFER_SIZE bytes
void Server::sendResponse(int client_socket, const std::string &response) {
    for (int i = 0; i < response.length(); i += BUFFER_SIZE) {
        char chunk[BUFFER_SIZE] = {};
        std::strncat(chunk, response.c_str() + i, BUFFER_SIZE);
        send(client_socket, chunk, strlen(chunk), 0);
    }
    // Send the end of message indicator
    send(client_socket, END_OF_MESSAGE, strlen(END_OF_MESSAGE), 0);
}


void Server::run()
{
    // Accept incoming connections
    while (!termination_flag) {
        sockaddr_in client_address;
        socklen_t client_address_size = sizeof(client_address);
        int client_socket = accept(server_socket, (sockaddr *)&client_address, &client_address_size);
        if (client_socket == -1) {
            std::cerr << "Error accepting incoming connection" << std::endl;
            continue;
        }

        // Spawn thread to handle client connection
        std::thread client_thread(&Server::handle_client, this, client_socket);
        client_thread.detach();
    }
}

Server::Server(int port)
{
    // Create server socket
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket == -1) {
        std::cerr << "Error creating server socket" << std::endl;
        exit(1);
    }

    // Bind socket to port
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(port);
    if (bind(server_socket, (sockaddr *)&server_address, sizeof(server_address)) == -1) {
        std::cerr << "Error binding server socket" << std::endl;
        close();
        exit(1);
    }

    // Listen for incoming connections
    if (listen(server_socket, SOMAXCONN) == -1) {
        std::cerr << "Error listening for incoming connections" << std::endl;
        close();
        exit(1);
    }
    std::cout << "\nServer started, listening on port " << port << std::endl;
}

void Server::close() {
    termination_flag = true;
    if (server_socket != -1) {
        shutdown(server_socket, SHUT_RDWR);
        ::close(server_socket);
    }
}

void Server::createLSMTree(int argc, char **argv)
{
    // Process command line arguments based on your LSM-Tree implementation
    int opt, bf_bitset_size, buffer_num_pages, fanout;
    float bf_error_rate;
    Level::Policy level_policy;

    bf_error_rate = DEFAULT_ERROR_RATE;
    bf_bitset_size = DEFAULT_BITSET_SIZE;
    buffer_num_pages = DEFAULT_NUM_PAGES;
    fanout = DEFAULT_FANOUT;
    level_policy = DEFAULT_LEVELING_POLICY;

    while ((opt = getopt(argc, argv, "e:b:n:f:l:h")) != -1) {
        switch (opt) {
        case 'e':
            bf_error_rate = atof(optarg);
            break;
        case 'b':
            bf_bitset_size = atoi(optarg);
            break;
        case 'n':
            buffer_num_pages = atoi(optarg);
            break;
        case 'f':
            fanout = atoi(optarg);
            break;
        case 'l':
            if (strcmp(optarg, "TIERED") == 0) {
                level_policy = Level::Policy::TIERED;
            } else if (strcmp(optarg, "LEVELED") == 0) {
                level_policy = Level::Policy::LEVELED;
            }
            else if (strcmp(optarg, "LAZY_LEVELED") == 0) {
                level_policy = Level::Policy::LAZY_LEVELED;
            }
            else {
                std::cerr << "Invalid value for -l option. Valid options are TIERED, LEVELED, and LAZY_LEVELED" << std::endl;
                exit(1);
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
    // Create LSM-Tree with lsmTree unique pointer
    lsmTree = std::make_unique<LSMTree>(bf_error_rate, bf_bitset_size, buffer_num_pages, fanout, level_policy);
    lsmTree->deserialize(LSM_TREE_FILE);
    printLSMTreeParameters(lsmTree->getBfErrorRate(), lsmTree->getBfBitsetSize(), lsmTree->getBufferNumPages(), lsmTree->getFanout(), lsmTree->getLevelPolicy());
}

void Server::printHelp()
{
    std::cout << "Usage: ./server [OPTIONS]\n"
              << "Options:\n"
              << "  -e <error_rate>      Bloom filter error rate (default: " << DEFAULT_ERROR_RATE << ")\n"
              << "  -b <bitset_size>     Bloom filter Bitset size in bytes (default: " << DEFAULT_BITSET_SIZE << ")\n"
              << "  -n <num_pages>       Number of buffer pages (default: " << DEFAULT_NUM_PAGES << ")\n"
              << "  -f <fanout>          LSM-tree fanout (default: " << DEFAULT_FANOUT << ")\n"
              << "  -l <level_policy>    Level policy (default: " << Level::policyToString(DEFAULT_LEVELING_POLICY) << ")\n"
              << "  -h                   Print this help message\n" << std::endl
    ;
}

void Server::printLSMTreeParameters(float bf_error_rate, int bf_bitset_size, int buffer_num_pages, int fanout, Level::Policy level_policy) {
    std::cout << "LSMTree parameters:" << std::endl;
    std::cout << "  Bloom filter error rate: " << bf_error_rate << std::endl;
    std::cout << "  Bloom filter Bitset size: " << bf_bitset_size << " bits" << std::endl;
    std::cout << "  Number of buffer pages: " << buffer_num_pages << std::endl;
    std::cout << "  LSM-tree fanout: " << fanout << std::endl;
    std::cout << "  Level policy: " << Level::policyToString(level_policy) << std::endl;
    std::cout << "\nLSM Tree ready and waiting for input" << std::endl;
}

std::string Server::printDSLHelp() {
    std::string helpText = 
        "LSM-Tree Domain Specific Language Help:\n\n"
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
        "   Syntax: l \"/path/to/file_name\"\n"
        "   Example: l \"~/load_file.bin\"\n\n"
        "6. Print Stats (Display information about the current state of the tree)\n"
        "   Syntax: s\n"
        "   Example: \n"
        "     Logical Pairs: 10\n"
        "     LVL1: 3, LVL3: 9\n"
        "     45:56:L1 56:84:L1 91:45:L1\n"
        "     7:32:L3 19:73:L3 32:91:L3 45:64:L3 58:3:L3 85:15:L3 91:71:L3 95:87:L3 97:76:L3\n\n"
        "Refer to the documentation for detailed examples and explanations of each command.\n";

    return helpText;
}

int main(int argc, char **argv) {
    // Set signal handler for SIGINT
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigint_handler;
    sigaction(SIGINT, &sa, NULL);

    // Create LSM-Tree
    server.createLSMTree(argc, argv);
    server.run();
    // Clean up resources
    server.close();
    return 0;
}
