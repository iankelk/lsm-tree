#include "server.hpp"

// Placeholders for LSM-Tree commands
// void put(KEY_t key, VAL_t value) {
//     // TODO: Implement put command
// }

int get(int key) {
    // TODO: Implement get command
    return 0;
}

std::vector<std::pair<int, int>> range(int lower_key, int upper_key) {
    // TODO: Implement range command
    return std::vector<std::pair<int, int>>();
}

void del(int key) {
    // TODO: Implement delete command
}

void load(const std::string& file_name) {
    // TODO: Implement load command
}

void printStats() {
    // TODO: Implement print stats command
}

// Thread function to handle client connections
void Server::handle_client(int client_socket) {
    std::vector<std::pair<int, int>> results = {};

    std::cout << "New client connected" << std::endl;

    // Read commands from client
    std::string command;
    char response[1024];
    while (std::getline(std::cin, command)) {
        // Parse command
        std::stringstream ss(command);
        char op;
        ss >> op;
        KEY_t key, lower_key, upper_key;
        VAL_t value;
        std::string file_name;
        
        switch (op) {
            case 'p':
                ss >> key >> value;
                lsmTree->put(key, value);
                snprintf(response, sizeof(response), "PUT %d %d\n", key, value);
                //send(client_socket, response, strlen(response), 0);
                break;
            case 'g':
                ss >> key;
                snprintf(response, sizeof(response),"%d\n", get(key));
                //send(client_socket, response, strlen(response), 0);
                break;
            case 'r':
                ss >> lower_key >> upper_key;
                results = range(lower_key, upper_key);
                for (const auto& p : results) {
                    snprintf(response, sizeof(response),"%d:%d ", p.first, p.second);
                    send(client_socket, response, strlen(response), 0);
                }
                snprintf(response, sizeof(response),"\n");
                //send(client_socket, response, strlen(response), 0);
                break;
            case 'd':
                ss >> key;
                del(key);
                snprintf(response, sizeof(response),"DEL %d\n", key);
                //send(client_socket, response, strlen(response), 0);
                break;
            case 'l':
                ss >> file_name;
                load(file_name);
                snprintf(response, sizeof(response),"LOADED %s\n", file_name.c_str());
                //send(client_socket, response, strlen(response), 0);
                break;
            case 's':
                lsmTree->printTree();
                snprintf(response, sizeof(response),"PRINT TREE %s\n", "PLACEHOLDER");
                break;
            default:
                snprintf(response, sizeof(response),"Invalid command\n");
                //send(client_socket, response, strlen(response), 0);
        }
        send(client_socket, response, strlen(response), 0);
    }
    
    // Clean up resources
    close();
    std::cout << "Client disconnected" << std::endl;
}

void Server::run() {
    // Accept incoming connections
    while (true) {
        sockaddr_in client_address;
        socklen_t client_address_size = sizeof(client_address);
        int client_socket = accept(server_socket, (sockaddr*)&client_address, &client_address_size);
        if (client_socket == -1) {
            std::cerr << "Error accepting incoming connection" << std::endl;
            continue;
        }
        
        // Spawn thread to handle client connection
        std::thread client_thread(&Server::handle_client, this, client_socket);
        client_thread.detach();
    }
}

Server::Server(int port) {
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
    if (::bind(server_socket, (sockaddr*)&server_address, sizeof(server_address)) == -1) {
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
    
    std::cout << "Server started, listening on port " << port << std::endl;
}

void Server::close() {
    if (server_socket != -1) {
        shutdown(server_socket, SHUT_RDWR);
        ::close(server_socket);
    }
}

void Server::createLSMTree(int argc, char** argv) {
    // Process command line arguments based on your LSM-Tree implementation
    int opt, bf_capacity, bf_bitset_size, buffer_num_pages, fanout;
    float bf_error_rate;
    Level::Policy level_policy;

    bf_capacity = DEFAULT_CAPACITY;
    bf_error_rate = DEFAULT_ERROR_RATE;
    bf_bitset_size = DEFAULT_BITSET_SIZE;
    buffer_num_pages = DEFAULT_NUM_PAGES;
    fanout = DEFAULT_FANOUT;
    level_policy = DEFAULT_LEVELING_POLICY;

    while ((opt = getopt(argc, argv, "c:e:b:n:f:l")) != -1) {
        switch (opt) {
            case 'c':
                bf_capacity = atoi(optarg);
                break;
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
                level_policy = Level::Policy::LAZY_LEVELED;
                break;
            default:
                std::cerr << "Usage: " << argv[0] << " [-c capacity] [-e error_rate] [-b bitset_size] [-n num_pages] [-f fanout] [-l level_policy]" << std::endl;
                exit(1);
        }
    }
    // Create LSM-Tree with lsmTree unique pointer
    lsmTree = std::make_unique<LSMTree>(bf_capacity, bf_error_rate, bf_bitset_size, buffer_num_pages, fanout, level_policy);
}

int main(int argc, char** argv) {
    // Create server object
    Server server(1234);
    server.createLSMTree(argc, argv);
    server.run();

    // Clean up resources
    server.close();
    return 0;
}
