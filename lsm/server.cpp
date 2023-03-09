#include <signal.h>
#include "server.hpp"

std::vector<std::pair<int, int>> range(int lower_key, int upper_key)
{
    // TODO: Implement range command
    return std::vector<std::pair<int, int>>();
}

void printStats()
{
    // TODO: Implement print stats command
}

// Thread function to handle client connections
void Server::handle_client(int client_socket)
{
    std::vector<std::pair<int, int>> results = {};

    std::cout << "New client connected" << std::endl;

    // Read commands from client
    char buffer[1024];
    // Check if client is still connected

    while (true)
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
        KEY_t key, lower_key, upper_key;
        VAL_t value;
        VAL_t* value_ptr;
        std::string file_name;

        char response[1024];

        switch (op)
        {
        case 'p':
            ss >> key >> value;
            // Throw error if key is less than VALUE_MIN or greater than VALUE_MAX
            if (value < VAL_MIN || value > VAL_MAX) {
                snprintf(response, sizeof(response), "ERROR: Value %d out of range [%d, %d], skipping\n", value, VAL_MIN, VAL_MAX);
                send(client_socket, response, strlen(response), 0);
                break;
            }

            lsmTree->put(key, value);
            snprintf(response, sizeof(response), "ok\n");
            // send(client_socket, response, strlen(response), 0);
            break;
        case 'g':
            ss >> key;
            value_ptr = lsmTree->get(key);
            if (value_ptr != nullptr) {
                snprintf(response, sizeof(response), "%d\n", *value_ptr);
            }
            else {
                snprintf(response, sizeof(response), "\r\n");
            }
            break;
        case 'r':
            ss >> lower_key >> upper_key;
            results = range(lower_key, upper_key);
            for (const auto &p : results) {
                snprintf(response, sizeof(response), "%d:%d ", p.first, p.second);
                send(client_socket, response, strlen(response), 0);
            }
            snprintf(response, sizeof(response), "\n");
            // send(client_socket, response, strlen(response), 0);
            break;
        case 'd':
            ss >> key;
            lsmTree->del(key);
            snprintf(response, sizeof(response), "ok\n");
            // send(client_socket, response, strlen(response), 0);
            break;
        case 'l':
            ss >> file_name;
            lsmTree->load(file_name);
            snprintf(response, sizeof(response), "ok\n");
            // send(client_socket, response, strlen(response), 0);
            break;
        case 's':
            lsmTree->printTree();
            snprintf(response, sizeof(response), "PRINT TREE %s\n", "PLACEHOLDER");
            break;
        default:
            snprintf(response, sizeof(response), "Invalid command\n");
            // send(client_socket, response, strlen(response), 0);
        }
        send(client_socket, response, strlen(response), 0);
    }

    // Clean up resources
    close();
    std::cout << "Client disconnected" << std::endl;
}

void Server::run()
{
    // Accept incoming connections
    while (true) {
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
    if (::bind(server_socket, (sockaddr *)&server_address, sizeof(server_address)) == -1) {
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

void Server::close()
{
    if (server_socket != -1) {
        shutdown(server_socket, SHUT_RDWR);
        ::close(server_socket);
    }
}

void Server::createLSMTree(int argc, char **argv)
{
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

    while ((opt = getopt(argc, argv, "c:e:b:n:f:l:h")) != -1) {
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
    lsmTree = std::make_unique<LSMTree>(bf_capacity, bf_error_rate, bf_bitset_size, buffer_num_pages, fanout, level_policy);
}

void Server::printHelp()
{
    // Create a mapping of the DEFAULT_LEVELING_POLICY enum to a string
    std::map<Level::Policy, std::string> level_policy_map;
    level_policy_map[Level::Policy::TIERED] = "TIERED";
    level_policy_map[Level::Policy::LEVELED] = "LEVELED";
    level_policy_map[Level::Policy::LAZY_LEVELED] = "LAZY_LEVELED";

    std::cout << "Usage: ./server [OPTIONS]\n"
              << "Options:\n"
              << "  -c <capacity>        Bloom filter capacity (default: " << DEFAULT_CAPACITY << ")\n"
              << "  -e <error_rate>      Bloom filter error rate (default: " << DEFAULT_ERROR_RATE << ")\n"
              << "  -b <bitset_size>     Bloom filter Bitset size in bytes (default: " << DEFAULT_BITSET_SIZE << ")\n"
              << "  -n <num_pages>       Number of buffer pages (default: " << DEFAULT_NUM_PAGES << ")\n"
              << "  -f <fanout>          LSM-tree fanout (default: " << DEFAULT_FANOUT << ")\n"
              << "  -l <level_policy>    Level policy (default: " << level_policy_map[DEFAULT_LEVELING_POLICY] << ")\n"
              << "  -h                   Print this help message\n" << std::endl
    ;
}

int main(int argc, char **argv) {
    // Create server object
    Server server(1234);
    server.createLSMTree(argc, argv);
    server.run();
    // Clean up resources
    server.close();
    return 0;
}
