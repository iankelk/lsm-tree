#include <signal.h>
#include "server.hpp"

volatile sig_atomic_t termination_flag = 0;

Server server(SERVER_PORT);

void sigint_handler(int signal) {
    termination_flag = 1;
    server.close();
}

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
        KEY_t key, lower_key, upper_key;
        VAL_t value;
        VAL_t* value_ptr;
        std::string file_name;

        //char response[BUFFER_SIZE];
        // Create a string of indefinite length called response
        std::string response;
        
        switch (op) {
        case 'p':
            ss >> key >> value;
            // Throw error if key is less than VALUE_MIN or greater than VALUE_MAX
            if (value < VAL_MIN || value > VAL_MAX) {
                // Create the error message "ERROR: Value %d out of range [%d, %d], skipping\n", value, VAL_MIN, VAL_MAX
                // and store it in response
                response = "ERROR: Value " + std::to_string(value) + " out of range [" + std::to_string(VAL_MIN) + ", " + std::to_string(VAL_MAX) + "]\n";
                //snprintf(response, sizeof(response), "ERROR: Value %d out of range [%d, %d], skipping\n", value, VAL_MIN, VAL_MAX);
                //send(client_socket, response, strlen(response), 0);
                break;
            }

            lsmTree->put(key, value);
            // Make response "ok\n"
            response = "ok";
            //snprintf(response, sizeof(response), "ok\n");
            // send(client_socket, response, strlen(response), 0);
            break;
        case 'g':
            ss >> key;
            value_ptr = lsmTree->get(key);
            if (value_ptr != nullptr) {
                // Make response "%d\n", *value_ptr
                response = std::to_string(*value_ptr);
                //snprintf(response, sizeof(response), "%d\n", *value_ptr);
            }
            else {
                // Make response "\r\n"
                response = "\r\n";
                //snprintf(response, sizeof(response), "\r\n");
            }
            break;
        case 'r':
            ss >> lower_key >> upper_key;
            results = range(lower_key, upper_key);
            for (const auto &p : results) {
                // Make response "%d:%d ", p.first, p.second
                response = std::to_string(p.first) + ":" + std::to_string(p.second) + " ";

                //snprintf(response, sizeof(response), "%d:%d ", p.first, p.second);
                //send(client_socket, response, strlen(response), 0);
            }
            // Make response "\n"
            response = "\n";
            //snprintf(response, sizeof(response), "\n");
            // send(client_socket, response, strlen(response), 0);
            break;
        case 'd':
            ss >> key;
            lsmTree->del(key);
            // Make response "ok\n"
            response = "ok";
            //snprintf(response, sizeof(response), "ok\n");
            // send(client_socket, response, strlen(response), 0);
            break;
        case 'l':
            ss >> file_name;
            lsmTree->load(file_name);
            // Make response "ok\n"
            response = "ok";
            //snprintf(response, sizeof(response), "ok\n");
            // send(client_socket, response, strlen(response), 0);
            break;
        case 'i':
            lsmTree->printTree();
            // Make response "PRINT TREE PLACEHOLDER\n"
            response = "PRINT TREE PLACEHOLDER\n";
            //snprintf(response, sizeof(response), "PRINT TREE %s\n", "PLACEHOLDER");
            break;
        case 's':            
            response = lsmTree->printStats();
            break;
        default:
            // Make response "Invalid command\n"
            response = "Invalid command";
        }
        // Send the message in chunks of BUFFER_SIZE in a loop until the end. 
        // Mark the end of the message with the END_OF_MESSAGE indicator.
        for (int i = 0; i < strlen(response.c_str()); i += BUFFER_SIZE) {
            char chunk[BUFFER_SIZE] = {};
            // Initialize chunk to all 0s
            std::strncat(chunk, response.c_str() + i, BUFFER_SIZE);
            // print the chunk to stdout
            //std::cout << "CHUNK:[" << chunk << "]" << std::endl;
            send(client_socket, chunk, strlen(chunk), 0);
        }
        // Send the end of message indicator
        send(client_socket, END_OF_MESSAGE, strlen(END_OF_MESSAGE), 0);
        
    }
    // Clean up resources
    close();
    std::cout << "Client disconnected" << std::endl;
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
