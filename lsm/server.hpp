#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <unistd.h>
#include <netinet/in.h>

#include "lsm_tree.hpp"

// Define the server class
class Server {
public:
    Server(int port);
    void createLSMTree(int argc, char** argv);
    void run();
    void close();
private:
    // Unique pointer to LSMTree
    std::unique_ptr<LSMTree> lsmTree;
    int port;
    int server_socket;
    int client_socket;
    struct sockaddr_in server_address;
    struct sockaddr_in client_address;
    socklen_t client_address_size;
    std::thread client_thread;
    void handle_client(int client_socket);
};
