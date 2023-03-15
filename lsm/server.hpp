#ifndef SERVER_HPP
#define SERVER_HPP
#include <thread>
#include <netinet/in.h>
#include "lsm_tree.hpp"

class Server
{
public:
    Server(int port);
    void createLSMTree(int argc, char **argv);
    void run();
    void close();
private:
    std::unique_ptr<LSMTree> lsmTree;
    int port;
    int server_socket;
    int client_socket;
    struct sockaddr_in server_address;
    struct sockaddr_in client_address;
    socklen_t client_address_size;
    std::thread client_thread;
    void handle_client(int client_socket);
    void printHelp();
    std::string printDSLHelp();
    void sendResponse(int client_socket, const std::string &response);
    void printLSMTreeParameters(float bf_error_rate, int bf_bitset_size, int buffer_num_pages, int fanout, Level::Policy level_policy);
};

#endif /* SERVER_HPP */