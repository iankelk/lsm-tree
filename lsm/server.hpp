#ifndef SERVER_HPP
#define SERVER_HPP
#include <thread>
#include <netinet/in.h>
#include "lsm_tree.hpp"

void printHelp();

class Server
{
public:
    explicit Server(int port);
    void createLSMTree(float bf_error_rate, int buffer_num_pages, int fanout, Level::Policy level_policy);
    void run();
    void close();
    void listenToStdIn();
private:
    std::unique_ptr<LSMTree> lsmTree;
    std::shared_mutex shared_mtx;
    int port;
    int server_socket;
    struct sockaddr_in server_address;
    void handle_client(int client_socket);
    void handleCommand(std::stringstream& ss, int client_socket);
    std::string printDSLHelp();
    void sendResponse(int client_socket, const std::string &response);
    void printLSMTreeParameters(float bf_error_rate, int buffer_num_pages, int fanout, Level::Policy level_policy);
};

#endif /* SERVER_HPP */
