#ifndef SERVER_HPP
#define SERVER_HPP
#include <thread>
#include <netinet/in.h>
#include "lsm_tree.hpp"

void printHelp();

class Server
{
public:
    explicit Server(int port, bool verbose);
    void createLSMTree(float bfErrorRate, int bufferNumPages, int fanout, Level::Policy levelPolicy, size_t numThreads);
    void run();
    void close();
    void listenToStdIn();
private:
    std::unique_ptr<LSMTree> lsmTree;
    std::shared_mutex sharedMtx;
    int port;
    int serverSocket;
    struct sockaddr_in serverAddress;
    void handleClient(int clientSocket);
    void handleCommand(std::stringstream& ss, int clientSocket);
    std::string printDSLHelp();
    bool verbose;
    void sendResponse(int clientSocket, const std::string &response);
    void printLSMTreeParameters(float bfErrorRate, int bufferNumPages, int fanout, Level::Policy levelPolicy, size_t numThreads);
};

#endif /* SERVER_HPP */
