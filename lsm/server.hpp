#ifndef SERVER_HPP
#define SERVER_HPP
#include <thread>
#include <netinet/in.h>
#include <set>
#include "lsm_tree.hpp"

void printHelp();

class Server
{
public:
    explicit Server(int port, bool verbose, size_t verboseFrequency);
    void createLSMTree(float bfErrorRate, int bufferNumPages, int fanout, Level::Policy levelPolicy, 
                       size_t numThreads, float compactionPercentage);
    void run();
    void close();
    void listenToStdIn();
    std::vector<std::unique_ptr<std::thread>> clientThreads;


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
    size_t verboseFrequency;
    void sendResponse(int clientSocket, const std::string &response);
    void printLSMTreeParameters(float bfErrorRate, int bufferNumPages, int fanout, Level::Policy levelPolicy,
                                size_t numThreads, float compactionPercentage);

    std::set<int> connectedClients;
    std::mutex connectedClientsMutex;
    std::mutex coutMutex;
};

#endif /* SERVER_HPP */
