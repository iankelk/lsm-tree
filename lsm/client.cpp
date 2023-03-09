#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

int main() {
    // Connect to server
    int client_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (client_socket == -1) {
        std::cerr << "Error creating client socket" << std::endl;
        return 1;
    }

    sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(1234);
    inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr);
    if (connect(client_socket, (sockaddr *)&server_address, sizeof(server_address)) == -1) {
        std::cerr << "Error connecting to server" << std::endl;
        close(client_socket);
        return 1;
    }

    // Send commands to server
    std::string command_str;
    char buffer[1024];
    ssize_t n_read;

     // Start measuring time
    auto start_time = std::chrono::high_resolution_clock::now();

    while (std::getline(std::cin, command_str)) {
        send(client_socket, command_str.c_str(), command_str.size(), 0);

        // Receive responses from server
        std::stringstream response_ss;
        while ((n_read = recv(client_socket, buffer, sizeof(buffer), 0)) > 0) {
            // Append received data to response stringstream
            response_ss << std::string(buffer, n_read);
        
            // Check if the response is complete
            if (response_ss.str().back() == '\n') {
                // Parse response and clear buffer
                std::string response_str = response_ss.str();
                std::istringstream iss(response_str);
                std::string token;
                while (std::getline(iss, token)) {
                    std::cout << token << std::endl;
                }
                response_ss.str("");
                memset(buffer, 0, sizeof(buffer));
                break;
            }
        }
        if (n_read == -1) {
            std::cerr << "Error receiving data from server" << std::endl;
            break;
        }
    }

    // End measuring time
    auto end_time = std::chrono::high_resolution_clock::now();
    // Calculate duration
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
     // Print duration
    std::cout << "Processing the workload took " << duration.count() << " microseconds" << std::endl;

    // Clean up resources
    close(client_socket);
    return 0;
}
