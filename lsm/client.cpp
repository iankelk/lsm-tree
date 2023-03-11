#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "data_types.hpp"

int main() {
    // Connect to server
    int client_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (client_socket == -1) {
        std::cerr << "Error creating client socket" << std::endl;
        return 1;
    }

    sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr);
    if (connect(client_socket, (sockaddr *)&server_address, sizeof(server_address)) == -1) {
        std::cerr << "Error connecting to server" << std::endl;
        close(client_socket);
        return 1;
    }

    // Send commands to server
    std::string command_str;
    char buffer[BUFFER_SIZE];
    ssize_t n_read;

     // Start measuring time
    auto start_time = std::chrono::high_resolution_clock::now();

    while (std::getline(std::cin, command_str)) {
        // If the command_str is empty, skip it
        if (command_str.size() == 0) {
            continue;
        }
        send(client_socket, command_str.c_str(), command_str.size(), 0);

        // Read the response from the server in chunks of BUFFER_SIZE in a loop until END_OF_MESSAGE is reached
        // The end of the message is marked with the END_OF_MESSAGE indicator.
        std::string response;
        while ((n_read = recv(client_socket, buffer, BUFFER_SIZE, 0)) > 0) {
            buffer[n_read] = '\0';
            response += buffer;
            if (response.find(END_OF_MESSAGE) != std::string::npos) {
                break;
            }
        }

        if (n_read == -1) {
            std::cerr << "Error reading response from server" << std::endl;
            close(client_socket);
            return 1;
        }
        // If response is not empty and does not begin with an 'o' (for OK), print it
        if (response.size() > 0 && response[0] != 'o') {
            std::cout << response << std::endl;
        }
    }

    // End measuring time, calculate the duration, and print it
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "Processing the workload took " << duration.count() << " microseconds" << std::endl;

    // Clean up resources
    close(client_socket);
    return 0;
}
