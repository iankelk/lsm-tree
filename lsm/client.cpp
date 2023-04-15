#include <unistd.h>
#include <arpa/inet.h>
#include <iostream>
#include "data_types.hpp"
#include "utils.hpp"

int main(int argc, char *argv[]) {
    int opt, port = DEFAULT_SERVER_PORT;
    bool quiet = false;

    while ((opt = getopt(argc, argv, "p:q")) != -1) {
        switch (opt) {
            case 'p':
                port = std::stoi(optarg);
                break;
            case 'q':
                quiet = true;
                break;
            default:
                std::cerr << "Usage: " << argv[0] << " [-p port] [-q <quiet mode>]" << std::endl;
                return 1;
        }
    }

    // Connect to server
    int client_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (client_socket == -1) {
        std::cerr << "Error creating client socket" << std::endl;
        return 1;
    }

    sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(port);
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

        if (command_str == "q") {
            break;
        }

        std::string response;
        bool server_shutdown = false;
        while ((n_read = recv(client_socket, buffer, BUFFER_SIZE, 0)) > 0) {
            response.append(buffer, n_read);
            if (response.size() > std::strlen(END_OF_MESSAGE) && response.substr(response.size() - std::strlen(END_OF_MESSAGE)) == END_OF_MESSAGE) {
                response.resize(response.size() - std::strlen(END_OF_MESSAGE));
                break;
            }
        }

        // If recv() returns 0, it means the server has disconnected
        if (n_read == 0) {
            server_shutdown = true;
            std::cerr << "Server disconnected" << std::endl;
        } else if (n_read == -1) {
            std::cerr << "Error reading response from server" << std::endl;
            close(client_socket);
            return 1;
        }

        if (response == SERVER_SHUTDOWN) {
            server_shutdown = true;
            std::cout << "Server is shutting down" << std::endl;
        }

        if (!quiet && !server_shutdown) {
            if (response == NO_VALUE) {
                std::cout << std::endl;
            } else if (response.size() > 0 && response != OK) {
                std::cout << response << std::endl;
            }
        }

        if (server_shutdown) {
            break;
        }
    }


    // End measuring time, calculate the duration, and print it
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "Processing the workload took " << duration.count() << " microseconds (" << formatMicroseconds(duration.count()) + ")" << std::endl;

    // Clean up resources
    close(client_socket);
    return 0;
}
