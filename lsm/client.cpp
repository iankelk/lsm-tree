#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <iostream>
#include <thread>
#include <mutex>
#include "data_types.hpp"
#include <sys/select.h>

#include "utils.hpp"

std::mutex mtx;
std::mutex running_mtx;

std::condition_variable cv;
bool response_received = true;
bool running = true;

std::mutex command_mtx;
std::condition_variable command_cv;
bool command_processed = true;


bool is_stdin_connected_to_file() {
    struct stat stdin_stat;
    if (fstat(fileno(stdin), &stdin_stat) < 0) {
        SyncedCerr() << "Failed to check if stdin is connected to a file" << std::endl;
        exit(1);
    }
    return S_ISREG(stdin_stat.st_mode);
}


bool get_running() {
    std::unique_lock<std::mutex> lock(running_mtx);
    return running;
}

void set_running(bool value) {
    std::unique_lock<std::mutex> lock(running_mtx);
    running = value;
}


void listenToServer(int client_socket, bool quiet) {
    char buffer[BUFFER_SIZE];
    ssize_t n_read;

    while (true) {
        std::string response;

        while ((n_read = recv(client_socket, buffer, BUFFER_SIZE - 1, 0)) > 0) {
            buffer[n_read] = '\0'; // Add a null-terminator after the received bytes
            response.append(buffer);

            if (response.size() > std::strlen(END_OF_MESSAGE) && response.substr(response.size() - std::strlen(END_OF_MESSAGE)) == END_OF_MESSAGE) {
                response.resize(response.size() - std::strlen(END_OF_MESSAGE));
                break;
            }
        }

        // Check if the client is still running before printing the error message
        if (n_read == -1 && get_running()) {
            SyncedCerr() << "Error reading response from server" << std::endl;
            close(client_socket);
            return;
        }

        // If not quiet, print the response
        if (!quiet) {
            if (response == NO_VALUE) {
                SyncedCout() << std::endl;
            } else if (response.size() > 0 && response != OK && response != SERVER_SHUTDOWN) {
                SyncedCout() << response << std::endl;
            }
        }

        if (response == SERVER_SHUTDOWN) {
            std::unique_lock<std::mutex> lock(mtx);
            running = false;
            lock.unlock();
            SyncedCout() << "Server shutdown detected. Exiting..." << std::endl;
            return;
        }
        {
            std::unique_lock<std::mutex> lock(mtx);
            response_received = true;
        }
        {
            std::unique_lock<std::mutex> lock(command_mtx);
            command_processed = true;
        }
        cv.notify_one();
        command_cv.notify_one();
    }
}


void sendCommandsToServer(int client_socket, bool quiet, bool is_stdin_file) {
    std::string command_str;

    while (true) {
        {
            std::unique_lock<std::mutex> lock(mtx);
            if (!get_running()) {
                break;
            }
        }

        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(fileno(stdin), &read_fds);
        struct timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 100000; // Check for input every 100 ms

        int select_result = select(fileno(stdin) + 1, &read_fds, NULL, NULL, &tv);

        if (select_result > 0 && FD_ISSET(fileno(stdin), &read_fds)) {
            if (std::getline(std::cin, command_str)) {
                if (command_str.size() == 0) {
                    continue;
                }

                std::unique_lock<std::mutex> lock(command_mtx);
                command_cv.wait(lock, []{ return command_processed; });

                std::unique_lock<std::mutex> response_lock(mtx);
                send(client_socket, command_str.c_str(), command_str.size(), 0);
                response_lock.unlock();

                if (command_str == "q") {
                    break;
                }
                response_received = false;
                command_processed = false;
            } else if (is_stdin_file) {
                break; // Break the loop when EOF is reached while reading from the file
            }
        }
    }
}


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
                SyncedCerr() << "Usage: " << argv[0] << " [-p port] [-q <quiet mode>]" << std::endl;
                return 1;
        }
    }

    // Connect to server
    int client_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (client_socket == -1) {
        SyncedCerr() << "Error creating client socket" << std::endl;
        return 1;
    }

    sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr);
    if (connect(client_socket, (sockaddr *)&server_address, sizeof(server_address)) == -1) {
        SyncedCerr() << "Error connecting to server" << std::endl;
        close(client_socket);
        return 1;
    }

    // Start measuring time
    auto start_time = std::chrono::high_resolution_clock::now();

    std::thread server_listener(listenToServer, client_socket, quiet);
    //server_listener.detach();

    std::thread command_sender(sendCommandsToServer, client_socket, quiet, is_stdin_connected_to_file);
    command_sender.join();

    // End measuring time, calculate the duration, and print it
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    SyncedCout() << "Processing the workload took " << duration.count() << " microseconds (" << formatMicroseconds(duration.count()) + ")" << std::endl;

    // Clean up resources
    {
        std::unique_lock<std::mutex> lock(mtx);
        running = false;
    }
    close(client_socket);
    server_listener.join(); // Add this line
    return 0;
}