#ifndef UTILS_HPP
#define UTILS_HPP
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>

// Declarations of error and string handling functions
void die(const std::string& message);
std::string formatMicroseconds(size_t microseconds);
std::string addCommas(std::string s);
void printTrace();

class SyncedCout {
public:
    template <typename T>
    SyncedCout& operator<<(const T& data) {
        std::lock_guard<std::mutex> lock(_coutMutex);
        std::cout << data;
        return *this;
    }

    SyncedCout& operator<<(std::ostream& (*os)(std::ostream&)) {
        std::lock_guard<std::mutex> lock(_coutMutex);
        std::cout << os;
        return *this;
    }

private:
    static std::mutex _coutMutex;
};

class SyncedCerr {
public:
    template <typename T>
    SyncedCerr& operator<<(const T& data) {
        std::lock_guard<std::mutex> lock(_cerrMutex);
        std::cerr << data;
        return *this;
    }

    SyncedCerr& operator<<(std::ostream& (*os)(std::ostream&)) {
        std::lock_guard<std::mutex> lock(_cerrMutex);
        std::cerr << os;
        return *this;
    }

private:
    static std::mutex _cerrMutex;
};

#endif /* UTILS_HPP */