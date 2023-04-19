#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <mutex>
#include "idempotent_mutex.hpp"

void IdempotentMutex::set_mutex(std::shared_ptr<boost::upgrade_mutex> mutex) {
    if (!mutex_ && !shared_locked_ && !unique_locked_) {
        mutex_ = mutex;
    }
}

void IdempotentMutex::lock_shared() {
    if (!shared_locked_ && mutex_ != nullptr) {
        shared_lock_ = std::make_unique<boost::shared_lock<boost::upgrade_mutex>>(*mutex_);
        shared_locked_ = true;
    }
}

void IdempotentMutex::lock_shared() {
    while (lock_flag_.test_and_set(std::memory_order_acquire)) {
        // spin
    }
    if (!shared_locked_ && mutex_ != nullptr) {
        shared_lock_ = std::make_unique<boost::shared_lock<boost::upgrade_mutex>>(*mutex_);
        shared_locked_ = true;
    }
    lock_flag_.clear(std::memory_order_release);
}

void IdempotentMutex::unlock_shared() {
    while (lock_flag_.test_and_set(std::memory_order_acquire)) {
        // spin
    }
    if (shared_locked_) {
        shared_lock_.reset();
        shared_locked_ = false;
    }
    lock_flag_.clear(std::memory_order_release);
}

void IdempotentMutex::lock_unique() {
    while (lock_flag_.test_and_set(std::memory_order_acquire)) {
        // spin
    }
    if (!unique_locked_ && mutex_ != nullptr) {
        upgrade_lock_ = std::make_unique<boost::upgrade_lock<boost::upgrade_mutex>>(*mutex_);
        unique_lock_ = std::make_unique<boost::unique_lock<boost::upgrade_mutex>>(std::move(*upgrade_lock_));
        unique_locked_ = true;
    }
    lock_flag_.clear(std::memory_order_release);
}

void IdempotentMutex::unlock_unique() {
    while (lock_flag_.test_and_set(std::memory_order_acquire)) {
        // spin
    }
    if (unique_locked_) {
        unique_lock_.reset();
        unique_locked_ = false;
    }
    lock_flag_.clear(std::memory_order_release);
}

void IdempotentMutex::upgrade_to_unique() {
    while (lock_flag_.test_and_set(std::memory_order_acquire)) {
        // spin
    }
    if (!unique_locked_ && shared_locked_ && mutex_ != nullptr) {
        upgrade_lock_ = std::make_unique<boost::upgrade_lock<boost::upgrade_mutex>>(std::move(*shared_lock_));
        unique_lock_ = std::make_unique<boost::unique_lock<boost::upgrade_mutex>>(std::move(*upgrade_lock_));
        shared_lock_.reset(); // release the shared_lock_
        shared_locked_ = false;
        unique_locked_ = true;
    }
    lock_flag_.clear(std::memory_order_release);
}


