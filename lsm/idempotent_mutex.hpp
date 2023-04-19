#ifndef IDEMPOTENT_MUTEX_HPP
#define IDEMPOTENT_MUTEX_HPP
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <mutex>
#include <atomic>

class IdempotentMutex {
public:
    IdempotentMutex() // Default constructor
        : mutex_(nullptr), shared_lock_(nullptr), upgrade_lock_(nullptr), unique_lock_(nullptr),
          shared_locked_(false), unique_locked_(false) {}

    explicit IdempotentMutex(std::shared_ptr<boost::upgrade_mutex> mutex)
        : mutex_(mutex), shared_lock_(nullptr), upgrade_lock_(nullptr), unique_lock_(nullptr),
          shared_locked_(false), unique_locked_(false) {}

    ~IdempotentMutex() {
        if (unique_locked_) {
            unlock_unique();
        } else if (shared_locked_) {
            unlock_shared();
        }
    }
    void set_mutex(std::shared_ptr<boost::upgrade_mutex> mutex);
    void lock_shared();
    void unlock_shared();
    void lock_unique();
    void unlock_unique();
    void upgrade_to_unique();
private:
    std::shared_ptr<boost::upgrade_mutex> mutex_;
    std::unique_ptr<boost::shared_lock<boost::upgrade_mutex>> shared_lock_;
    std::unique_ptr<boost::upgrade_lock<boost::upgrade_mutex>> upgrade_lock_;
    std::unique_ptr<boost::unique_lock<boost::upgrade_mutex>> unique_lock_;

    std::atomic<bool> shared_locked_;
    std::atomic<bool> unique_locked_;

    std::atomic_flag lock_flag_;

};
#endif /* IDEMPOTENT_MUTEX_HPP */
