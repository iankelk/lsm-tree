#pragma once
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <functional>
#include <future>

class ThreadPool {
public:
    explicit ThreadPool(size_t numThreads);
    ~ThreadPool();

    template<class F>
    auto enqueue(F&& f) -> std::future<typename std::invoke_result<F>::type>;

    size_t getNumThreads() { return workers.size(); }
    void waitForAllTasks();
private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;

    std::mutex tasksMutex;
    std::condition_variable condition;
    std::atomic<std::size_t> activeTasks;
    bool stop;
};

template<class F>
auto ThreadPool::enqueue(F&& f) -> std::future<typename std::invoke_result<F>::type> {
    using return_type = typename std::invoke_result<F>::type;
    auto task = std::make_shared<std::packaged_task<return_type()>>(std::forward<F>(f));
    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock(tasksMutex);
        tasks.emplace([task, this]() { 
            (*task)();
            --activeTasks;
            condition.notify_all();
        });
        ++activeTasks;
    }
    condition.notify_one();
    return res;
}
