#ifndef UTIL_THREAD_SAFE_QUEUE_H_
#define UTIL_THREAD_SAFE_QUEUE_H_

#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <queue>
#include <atomic>
#include <condition_variable>

namespace daset{

#define QUEUE_TEMPLATE_ARGUMENTS template <typename T>

QUEUE_TEMPLATE_ARGUMENTS
class ThreadSafeQueue {
public:
    ThreadSafeQueue() = default;
    ~ThreadSafeQueue() = default;
    void Push(T&& item){
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.push(std::move(item));
        lock.unlock();
        cv_.notify_all();
    }

    auto Pop(T& item) -> bool{
        std::unique_lock<std::mutex> lock(mutex_);    
        cv_.wait(lock, [this]() { 
            return !queue_.empty() || stopped_; 
        });
        
        if (stopped_ && queue_.empty()) {
            return false;
        }
        item = std::move(queue_.front());
        queue_.pop();
        return true;
    }
    void Stop(){
        std::unique_lock<std::mutex> lock(mutex_);
        stopped_ = true;
        lock.unlock();
        cv_.notify_all();
    }
    auto Empty() const -> bool{
        std::unique_lock<std::mutex> lock(mutex_);
        return queue_.empty();
    }

private:
    mutable std::mutex mutex_;          
    std::condition_variable cv_;      
    std::queue<T> queue_;
    std::atomic<bool> stopped_{false};
};

}

#endif