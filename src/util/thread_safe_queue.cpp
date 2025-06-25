// #include "util/thread_safe_queue.h"

// namespace daset{

// QUEUE_TEMPLATE_ARGUMENTS
// void ThreadSafeQueue<T>::Push(T&& item){
//     std::unique_lock<std::mutex> lock(mutex_);
//     queue_.push(std::move(item));
//     lock.unlock();
//     cv_.notify_all();
// }

// QUEUE_TEMPLATE_ARGUMENTS
// auto ThreadSafeQueue<T>::Pop(T& item) -> bool{
//     std::unique_lock<std::mutex> lock(mutex_);    
//     cv_.wait(lock, [this]() { 
//         return !queue_.empty() || stopped_; 
//     });
        
//     if (stopped_ && queue_.empty()) {
//         return false;
//     }
//     item = std::move(queue_.front());
//     queue_.pop();
//     return true;
// }

// QUEUE_TEMPLATE_ARGUMENTS
// void ThreadSafeQueue<T>::Stop(){
//     std::unique_lock<std::mutex> lock(mutex_);
//     stopped_ = true;
//     lock.unlock();
//     cv_.notify_all();
// }

// QUEUE_TEMPLATE_ARGUMENTS
// auto ThreadSafeQueue<T>::Empty() const -> bool{
//     std::unique_lock<std::mutex> lock(mutex_);
//     return queue_.empty();
// }

// }