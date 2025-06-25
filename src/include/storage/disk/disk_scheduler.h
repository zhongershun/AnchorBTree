#ifndef STORAGE_DISK_SCHEDULER_H_
#define STORAGE_DISK_SCHEDULER_H_

#include "storage/disk/disk_manager.h"
#include "util/thread_safe_queue.h"
// #include "util/thread_safe_queue.cpp"
#include "config/config.h"
#include <future>

namespace daset{

enum DiskRequestType { PAGE_READ=0, PAGE_WRITE, PAGE_DEL};

class DiskRequest{
public:
    DiskRequest(DiskRequestType req_type, page_id_t page_id, std::promise<bool>&& promise, byte* data);
    DiskRequest() = default;
    DiskRequestType req_type_;
    page_id_t page_id_;
    std::promise<bool> promise_;
    byte* data_;
};

class DiskScheduler{
public:
    DiskScheduler(DiskManager* disk_manager, size_t schduler_workers = 4);
    ~DiskScheduler();

    void Scheduler(DiskRequest r);
    void Shutdown();
    void runDiskRequest(DiskRequest r);
    void StartWorkerThread();
    auto CreatePromise() -> std::pair<std::promise<bool>,std::future<bool>>;

private:
    DiskManager* disk_manager_;
    ThreadSafeQueue<DiskRequest> requests_;
    std::vector<std::thread> workers_;
    // std::thread worker_thread_;
};

}

#endif