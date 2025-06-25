#include "storage/disk/disk_scheduler.h"

namespace daset{

// template class ThreadSafeQueue<DiskRequest>;

DiskRequest::DiskRequest(DiskRequestType req_type, page_id_t page_id, std::promise<bool>&& promise, byte* data):
    req_type_(req_type),page_id_(page_id){
    promise_ = std::move(promise);
    data_ = data;
}

DiskScheduler::DiskScheduler(DiskManager* disk_manager,size_t schduler_workers):disk_manager_(disk_manager){
    workers_.reserve(schduler_workers);
    for (size_t i = 0; i < schduler_workers; i++)
    {
        workers_.emplace_back(std::thread(&DiskScheduler::StartWorkerThread, this));
    }
}

DiskScheduler::~DiskScheduler(){
    Shutdown();
}

void DiskScheduler::Shutdown(){
    requests_.Stop();
    for (auto& thread: workers_){
        if(thread.joinable()){
            thread.join();
        }
    }
}

void DiskScheduler::StartWorkerThread() {
    DiskRequest r; 
    while (requests_.Pop(r)) {
        runDiskRequest(std::move(r));
    }
}


void DiskScheduler::runDiskRequest(DiskRequest r){
    switch (r.req_type_)
    {
    case DiskRequestType::PAGE_READ:
        disk_manager_->ReadPage(r.page_id_,r.data_);
        break;
    case DiskRequestType::PAGE_WRITE:
        disk_manager_->WritePage(r.page_id_,r.data_);
        break;
    case DiskRequestType::PAGE_DEL:
        disk_manager_->DeletePage(r.page_id_);
        break;
    }
    r.promise_.set_value(true);
}

void DiskScheduler::Scheduler(DiskRequest r){
    requests_.Push(std::move(r));
}

auto DiskScheduler::CreatePromise() -> std::pair<std::promise<bool>,std::future<bool>>{
    std::promise<bool> promise;
    std::future<bool> future = promise.get_future();
    return {std::move(promise), std::move(future)};
}

}