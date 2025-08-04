#include "buffer/buffer_pool_instance.h"
#include "util/daset_debug_logger.h"
namespace daset{

FrameHeader::FrameHeader(frame_id_t frame_id)
    :frame_id_(frame_id),page_id_(DASET_INVALID_PAGE_ID){
    in_use_ = true;
    is_dirty_ = false;
    pin_cnt_.store(0);
    data_ = new byte[DASET_PAGE_SIZE];
    memset(data_,0,DASET_PAGE_SIZE);
}

FrameHeader::~FrameHeader(){
    delete[] data_;
}

void FrameHeader::Reset(){
    // in_use_ = false;
    // SetUsed(false);
    is_dirty_ = false;
    pin_cnt_.store(0);
    memset(data_,0,DASET_PAGE_SIZE);
}

auto FrameHeader::GetData() const -> const byte*{
    return data_;
}

auto FrameHeader::GetDataMut() -> byte*{
    is_dirty_ = true;
    return data_;
}

auto FrameHeader::IsDirty() -> bool{
    return is_dirty_;
}

auto FrameHeader::IsUsed() -> bool{
    return in_use_;
}

auto FrameHeader::GetPageID() -> page_id_t{
    return page_id_;
}

auto FrameHeader::GetFrameID() -> frame_id_t{
    return frame_id_;
}

void FrameHeader::SetPageID(page_id_t page_id){
    page_id_ = page_id;
}

void FrameHeader::SetUsed(bool used){
    in_use_=used;
}

auto FrameHeader::GetPin() -> size_t{
    return pin_cnt_;
}

auto FrameHeader::AddPin() -> size_t{
    pin_cnt_.fetch_add(1);
    return pin_cnt_;
}

auto FrameHeader::SubPin() -> size_t{
    pin_cnt_.fetch_sub(1);
    return pin_cnt_;
}

BufferPoolInstance::BufferPoolInstance(size_t instance_no, size_t num_frames, DiskManager* disk_manager)
    :instance_no_(instance_no), num_frames_(num_frames){
    next_page_id_.store(0);
    page_table_.clear();
    frames_.reserve(num_frames_);
    for (size_t i = 0; i < num_frames_; i++)
    {
        frames_.push_back(std::make_shared<FrameHeader>(i));
        free_frames_.push_back(i);
    }
    replacer_ = std::make_shared<LRUReplacer>(num_frames_);
    disk_scheduler_ = std::make_shared<DiskScheduler>(disk_manager);
}

auto BufferPoolInstance::NewPage() -> page_id_t{
    std::unique_lock<std::mutex> lock(latch_);
    page_id_t new_page_id = next_page_id_;
    frame_id_t frame_id;
    std::shared_ptr<FrameHeader> frame_header;
    if(free_frames_.empty()){
        // evict a frame and do read IO
        while (!replacer_->Evict(frame_id)){
            // waiting for evict;
        }
        frame_header = frames_[frame_id];
    }else{
        frame_id = free_frames_.back();
        free_frames_.pop_back();
        frame_header = frames_[frame_id];
    }
    // std::unique_lock<std::shared_mutex> frame_rw_lock(frame_header->rw_latch_);
    if(frame_header->IsDirty()){
        // Flush page/frame
        #if DASET_DEBUG==true
        page_id_t page_id = frame_header->GetPageID();
        if(page_id==DASET_INVALID_PAGE_ID){
            // printf("[Warring] : NewPage page id INVALID_PAGE_ID\n");
            LOG_WARNING("NewPage page id INVALID_PAGE_ID");
        }
        #endif
        FlushFramePage(frame_header);
    }
    frame_header->Reset();
    frame_header->SetPageID(next_page_id_);
    page_table_[new_page_id]=frame_id;
    replacer_->Unpin(frame_id);
    frame_header->SetUsed(false);
    // frame_rw_lock.unlock();
    next_page_id_.fetch_add(1);
    return new_page_id;
}

auto BufferPoolInstance::Size() const -> size_t{
    return page_table_.size();
}

auto BufferPoolInstance::WritePage(page_id_t page_id) -> WritePageGuard{
    std::unique_lock<std::mutex> lock(latch_);
    frame_id_t frame_id;
    std::shared_ptr<FrameHeader> frame_header;
    if(page_table_.find(page_id)!=page_table_.end()){
        // page in buffer pool
        frame_id = page_table_[page_id];
        frame_header = frames_[frame_id];
        if(frame_header->IsUsed()){
            // do nothing
        }else{
            replacer_->Pin(frame_id);
            frame_header->SetUsed(true);
        }
        lock.unlock();
        return WritePageGuard(page_id,frame_id,frame_header,replacer_);
    }else{
        // page not in buffer pool
        if(free_frames_.empty()){
            while (!replacer_->Evict(frame_id)){
                // waiting for evict;
            }
        }else{
            frame_id = free_frames_.back();
            free_frames_.pop_back();
        }
        frame_header = frames_[frame_id];
        if(frame_header->IsDirty()){
            // Flush page/frame
            #if DASET_DEBUG==true
            page_id_t page_id_tmp = frame_header->GetPageID();
            if(page_id_tmp==DASET_INVALID_PAGE_ID){
                // printf("[Warring] : NewPage page id INVALID_PAGE_ID\n");
                LOG_WARNING("NewPage page id INVALID_PAGE_ID");
            }
            #endif
            FlushFramePage(frame_header);
        }
        // 无论是Evict还是从free中取出来的frame都不会在replace中有这个frame
        frame_header->Reset();
        frame_header->SetPageID(page_id);
        page_table_[page_id]=frame_id;
        frame_header->SetUsed(true);
        LoadFramePage(frame_header,page_id);
        lock.unlock();
        return WritePageGuard(page_id,frame_id,frame_header,replacer_);
    }
}

auto BufferPoolInstance::ReadPage(page_id_t page_id) -> ReadPageGuard{
    std::unique_lock<std::mutex> lock(latch_);
    frame_id_t frame_id;
    std::shared_ptr<FrameHeader> frame_header;
    if(page_table_.find(page_id)!=page_table_.end()){
        // page in buffer pool
        frame_id = page_table_[page_id];
        frame_header = frames_[frame_id];
        if(frame_header->IsUsed()){
            // do nothing
        }else{
            replacer_->Pin(frame_id);
            frame_header->SetUsed(true);
        }
        lock.unlock();
        return ReadPageGuard(page_id,frame_id,frame_header,replacer_);
    }else{
        // page not in buffer pool
        if(free_frames_.empty()){
            while (!replacer_->Evict(frame_id)){
                // waiting for evict;
            }
        }else{
            frame_id = free_frames_.back();
            free_frames_.pop_back();
        }
        frame_header = frames_[frame_id];
        if(frame_header->IsDirty()){
            // Flush page/frame
            #if DASET_DEBUG==true
            page_id_t page_id_tmp = frame_header->GetPageID();
            if(page_id_tmp==DASET_INVALID_PAGE_ID){
                // printf("[Warring] : NewPage page id INVALID_PAGE_ID\n");
                LOG_WARNING("NewPage page id INVALID_PAGE_ID");
            }
            #endif
            FlushFramePage(frame_header);
        }
        frame_header->Reset();
        frame_header->SetPageID(page_id);
        page_table_[page_id]=frame_id;
        frame_header->SetUsed(true);
        LoadFramePage(frame_header,page_id);
        lock.unlock();
        return ReadPageGuard(page_id,frame_id,frame_header,replacer_);
    }
}

auto BufferPoolInstance::DeletePage(page_id_t page_id) -> bool{
    std::unique_lock<std::mutex> lock(latch_);
    frame_id_t frame_id;
    std::shared_ptr<FrameHeader> frame_header;
    if(page_table_.find(page_id)!=page_table_.end()){
        // page in buffer pool
        frame_id = page_table_[page_id];
        frame_header = frames_[frame_id];
        if(frame_header->GetPin()>0){
            LOG_ERROR("DeletePage the frame is still in used, page_id : "+std::to_string(page_id)+", frame_id : "+std::to_string(frame_id));
            #if DASET_DEBUG
            while (true){}
            #endif
            return false;
        }else{
            FlushFramePage(frame_header);
            replacer_->Unpin(frame_id);
            frame_header->SetUsed(false);
            free_frames_.push_back(frame_id);
        }
    }
    FreeDiskPage(page_id);
    return true;
}

auto BufferPoolInstance::GetPinCount(page_id_t page_id) -> size_t{
    std::unique_lock<std::mutex> lock(latch_);
    if(page_table_.find(page_id)!=page_table_.end()){
        frame_id_t frame_id = page_table_[page_id];
        auto frame_header = frames_[frame_id];
        return frame_header->GetPin();
    }else{
        // printf("[Warring] : GetPinCount page is not in buffer pool\n");
        LOG_WARNING("GetPinCount page is not in buffer pool, page_id : "+std::to_string(page_id));
        return 0;
    }
}

/// @brief 外部调用:将一个page进行刷操作(异步)
/// @param page_id 
/// @return 
auto BufferPoolInstance::FlushPage(page_id_t page_id) -> bool {
    std::unique_lock<std::mutex> lock(latch_);
    if(page_table_.find(page_id)==page_table_.end()){
        // printf("[Warring] : FlushPage page is not in buffer pool\n");
        LOG_WARNING("FlushPage page is not in buffer pool, page_id : "+std::to_string(page_id));
        return false;
    }else{
        frame_id_t frame_id = page_table_[page_id];
        auto frame_header = frames_[frame_id];
        replacer_->Unpin(frame_id);
        frame_header->SetUsed(false);
        auto promise_future = disk_scheduler_->CreatePromise();
        disk_scheduler_->Scheduler(DiskRequest(PAGE_WRITE,page_id,std::move(promise_future.first),frame_header->GetDataMut()));   
        return true;
    }
}

/// @brief 外部调用:将所有的page进行刷操作(异步)
/// @return 
auto BufferPoolInstance::FlushAllPage() -> bool {
    for (auto it : page_table_)
    {
        page_id_t page_id = it.first;
        frame_id_t frame_id = it.second;
        auto frame_header = frames_[frame_id];
        replacer_->Unpin(frame_id);
        frame_header->SetUsed(false);
        auto promise_future = disk_scheduler_->CreatePromise();
        disk_scheduler_->Scheduler(DiskRequest(PAGE_WRITE,page_id,std::move(promise_future.first),frame_header->GetDataMut()));   
    }
    return true;
}

/// @brief 内部调用:将一个dirty的frame中的内容刷入磁盘（同步）
/// @param frame 
/// @return 
auto BufferPoolInstance::FlushFramePage(std::shared_ptr<FrameHeader> frame) -> bool {
    // #if DASET_DEBUG == true
    // assert(frame->IsDirty()||frame->IsUsed());
    // assert(frame->IsUsed()==true);
    // #endif
    page_id_t page_id = frame->GetPageID();
    if(frame->IsDirty()){
        auto promise_future = disk_scheduler_->CreatePromise();
        auto future = std::move(promise_future.second);
        disk_scheduler_->Scheduler(DiskRequest(PAGE_WRITE,page_id,std::move(promise_future.first),frame->GetDataMut()));
        future.wait();
    }
    page_table_.erase(page_id);
    frame->Reset();
    return true;
}

/// @brief 内部调用:将一个page载入到frame中存储（同步）
/// @param frame 
/// @param page_id 
/// @return 
auto BufferPoolInstance::LoadFramePage(std::shared_ptr<FrameHeader> frame, page_id_t page_id) -> bool {
    auto promise_future = disk_scheduler_->CreatePromise();
    auto future = std::move(promise_future.second);
    disk_scheduler_->Scheduler(DiskRequest(PAGE_READ,page_id,std::move(promise_future.first),frame->GetDataMut()));   
    future.wait();
    return true;
}

/// @brief 内部调用:将一个page从disk上删除了（同步）
/// @param page_id 
/// @return 
auto BufferPoolInstance::FreeDiskPage(page_id_t page_id) ->bool{
    auto promise_future = disk_scheduler_->CreatePromise();
    auto future = std::move(promise_future.second);
    disk_scheduler_->Scheduler(DiskRequest(PAGE_DEL,page_id,std::move(promise_future.first),nullptr));   
    future.wait();
    return true;
}

}