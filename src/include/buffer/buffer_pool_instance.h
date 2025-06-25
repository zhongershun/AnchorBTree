#ifndef BUFFER_POOL_INSTANCE_H_
#define BUFFER_POOL_INSTANCE_H_


#include "config/config.h"
#include "storage/disk/disk_scheduler.h"
#include "buffer/lru_replacer.h"
#include "storage/page/page_guard.h"
#include <shared_mutex>
#include <cassert>

namespace daset{

class FrameHeader{
public:
    FrameHeader(frame_id_t);
    ~FrameHeader();

    void Reset();
    auto GetData() const -> const byte*;
    auto GetDataMut() -> byte*;
    auto IsDirty() -> bool;
    auto IsUsed() -> bool;

    auto GetPageID() -> page_id_t;
    auto GetFrameID() -> frame_id_t;
    void SetPageID(page_id_t page_id);
    void SetUsed();
    auto GetPin() -> size_t;

    auto AddPin() -> size_t;
    auto SubPin() -> size_t;
    std::shared_mutex rw_latch_;
private:
    const frame_id_t frame_id_;
    bool in_use_;
    page_id_t page_id_;
    bool is_dirty_;
    std::atomic<size_t> pin_cnt_;
    byte* data_; 
};

class BufferPoolInstance{
public:
    BufferPoolInstance(size_t instance_no, size_t num_frames, DiskManager* disk_manager);
    ~BufferPoolInstance() = default;

    auto Size() const -> size_t;
    auto NewPage() -> page_id_t;
    auto DeletePage(page_id_t page_id) -> bool;
    auto FlushPage(page_id_t page_id) -> bool;
    auto FlushAllPage() -> bool;

    auto WritePage(page_id_t page_id) -> WritePageGuard;
    auto ReadPage(page_id_t page_id) -> ReadPageGuard;

    auto GetPinCount(page_id_t page_id) -> size_t;

private:
    auto FlushFramePage(std::shared_ptr<FrameHeader> frame) -> bool;
    auto LoadFramePage(std::shared_ptr<FrameHeader> frame, page_id_t page_id) -> bool;
    auto FreeDiskPage(page_id_t page_id) -> bool;
    // TODO : 为一个buffer pool instance 添加一个AHI (page_id -> instance_no)
    const size_t instance_no_;
    const size_t num_frames_;

    std::atomic<page_id_t> next_page_id_;
    std::mutex latch_;

    std::unordered_map<page_id_t, frame_id_t> page_table_;
    // 存储空闲frame在frame_中存储的偏移
    std::vector<std::shared_ptr<FrameHeader>> frames_;
    std::list<size_t> free_frames_;
    // frame_id : 0 - (num_frames-1)
    

    std::shared_ptr<LRUReplacer> replacer_;
    std::shared_ptr<DiskScheduler> disk_scheduler_;
};

}


#endif