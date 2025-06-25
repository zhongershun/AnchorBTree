#ifndef STORAGE_PAGE_GUARD_H_
#define STORAGE_PAGE_GUARD_H_

#include <memory>
#include <mutex>
#include <shared_mutex>
#include "config/config.h"
#include "buffer/buffer_pool_instance.h"

namespace daset{

class FrameHeader;

class PageGuard{
public:
    PageGuard() = default;
    PageGuard(page_id_t page_id, frame_id_t frame_id,
        std::shared_ptr<FrameHeader> frame, std::shared_ptr<LRUReplacer> replacer);
    auto GetPageID() const -> page_id_t;
    auto GetFrameID() const -> frame_id_t;

    ~PageGuard() = default;
    auto GetData() const -> const byte*;
    auto GetDataMut() -> byte*;
    bool is_valid_{false}; 
    page_id_t page_id_;
    frame_id_t frame_id_;
    std::shared_ptr<FrameHeader> frame_;
    std::shared_ptr<LRUReplacer> replacer_;
};

class WritePageGuard : public PageGuard{
public:
    WritePageGuard(page_id_t page_id, frame_id_t frame_id,
        std::shared_ptr<FrameHeader> frame, std::shared_ptr<LRUReplacer> replacer);
    WritePageGuard() = default;
    ~WritePageGuard();
    void Drop();
    WritePageGuard(const WritePageGuard &) = delete;
    auto operator=(const WritePageGuard &) -> WritePageGuard & = delete;
    WritePageGuard(WritePageGuard &&other);
    auto operator=(WritePageGuard &&other) -> WritePageGuard &;
    // auto GetData() const -> const byte*;
    template<typename T>
    auto As() const -> const T*{
        return reinterpret_cast<const T*>(GetData());
    }
    // auto GetDataMut() -> byte*;
    template<typename T>
    auto AsMut() -> T*{
        return reinterpret_cast<T*>(GetDataMut());
    }
private:
    std::unique_lock<std::shared_mutex> guard_latch_;
};

class ReadPageGuard : public PageGuard{
public:
    ReadPageGuard(page_id_t page_id, frame_id_t frame_id,
        std::shared_ptr<FrameHeader> frame, std::shared_ptr<LRUReplacer> replacer);
    ReadPageGuard() = default;
    ~ReadPageGuard();
    void Drop();
    ReadPageGuard(const ReadPageGuard &) = delete;
    auto operator=(const ReadPageGuard &) -> ReadPageGuard & = delete;
    ReadPageGuard(ReadPageGuard &&other);
    auto operator=(ReadPageGuard &&other) -> ReadPageGuard &;
    // auto GetData() const -> const byte*;
    template<typename T>
    auto As() const -> const T*{
        return reinterpret_cast<const T*>(GetData());
    }
private:
    std::shared_lock<std::shared_mutex> guard_latch_;

};

}

#endif