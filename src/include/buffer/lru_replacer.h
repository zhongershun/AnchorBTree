#ifndef BUFFER_LRU_REPLACER_H_
#define BUFFER_LRU_REPLACER_H_

#include "buffer/replacer.h"
#include <unordered_map>
#include <chrono>
#include <memory>
#include <mutex>

namespace daset{

class LRUReplacer : public Replacer{
public:
    LRUReplacer(size_t capcity);
    ~LRUReplacer();

    auto Evict(frame_id_t &frame_id) -> bool override;
    void Pin(frame_id_t frame_id) override;
    void Unpin(frame_id_t frame_id) override;
    auto Size() -> size_t override;

private:
    class LRUNode{
    public:
        LRUNode(frame_id_t frame_id);
        ~LRUNode();
        void UpdateTimestamp();
        void SetEvictable(bool is_evictable);
        
        std::shared_ptr<LRUNode> prev_;
        std::shared_ptr<LRUNode> next_;

        frame_id_t frame_id_;
        bool is_evictable_{false};
        uint64_t timestamp_;
    };
    void AddToFront(const std::shared_ptr<LRUNode>& node);
    void RemoveNode(const std::shared_ptr<LRUNode>& node);
    void MoveToFront(const std::shared_ptr<LRUNode>& node);

    size_t cnt_{0};
    size_t capcity_;
    std::mutex latch_;
    std::unordered_map<frame_id_t, std::shared_ptr<LRUNode>> lru_node_storage_;
    std::shared_ptr<LRUNode> head_;
    std::shared_ptr<LRUNode> tail_;
};

}

#endif