#include "buffer/lru_replacer.h"
#include <iostream>
namespace daset{

LRUReplacer::LRUNode::LRUNode(frame_id_t frame_id){
    frame_id_ = frame_id;
    timestamp_ = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    prev_=nullptr;
    next_=nullptr;
}

LRUReplacer::LRUNode::~LRUNode(){
    prev_=nullptr;
    next_=nullptr;
}

void LRUReplacer::LRUNode::UpdateTimestamp(){
    timestamp_ = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

void LRUReplacer::LRUNode::SetEvictable(bool is_evictable){
    is_evictable_ = is_evictable;
}

LRUReplacer::LRUReplacer(size_t capcity){
    capcity_=capcity;
    lru_node_storage_.clear();
    cnt_=0;
    head_=nullptr;
    tail_=nullptr;
}

LRUReplacer::~LRUReplacer(){
    lru_node_storage_.clear();
    cnt_=0;
}

auto LRUReplacer::Size() -> size_t{
    std::unique_lock<std::mutex> lock(latch_);
    return cnt_;
}

auto LRUReplacer::Evict(frame_id_t& frame_id) -> bool{
    std::unique_lock<std::mutex> lock(latch_);
    if(cnt_==0){
        printf("[Error] : Evict lru_replacer but no frame in replacer!\n");
        return false;
    }
    std::shared_ptr<LRUNode> node = tail_;
    while (node!=nullptr&&!node->is_evictable_)
    {
        node = node->prev_;
    }
    if(node==nullptr){
        printf("[Error] : Evict lru_replacer but no frame is evictable in replacer!\n");
        return false;
    }else{
        RemoveNode(node);
        frame_id = node->frame_id_;
        return true;
    }
}

void LRUReplacer::Pin(frame_id_t frame_id){
    std::unique_lock<std::mutex> lock(latch_);
    if(lru_node_storage_.find(frame_id)==lru_node_storage_.end()){
        printf("[Warning] : Pin lru_replacer but frame not in replacer!\n");
    }else{
        std::shared_ptr<LRUNode> node = lru_node_storage_[frame_id];
        if(node==nullptr){
            printf("[Error] : Pin lru_replacer node is nullptr!\n");
        }
        RemoveNode(node);
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id){
    std::unique_lock<std::mutex> lock(latch_);
    if(lru_node_storage_.find(frame_id)==lru_node_storage_.end()){
        // add a new frame to lru_replacer
        if(cnt_<capcity_){
            std::shared_ptr<LRUNode> node = std::make_shared<LRUNode>(frame_id);
            node->UpdateTimestamp();
            node->SetEvictable(true);
            MoveToFront(node);
            // lru_node_storage_[frame_id]=std::move(node);
            // cnt_++;
        }else{
            printf("[Error] : Pin lru_replacer but frame not in replacer while replacer storage is full!\n");
        }
    }else{
        std::shared_ptr<LRUNode> node = lru_node_storage_[frame_id];
        if(node==nullptr){
            printf("[Error] : Pin lru_replacer node is nullptr!\n");
        }
        node->SetEvictable(true);
    }
}

void LRUReplacer::MoveToFront(const std::shared_ptr<LRUNode>& node){
    if(head_==nullptr){
        head_=node;
        tail_=node;
    }else{
        node->next_=head_;
        head_->prev_=node;
        head_=node;
    }
    lru_node_storage_[node->frame_id_]=std::move(node);
    ++cnt_;
}

void LRUReplacer::RemoveNode(const std::shared_ptr<LRUNode>& node){
    if(head_==node&&tail_==node){
        head_=nullptr;
        tail_=nullptr;
    }else if(tail_==node){
        node->prev_->next_=nullptr;
        tail_=node->prev_;
    }else if(head_==node){
        node->next_->prev_=nullptr;
        head_=node->next_;
    }else{
        node->prev_->next_=node->next_;
        node->next_->prev_=node->prev_;
    }
    lru_node_storage_.erase(node->frame_id_);
    --cnt_;
}

}