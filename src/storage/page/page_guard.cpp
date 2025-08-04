#include "buffer/buffer_pool_instance.h"

namespace daset{

PageGuard::PageGuard(page_id_t page_id, frame_id_t frame_id,
    std::shared_ptr<FrameHeader> frame, std::shared_ptr<LRUReplacer> replacer)
    :page_id_(page_id),frame_id_(frame_id),frame_(std::move(frame)),replacer_(replacer){
    is_valid_ = true;
    // replacer_->Pin(frame_id_);
    frame_->AddPin();
}

void ReadPageGuard::Drop(){
    if(!is_valid_){
        return;
    }else{
        if(frame_->SubPin()==0){ // pin equals to 0, release the lock
            replacer_->Unpin(frame_id_);
            frame_->SetUsed(false);
        }
    }
    is_valid_=false;
    guard_latch_.unlock();
}

void WritePageGuard::Drop(){
    if(!is_valid_){
        return;
    }else{
        if(frame_->SubPin()==0){ // pin equals to 0, release the lock
            replacer_->Unpin(frame_id_);
            frame_->SetUsed(false);
        }
    }
    is_valid_=false;
    guard_latch_.unlock();
}

auto PageGuard::GetData() const -> const byte*{
    return frame_->GetData();
}

auto PageGuard::GetDataMut() -> byte*{
    return frame_->GetDataMut();
}

WritePageGuard::WritePageGuard(page_id_t page_id, frame_id_t frame_id,
    std::shared_ptr<FrameHeader> frame, std::shared_ptr<LRUReplacer> replacer)
    :PageGuard(page_id,frame_id,frame,replacer),guard_latch_(frame->rw_latch_){
    // frame_->AddPin();
}

WritePageGuard::~WritePageGuard(){
    Drop();
}

WritePageGuard::WritePageGuard(WritePageGuard &&other){
    page_id_=std::move(other.page_id_);
    frame_id_=std::move(other.frame_id_);
    frame_=std::move(other.frame_);
    replacer_=std::move(other.replacer_);
    is_valid_=std::move(other.is_valid_);
    guard_latch_=std::move(other.guard_latch_);
    other.is_valid_=false;
}

auto WritePageGuard::operator=(WritePageGuard &&other) -> WritePageGuard &{
    if(this!=&other){
        this->Drop();
        page_id_=std::move(other.page_id_);
        frame_id_=std::move(other.frame_id_); 
        frame_=std::move(other.frame_);
        replacer_=std::move(other.replacer_);
        is_valid_=std::move(other.is_valid_);
        guard_latch_=std::move(other.guard_latch_);
        other.is_valid_=false;
    }
    return *this; 
}

ReadPageGuard::ReadPageGuard(page_id_t page_id, frame_id_t frame_id,
    std::shared_ptr<FrameHeader> frame, std::shared_ptr<LRUReplacer> replacer)
    :PageGuard(page_id,frame_id,frame,replacer),guard_latch_(frame->rw_latch_){
    // frame_->AddPin();
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&other){
    page_id_=std::move(other.page_id_);
    frame_id_=std::move(other.frame_id_);
    frame_=std::move(other.frame_);
    replacer_=std::move(other.replacer_);
    is_valid_=std::move(other.is_valid_);
    guard_latch_=std::move(other.guard_latch_);
    other.is_valid_=false;
}

ReadPageGuard::~ReadPageGuard(){
    Drop();
}

auto ReadPageGuard::operator=(ReadPageGuard &&other) -> ReadPageGuard &{
    if(this!=&other){
        this->Drop();
        page_id_=std::move(other.page_id_);
        frame_id_=std::move(other.frame_id_); 
        frame_=std::move(other.frame_);
        replacer_=std::move(other.replacer_);
        is_valid_=std::move(other.is_valid_);
        guard_latch_=std::move(other.guard_latch_);
        other.is_valid_=false;
    }
    return *this; 
}

auto PageGuard::GetPageID() const -> page_id_t{
    return page_id_;
}

auto PageGuard::GetFrameID() const -> frame_id_t{
    return frame_id_;
}

}