#include "storage/page/HashPage/hash_page.h"

namespace daset{

void BucketPage::init(page_id_t page_id){
    page_id_ = page_id;
    right_page_id_ = DASET_INVALID_PAGE_ID;
    next_page_id_ = DASET_INVALID_PAGE_ID;
    count_ = 0;
    data_offset_ = static_cast<size_t>(DASET_PAGE_SIZE);
    free_space_ = data_offset_-(reinterpret_cast<byte*>(slot)-ptr());
}

void BucketPage::reset(){
    count_ = 0;
    data_offset_ = static_cast<size_t>(DASET_PAGE_SIZE);
    free_space_ = data_offset_-(reinterpret_cast<byte*>(slot)-ptr());
}

inline auto BucketPage::ptr() -> byte*{
    return reinterpret_cast<byte*>(this);
}

inline auto BucketPage::ptr() const -> const byte*{
    return reinterpret_cast<const byte*>(this);
}

inline auto BucketPage::getKey(int slotId) -> byte*{
    return ptr()+slot[slotId].offset_;
}

inline auto BucketPage::getKey(int slotId) const -> const byte*{
    return ptr()+slot[slotId].offset_;
}

inline auto BucketPage::getKeyLen(int slotId) -> size_t{
    return slot[slotId].key_len_;
}

inline auto BucketPage::getKeyLen(int slotId) const -> const size_t{
    return slot[slotId].key_len_;
}

inline auto BucketPage::getPayload(int slotId) -> byte*{
    return ptr()+slot[slotId].offset_+slot[slotId].key_len_;
}

inline auto BucketPage::getPayload(int slotId) const -> const byte*{
    return ptr()+slot[slotId].offset_+slot[slotId].key_len_;
}

inline auto BucketPage::getPayloadLen(int slotId) -> size_t{
    return slot[slotId].payload_len_;
}

inline auto BucketPage::getPayloadLen(int slotId) const -> const size_t{
    return slot[slotId].payload_len_;
}

inline auto BucketPage::getPageId() -> page_id_t{
    return page_id_;
}

auto BucketPage::spaceNeeded(size_t key_len, size_t payload_len) -> size_t{
    size_t res = HASH_SLOT_SIZE + key_len + payload_len;
    return res;
}

auto BucketPage::reuseSlot(size_t key_len, size_t payload_len) -> int{
    // TODO: 目前来将在一个索引中没一行的长度是一致的(暂时不考虑而varchar)，所以slot要复用就是首先复用最靠近slot尾部的一个就行
    if(count_==0){
        return -1;
    }
    size_t start = count_;
    while ((reinterpret_cast<byte*>(slot+start)-ptr())<data_offset_&&slot[start].re_use_) //实际上第一个就会复用
    {
        if(slot[start].key_len_>=key_len&&slot[start].payload_len_>=payload_len){
            return start;
        }
        start++;
    }
    return -1;
}

auto BucketPage::canInsert(size_t key_len, size_t payload_len) -> bool{
    return free_space_>=spaceNeeded(key_len,payload_len);
}

// TODO : 当前的search不考虑非主键的查询
auto BucketPage::search(const byte* key, size_t key_len, byte* payload, size_t payload_len, const PageKeyCompator& compator) -> bool{
    if(count_==0){
        return false;
    }
    for (size_t i = 0; i < count_; i++)
    {
        if(compator(key,getKey(i))==0){
            memcpy(payload,getPayload(i),getPayloadLen(i));
            payload_len = getPayloadLen(i);
            return true;
        }
    }
    return false;
}

auto BucketPage::search(const byte* key, size_t key_len, byte* payload, size_t payload_len, const PageKeyCompator& compator) const -> bool{
    if(count_==0){
        return false;
    }
    for (size_t i = 0; i < count_; i++)
    {
        if(compator(key,getKey(i))==0){
            memcpy(payload,getPayload(i),getPayloadLen(i));
            payload_len = getPayloadLen(i);
            return true;
        }
    }
    return false;
}

auto BucketPage::insert(const byte* key, size_t key_len, const byte* payload, size_t payload_len, const PageKeyCompator& compator) -> bool{
    // TODO : 此处没有考虑逐渐的node的插入
    // if(!canInsert(key_len,payload_len)){
    //     return false;
    // }
    int reuse_slot_id = reuseSlot(key_len,payload_len);
    size_t offset;
    if(reuse_slot_id==-1){
        size_t space = key_len+payload_len;
        data_offset_ -= space;
        offset = data_offset_;
    }else{
        offset = slot[reuse_slot_id].offset_;
    }
    // reuse_slot_id = count_;
    // memmove(slot++1,slot+slotId,SLOT_SIZE*(count_-slotId));
    // memcpy(&(slot[count_].key_),key,DASET_PAGE_KEY_LEN);
    slot[count_].key_len_=key_len;
    slot[count_].payload_len_=payload_len;
    slot[count_].offset_=offset;
    memcpy(getKey(count_),key,key_len);
    memcpy(getPayload(count_),payload,payload_len);
    count_+=1;
    free_space_ -= spaceNeeded(key_len,payload_len);
    return true;
}

auto BucketPage::remove(const byte* key, size_t key_len, const PageKeyCompator& compator) -> bool{
    if(count_==0){
        return false;
    }
    for (size_t i = 0; i < count_; i++)
    {
        if(compator(key,getKey(i))==0){
            HashSlot slot_to_remove = slot[i];
            memmove(slot+i,slot+i+1,HASH_SLOT_SIZE*(count_-i-1));
            count_--;
            slot[count_].re_use_=true;
            slot[count_].key_len_=slot_to_remove.key_len_;
            slot[count_].payload_len_=slot_to_remove.payload_len_;
            slot[count_].offset_=slot_to_remove.offset_;
            free_space_ += spaceNeeded(slot_to_remove.key_len_,slot_to_remove.payload_len_);
            if(count_==0){
                reset();
            }
            return true;
        }
    }
    return false;
}

}