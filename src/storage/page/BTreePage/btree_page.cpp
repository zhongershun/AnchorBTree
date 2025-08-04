#include "storage/page/BTreePage/btree_page.h"
#include <iostream>
#include "util/daset_debug_logger.h"
namespace daset{

void BTreePage::init(page_id_t page_id, IndexPageType page_type){
    page_id_ = page_id;
    next_page_id_ = DASET_INVALID_PAGE_ID;
    page_type_ = page_type;
    count_ = 0;

    // TODO : 主键和非主键索引
    is_pk_ = true;
    data_offset_ = static_cast<size_t>(DASET_PAGE_SIZE);
    free_space_ = data_offset_-(reinterpret_cast<byte*>(slot)-ptr());
}

inline auto BTreePage::ptr() -> byte*{
    return reinterpret_cast<byte*>(this);
}

inline auto BTreePage::ptr() const -> const byte*{
    return reinterpret_cast<const byte*>(this);
}

inline auto BTreePage::getKey(int slotId) -> byte*{
    return ptr()+slot[slotId].offset_;
}

inline auto BTreePage::getKey(int slotId) const -> const byte*{
    return ptr()+slot[slotId].offset_;
}

inline auto BTreePage::getKeyLen(int slotId) -> size_t{
    return slot[slotId].key_len_;
}

inline auto BTreePage::getKeyLen(int slotId) const -> const size_t{
    return slot[slotId].key_len_;
}

inline auto BTreePage::getPayload(int slotId) -> byte*{
    return ptr()+slot[slotId].offset_+slot[slotId].key_len_;
}

inline auto BTreePage::getPayload(int slotId) const -> const byte*{
    return ptr()+slot[slotId].offset_+slot[slotId].key_len_;
}

inline auto BTreePage::getPayloadLen(int slotId) -> size_t{
    return slot[slotId].payload_len_;
}

inline auto BTreePage::getPayloadLen(int slotId) const -> const size_t{
    return slot[slotId].payload_len_;
}

inline auto BTreePage::getPageId() -> page_id_t{
    return page_id_;
}

void BTreePage::setKey(int slodId, const byte* key, size_t key_len){
    memcpy(getKey(slodId),key,key_len);
}

auto BTreePage::spaceNeeded(size_t key_len, size_t payload_len) -> size_t{
    size_t res = SLOT_SIZE + key_len + payload_len;
    return res;
}

auto BTreePage::freeSpace() -> size_t{
    // #if DASET_DEBUG==true
        // auto free_space = data_offset_-(reinterpret_cast<byte*>(slot+count_)-ptr());
    // #endif //DASET_DEBUG
    // return data_offset_-(reinterpret_cast<byte*>(slot+count_)-ptr());
    return free_space_;
}

auto BTreePage::fillFactor() -> size_t{
    #if DASET_DEBUG==true
        size_t fill_factor = ((DASET_PAGE_SIZE-freeSpace())*100) / DASET_PAGE_SIZE;
    #endif //DASET_DEBUG
    return ((DASET_PAGE_SIZE-freeSpace())*100) / DASET_PAGE_SIZE;
}

auto BTreePage::canInsert(size_t key_len, size_t payload_len) -> bool{
    if(page_type_==IndexPageType::LEAF_PAGE){
        return freeSpace()>=spaceNeeded(key_len,payload_len)&&fillFactor()<DASET_SPLIT_FACTOR;
    }else{
        return freeSpace()>=spaceNeeded(key_len,sizeof(page_id_t))&&fillFactor()<DASET_SPLIT_FACTOR;
    }
}

auto BTreePage::canRemove(size_t key_len, size_t payload_len) -> bool{
    if(page_type_==IndexPageType::LEAF_PAGE){
        // size_t res = DASET_PAGE_SIZE-freeSpace()-spaceNeeded(key_len,payload_len);
        size_t fill_factor = ((DASET_PAGE_SIZE-freeSpace()-spaceNeeded(key_len,payload_len))*100) / DASET_PAGE_SIZE;
        return fill_factor>DASET_MERGE_FACTOR;
    }else{
        // size_t res = DASET_PAGE_SIZE-freeSpace()-spaceNeeded(key_len,payload_len);
        size_t fill_factor = ((DASET_PAGE_SIZE-freeSpace()-spaceNeeded(key_len,sizeof(page_id_t)))*100) / DASET_PAGE_SIZE;
        return fill_factor>DASET_MERGE_FACTOR;
    }
}

auto BTreePage::lowerBound(const byte* key, size_t key_len, const PageKeyCompator& compator) -> int{
    int lower;
    int higher;
    if(page_type_== IndexPageType::INNER_PAGE){
        lower = 1;
        higher = count_;
    }else{
        lower = 0;
        higher = count_;
    }
    while (lower<higher)
    {
        int mid = (lower + higher) >>1;
        if(compator(key,getKey(mid))<0){
            higher = mid;
        }else if(compator(key,getKey(mid))>0){
            lower = mid+1;
        }else{
            return mid;
        }
    }
    return lower;
}

auto BTreePage::lowerBound(const byte* key, size_t key_len, const PageKeyCompator& compator) const -> int{
    int lower;
    int higher;
    if(page_type_== IndexPageType::INNER_PAGE){
        lower = 1;
        higher = count_;
    }else{
        lower = 0;
        higher = count_;
    }
    while (lower<higher)
    {
        int mid = (lower + higher) >>1;
        if(compator(key,getKey(mid))<0){
            higher = mid;
        }else if(compator(key,getKey(mid))>0){
            lower = mid+1;
        }else{
            return mid;
        }
    }
    return lower;
}

auto BTreePage::search(const byte* key, size_t key_len, byte* payload, size_t payload_len, const PageKeyCompator& compator) -> bool{
    if(count_==0){
        return false;
    }
    int slotId = lowerBound(key,key_len,compator);
    if(slotId>=count_){
        if(page_type_==IndexPageType::LEAF_PAGE){
            return false;
        }else if(page_type_==IndexPageType::INNER_PAGE){
            // payload = getPayload(slotId-1);
            memcpy(payload,getPayload(slotId-1),sizeof(page_id_t));
            payload_len = getPayloadLen(slotId-1);
            return true;
        }
    }else if(compator(key,getKey(slotId))==0){
        // payload = getPayload(slotId);
        memcpy(payload,getPayload(slotId),sizeof(page_id_t));
        payload_len = getPayloadLen(slotId);
    }else{
        if(page_type_==IndexPageType::LEAF_PAGE){
            return false;
        }
        else if(page_type_==IndexPageType::INNER_PAGE){
            // payload = getPayload(slotId-1);
            memcpy(payload,getPayload(slotId-1),sizeof(page_id_t));
            payload_len = getPayloadLen(slotId-1);
            return true;
        }
    }
    return true;
}

auto BTreePage::search(const byte* key, size_t key_len, byte* payload, size_t payload_len, const PageKeyCompator& compator) const -> bool{
    if(count_==0){
        return false;
    }
    int slotId = lowerBound(key,key_len,compator);
    if(slotId>=count_){
        if(page_type_==IndexPageType::LEAF_PAGE){
            return false;
        }else if(page_type_==IndexPageType::INNER_PAGE){
            // payload = getPayload(slotId-1);
            memcpy(payload,getPayload(slotId-1),sizeof(page_id_t));
            payload_len = getPayloadLen(slotId-1);
            return true;
        }
    }else if(compator(key,getKey(slotId))==0){
        // payload = getPayload(slotId);
        memcpy(payload,getPayload(slotId),sizeof(page_id_t));
        payload_len = getPayloadLen(slotId);
    }else{
        if(page_type_==IndexPageType::LEAF_PAGE){
            return false;
        }
        else if(page_type_==IndexPageType::INNER_PAGE){
            // payload = getPayload(slotId-1);
            memcpy(payload,getPayload(slotId-1),sizeof(page_id_t));
            payload_len = getPayloadLen(slotId-1);
            return true;
        }
    }
    return true;
}

auto BTreePage::exist(const byte* key, size_t key_len, const PageKeyCompator& compator) const -> int{
    if(count_==0){
        return -1;
    }
    int slotId = lowerBound(key,key_len,compator);
    if(slotId>=count_){
        return -1;
    }else{
        return compator(key,getKey(slotId))==0?slotId:-1;
    }
}

auto BTreePage::reuseSlot(size_t key_len, size_t payload_len) -> int{
    // TODO: 目前来将在一个索引中没一行的长度是一致的(暂时不考虑而varchar)，所以slot要复用就是首先复用最靠近slot尾部的一个就行
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

auto BTreePage::insert(const byte* key, size_t key_len, const byte* payload, size_t payload_len, const PageKeyCompator& compator) -> bool{
    int slotId = 0;
    int reuse_slot_id =-1;
    if(count_!=0){
        slotId = lowerBound(key,key_len,compator);
    }
    if(is_pk_&&slotId<count_&&compator(key,getKey(slotId))==0){
        return false;
    }
    if(count_!=0){
        reuse_slot_id = reuseSlot(key_len,payload_len);
    }
    size_t offset;
    if(reuse_slot_id==-1){
        size_t space = key_len+payload_len;
        data_offset_ -= space;
        offset = data_offset_;
    }else{
        offset = slot[reuse_slot_id].offset_;
    }
    memmove(slot+slotId+1,slot+slotId,SLOT_SIZE*(count_-slotId));
    memcpy(&(slot[slotId].key_),key,DASET_PAGE_KEY_LEN);
    slot[slotId].key_len_=key_len;
    slot[slotId].payload_len_=payload_len;
    slot[slotId].offset_=offset;
    memcpy(getKey(slotId),key,key_len);
    memcpy(getPayload(slotId),payload,payload_len);
    count_+=1;
    free_space_ -= spaceNeeded(key_len,payload_len);
    return true;
}

auto BTreePage::remove(const byte* key, size_t key_len, const PageKeyCompator& compator) -> bool{
    int slotId = lowerBound(key,key_len,compator);
    if(slotId<count_&&compator(key,getKey(slotId))==0){
        Slot slot_to_remove = slot[slotId];
        memmove(slot+slotId,slot+slotId+1,SLOT_SIZE*(count_-slotId-1));
        count_--;
        slot[count_].re_use_=true;
        slot[count_].key_=slot_to_remove.key_;
        slot[count_].key_len_=slot_to_remove.key_len_;
        slot[count_].payload_len_=slot_to_remove.payload_len_;
        slot[count_].offset_=slot_to_remove.offset_;
        free_space_ += spaceNeeded(slot_to_remove.key_len_,slot_to_remove.payload_len_);
        return true;
    }
    return false;
}

auto BTreePage::remove(int slotId) -> bool{
    if(slotId>=count_){
        return false;
    }
    Slot slot_to_remove = slot[slotId];
    memmove(slot+slotId,slot+slotId+1,SLOT_SIZE*(count_-slotId-1));
    count_--;
    slot[count_].re_use_ = true;
    slot[count_].key_=slot_to_remove.key_;
    slot[count_].key_len_=slot_to_remove.key_len_;
    slot[count_].payload_len_=slot_to_remove.payload_len_;
    slot[count_].offset_=slot_to_remove.offset_;
    free_space_ += spaceNeeded(slot_to_remove.key_len_,slot_to_remove.payload_len_);
    return true;
}

auto BTreePage::removeAllSlot() ->bool{
    for (size_t i = 0; i < max_theorical_slots_capacity; i++)
    {
        slot[i].re_use_=false;
    }
    count_=0;
    data_offset_=static_cast<size_t>(DASET_PAGE_SIZE);
    free_space_=data_offset_-(reinterpret_cast<byte*>(slot)-ptr());
    return true;
}

// 将src(this)节点上的从src_slot_id开始的总共count个slot复制到dst从dst_slot_id开始的位置
void BTreePage::copyKV(BTreePage* dst, int dst_slot_id, int src_slot_id){
    size_t offset;
    size_t key_len = getKeyLen(src_slot_id);
    size_t payload_len = getPayloadLen(src_slot_id);
    if(dst->slot[dst_slot_id].re_use_){
        offset = dst->slot[dst_slot_id].offset_;
    }else{
        dst->data_offset_-=(key_len+payload_len);
        offset = dst->data_offset_;
    }
    memcpy(dst->slot+dst_slot_id, slot+src_slot_id, SLOT_SIZE);
    // 对应拷贝过去之后的偏移量需要重新计算
    dst->data_offset_-=(key_len+payload_len);
    dst->slot[dst_slot_id].offset_ = offset;
    memcpy(dst->ptr()+offset,ptr()+slot[src_slot_id].offset_,(key_len+payload_len));
    dst->count_++;
}

void BTreePage::copyKVRange(BTreePage* dst, int dst_slot_id, int src_slot_id, int count){
    for (int i = 0; i < count; i++)
    {
        // TODO: 判断reuse的大小是否足够
        size_t offset;
        size_t key_len = getKeyLen(src_slot_id+i);
        size_t payload_len = getPayloadLen(src_slot_id+i);
        if(dst->slot[dst_slot_id+i].re_use_){
            offset = dst->slot[dst_slot_id+i].offset_;
        }else{
            dst->data_offset_-=(key_len+payload_len);
            offset=dst->data_offset_;
        }
        memcpy(dst->slot+dst_slot_id+i, slot+src_slot_id+i, SLOT_SIZE);
        dst->slot[dst_slot_id+i].offset_=offset;
        memcpy(dst->ptr()+offset,ptr()+slot[src_slot_id+i].offset_,(key_len+payload_len));
        dst->count_++;
        dst->free_space_-=spaceNeeded(key_len,payload_len);
    }
}

auto BTreePage::payloadSplit(BTreePage* nodeRight) -> page_key_t{
    int slotId = count_/2;
    page_key_t res = slot[slotId].key_;
    // byte* res = getKey(slotId);
    copyKVRange(nodeRight,0,slotId,count_-slotId);
    for (int i = slotId; i < count_; i++)
    {
        slot[i].re_use_=true;
        free_space_ += spaceNeeded(slot[i].key_len_,slot[i].payload_len_);
    }
    count_=slotId;
    return res;
}

void BTreePage::MoveFirstToEndOf(BTreePage* page_other){
    int page_other_size = page_other->count_;
    copyKVRange(page_other,page_other_size,0,1);
    remove(0);
}

void BTreePage::MoveLastToFrontOf(BTreePage* page_other){
    int page_other_size = page_other->count_;
    Slot reuse_slot = page_other->slot[page_other_size];
    memmove(page_other->slot+1,page_other->slot,SLOT_SIZE*page_other_size);
    page_other->slot[0]=reuse_slot;
    copyKVRange(page_other,0,count_-1,1);
    remove(count_-1);
}

void BTreePage::nodeRepadding(){

}

auto BTreePage::valueIndex(const byte* payload) -> int{
    // inner节点专用
    if(page_type_!=IndexPageType::INNER_PAGE){
        // printf("[Error] : valueIndex not a Inner page!");
        LOG_ERROR("valueIndex not a Inner page");
        #if DASET_DEBUG
        while (true){}
        #endif
        return -1;
    }
    for (int i = 0; i < count_; i++)
    {
        if(memcmp(payload,getPayload(i),sizeof(page_id_t))==0){
            return i;
        }
    }
    return -1;
}

void BTreePage::setKeyAt(int slodId, const byte* key){
    memcpy(&(slot[slodId].key_),key,DASET_PAGE_KEY_LEN);
    memcpy(getKey(slodId),key,DASET_PAGE_KEY_LEN);
}

}