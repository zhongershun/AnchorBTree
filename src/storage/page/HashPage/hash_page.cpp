#include "storage/page/HashPage/hash_page.h"

namespace daset{

void BucketPage::init(page_id_t page_id){
    page_id_ = page_id;
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

// TODO : 当前的search不考虑非主键的查询
auto BucketPage::search(const byte* key, size_t key_len, byte* payload, size_t payload_len, const PageKeyCompator& compator) -> bool{
    if(node_cnt_==0){
        return false;
    }else{
        if(compator(key,getKey(0))==0){
            memcpy(payload,getPayload(0),getPayloadLen(0));
            payload_len = getPayloadLen(0);
            return true;
        }else{
            return false;
        }
    }
    return true;
}

auto BucketPage::search(const byte* key, size_t key_len, byte* payload, size_t payload_len, const PageKeyCompator& compator) const -> bool{
    if(node_cnt_==0){
        return false;
    }else{
        if(compator(key,getKey(0))==0){
            memcpy(payload,getPayload(0),getPayloadLen(0));
            payload_len = getPayloadLen(0);
            return true;
        }else{
            return false;
        }
    }
    return true;
}

auto BucketPage::insert(const byte* key, size_t key_len, const byte* payload, size_t payload_len, const PageKeyCompator& compator) -> bool{
    if(node_cnt_!=0&&is_pk_&&compator(key,getKey(0))==0){
        return false;
    }
    int slotId = 0;
    data_offset_ -= (key_len+payload_len);
    slot[slotId].key_len_ = key_len;
    slot[slotId].payload_len_ = payload_len;
    slot[slotId].offset_ = data_offset_;
    memcpy(getKey(slotId),key,key_len);
    memcpy(getPayload(slotId),payload,payload_len);
    node_cnt_ += 1;
    free_space_ -= spaceNeeded(key_len,payload_len);
    return true;
}

auto BucketPage::remove(const byte* key, size_t key_len, const PageKeyCompator& compator) -> bool{
    if(node_cnt_==0){
        return false;
    }
    int slotId = 0;
    if(compator(key,getKey(slotId))==0){
        // HashSlot slot_to_remove = slot[slotId];
        data_offset_ += (getKeyLen(slotId)+getPayloadLen(slotId));
        free_space_ += spaceNeeded(getKeyLen(slotId),getPayloadLen(slotId));
        node_cnt_--;
        return true;
    }else{
        return false;
    }
}

}