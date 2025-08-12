#ifndef STORAGE_BTREE_PAGE_H_
#define STORAGE_BTREE_PAGE_H_

#include "config/config.h"
#include <cstring>
#include "util/page_key_compator.h"

namespace daset{

// TODO : 当前的实现中一个tuple对应一个page
/**
 * NOW:
 *       1 to 1
 * page --------> tuple (BucketNode)
 * 
 * TODO:
 *       1 to n
 * page --------> tuple (BucketNode)
*/

class BucketPageHeader{
public:
    page_id_t   page_id_;
    page_id_t   next_page_id_;
    size_t      node_cnt_;
    // page_key_t  key_;
    bool        is_pk_;
    size_t      free_space_;
    size_t      data_offset_ = static_cast<size_t>(DASET_PAGE_SIZE);
    BucketPageHeader(){
        page_id_ = DASET_INVALID_PAGE_ID;
        next_page_id_ = DASET_INVALID_PAGE_ID;
        node_cnt_ = 0;
        is_pk_ = true;

    }
    BucketPageHeader(page_id_t page_id){
        page_id_ = page_id;
        next_page_id_ = DASET_INVALID_PAGE_ID;
        node_cnt_ = 0;
        is_pk_ = true;
    }
};

struct __attribute__((packed)) HashSlot {
    size_t      offset_;
    size_t      key_len_;
    size_t      payload_len_;
};

#define HASH_SLOT_SIZE   sizeof(HashSlot)

class BucketPage : public BucketPageHeader{
    static constexpr size_t max_theorical_slots_capacity = (DASET_PAGE_SIZE - sizeof(BucketPageHeader)) / (HASH_SLOT_SIZE);
    static constexpr size_t left_space_to_waste = (DASET_PAGE_SIZE - sizeof(BucketPageHeader)) - max_theorical_slots_capacity*(HASH_SLOT_SIZE);
    HashSlot slot[max_theorical_slots_capacity] __attribute__((aligned(1)));
    byte paddingSpace[left_space_to_waste] __attribute__((aligned(1)));
public:
    BucketPage():BucketPageHeader(){};
    BucketPage(page_id_t page_id):BucketPageHeader(page_id){};


    void init(page_id_t page_id);
    inline auto ptr() -> byte*;
    inline auto ptr() const -> const byte*;
    inline auto getKey(int slotId) -> byte*;
    inline auto getKey(int slotId) const -> const byte*;
    inline auto getKeyLen(int slotId) -> size_t;
    inline auto getKeyLen(int slotId) const -> const size_t;
    inline auto getPayload(int slotId) -> byte*;
    inline auto getPayload(int slotId) const -> const byte*;
    inline auto getPayloadLen(int slotId) -> size_t;
    inline auto getPayloadLen(int slotId) const -> const size_t;

    inline auto getPageId() -> page_id_t;

    auto spaceNeeded(size_t key_len, size_t payload_len) -> size_t;

    auto search(const byte* key, size_t key_len, byte* payload, size_t payload_len, const PageKeyCompator& compator) -> bool;
    auto search(const byte* key, size_t key_len, byte* payload, size_t payload_len, const PageKeyCompator& compator) const -> bool;

    auto insert(const byte* key, size_t key_len, const byte* payload, size_t payload_len, const PageKeyCompator& compator) -> bool;
    auto remove(const byte* key, size_t key_len, const PageKeyCompator& compator) -> bool;
};

}

#endif