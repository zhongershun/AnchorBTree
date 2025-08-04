#ifndef STORAGE_BTREE_PAGE_H_
#define STORAGE_BTREE_PAGE_H_

#include "config/config.h"
#include <cstring>

namespace daset{

using page_key_t = int64_t;
#define DASET_PAGE_KEY_LEN sizeof(page_key_t)

enum class IndexPageType { INVALID_INDEX_PAGE = 0, INNER_PAGE, LEAF_PAGE};

class PageKeyCompator{
public:
    auto operator()(const byte* lhs, const byte* rhs) const -> int{
        page_key_t lhs_value;
        memcpy(&lhs_value,lhs,DASET_PAGE_KEY_LEN);
        page_key_t rhs_value;
        memcpy(&rhs_value,rhs,DASET_PAGE_KEY_LEN);
        if(lhs_value<rhs_value){
            return -1;
        }else if(lhs_value==rhs_value){
            return 0;
        }else{
            return 1;
        }
    }
};

class BTreePageHeader{
public:
    page_id_t       page_id_;
    page_id_t       next_page_id_;
    size_t          count_;
    IndexPageType   page_type_;
    bool            is_pk_;
    size_t          free_space_;
    // uint64_t        fill_factor_;
    size_t          data_offset_ = static_cast<size_t>(DASET_PAGE_SIZE);
    BTreePageHeader(){
        page_id_ = DASET_INVALID_PAGE_ID;
        next_page_id_ = DASET_INVALID_PAGE_ID;
        page_type_ = IndexPageType::INVALID_INDEX_PAGE;
        count_ = 0;
        is_pk_ = true;
    }
    BTreePageHeader(page_id_t page_id, IndexPageType page_type){
        next_page_id_ = DASET_INVALID_PAGE_ID;
        count_ = 0;
        page_id_ = page_id;
        is_pk_ = true;
        page_type_ = page_type;
    }
    BTreePageHeader(page_id_t page_id, bool is_pk, IndexPageType page_type){
        next_page_id_ = DASET_INVALID_PAGE_ID;
        count_ = 0;
        page_id_ = page_id;
        is_pk_ = is_pk;
        page_type_ = page_type;
    }
};

struct __attribute__((packed)) Slot {
    bool        re_use_{false};
    page_key_t  key_;
    size_t      offset_;
    size_t      key_len_;
    size_t      payload_len_;
};

#define SLOT_SIZE      sizeof(Slot)
#define NODE_HEADER    sizeof(BTreePageHeader)
// #define NODE_HEADER_NI (sizeof(uint64_t)+sizeof(uint64_t))
// #define NODE_HEADER_UN NODE_HEADER-NODE_HEADER_NI
#define SLOT_OFFSET    NODE_HEADER
#define DATA_OFFSET    DASET_PAGE_SIZE     //data指向node尾部，从后面来存储数据

class BTreePage : public BTreePageHeader{
    
    static constexpr size_t max_theorical_slots_capacity = (DASET_PAGE_SIZE - sizeof(BTreePageHeader)) / (SLOT_SIZE);
    static constexpr size_t left_space_to_waste = (DASET_PAGE_SIZE - sizeof(BTreePageHeader)) - max_theorical_slots_capacity*(SLOT_SIZE);
    Slot slot[max_theorical_slots_capacity] __attribute__((aligned(1)));
    byte paddingSpace[left_space_to_waste] __attribute__((aligned(1)));
public:
    BTreePage():BTreePageHeader(){};
    BTreePage(page_id_t page_id, IndexPageType page_type):BTreePageHeader(page_id,page_type){};
    BTreePage(page_id_t page_id,bool is_pk,IndexPageType page_type):BTreePageHeader(page_id,is_pk,page_type){};
    

    void init(page_id_t page_id, IndexPageType page_type_);
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

    // inline byte* ptr();
    // inline byte* getKey(uint64_t slotId);
    // inline uint64_t getKeyLen(uint64_t slotId);
    // inline byte* getPayload(uint64_t slotId);
    // inline uint64_t getPayloadLen(uint64_t slotId);
    // inline uint64_t getSlotKey(uint64_t slotId);

    // inline page_id_t nid(){ return this->page_id_; };
    inline auto getPageId() -> page_id_t;
    // void nid2byte(byte*);

    void setKey(int slodId, const byte* key, size_t key_len);
    
    auto spaceNeeded(size_t key_len, size_t payload_len) -> size_t;
    auto freeSpace() -> size_t;
    auto fillFactor() -> size_t;

    auto reuseSlot(size_t key_len, size_t payload_len) -> int;

    auto canInsert(size_t key_len, size_t payload_len) -> bool;
    auto canRemove(size_t key_len, size_t payload_len) -> bool;

    auto lowerBound(const byte* key, size_t key_len, const PageKeyCompator& compator) -> int;
    auto lowerBound(const byte* key, size_t key_len, const PageKeyCompator& compator) const -> int;

    auto search(const byte* key, size_t key_len, byte* payload, size_t payload_len, const PageKeyCompator& compator) -> bool;
    auto search(const byte* key, size_t key_len, byte* payload, size_t payload_len, const PageKeyCompator& compator) const -> bool;
    // only for leafnode
    auto exist(const byte* key, size_t key_len, const PageKeyCompator& compator) const -> int;

    // bool findInner(const byte* key, uint64_t key_len, byte*& payload, uint64_t& payload_len);
    // bool findLeaf(const byte* key, uint64_t key_len, byte*& payload, uint64_t& payload_len);
    auto insert(const byte* key, size_t key_len, const byte* payload, size_t payload_len, const PageKeyCompator& compator) -> bool;
    auto remove(const byte* key, size_t key_len, const PageKeyCompator& compator) -> bool;
    auto remove(int slotId) -> bool;

    auto valueIndex(const byte* payload) -> int;
    void setKeyAt(int slodId, const byte* key);

    // void setNodeKey(uint64_t key){
    //     node_key_ = key;
    // }

    auto removeAllSlot() -> bool;
    
    // 从本节点到目标节点的copy,不会影响当前及节点上的payload情况
    void copyKV(BTreePage* dst, int dst_slot_id, int src_slot_id);
    void copyKVRange(BTreePage* dst, int dst_slot_id, int src_slot_id, int count);

    // 本节点与右侧节点的payloadsplit,操作过后右侧节点和本节点上的count数量都会改变
    auto payloadSplit(BTreePage* pageRight) -> page_key_t;

    void nodeRepadding();

    void MoveFirstToEndOf(BTreePage* page_other);

    void MoveLastToFrontOf(BTreePage* page_other);

};

}

#endif