#ifndef ANCHOR_NODE_H_
#define ANCHOR_NODE_H_

#include <cstring>
#include <cstdlib>
#include <mutex>
#include <cstddef>
#include <vector>
#include "util/db_rw_lock.h"
#include "system/config.h"
#include <cassert>

namespace anchorBTree{

typedef uint64_t bid_t; //每一个节点的id号

#define NID_NIL             ((bid_t)0)
#define NID_SCHEMA          ((bid_t)1)
#define NID_START           (NID_NIL + 2)
#define NID_LEAF_START      (bid_t)((1LL << 48) + 1)  // leaf node的id从2^48+1开始直到2^64-1
#define IS_LEAF(nid)        ((nid) >= NID_LEAF_START)

struct __attribute__((packed)) Slot {
    uint64_t key;
    uint64_t offset;
    // IndexKey key;
    uint64_t key_len;
    uint64_t payload_len;
};

#define SLOT_SIZE      sizeof(anchorBTree::Slot)

typedef unsigned char Byte;

inline int cmpKey(const Byte* a, const uint64_t a_len, const Byte* b, const uint64_t b_len);

class AnchorNodeHeader{
public:
    bid_t       nid_;     //8 B
    uint64_t    count_;   //8 B
    bool        is_pk_;
    uint64_t    data_offset_;  //8 B
    AnchorNodeHeader(){
        nid_ = NID_NIL;
        count_ = 0;
        is_pk_ = true;
    }
    AnchorNodeHeader(bid_t nid){
        nid_ = NID_NIL;
        count_ = 0;
        nid_ = nid;
        is_pk_ = true;
    }
    AnchorNodeHeader(bid_t nid, bool is_pk){
        nid_ = NID_NIL;
        count_ = 0;
        nid_ = nid;
        is_pk_ = is_pk;
    }
};


#define NODE_HEADER    sizeof(anchorBTree::AnchorNodeHeader)
#define SLOT_OFFSET    NODE_HEADER
#define PAGE_SIZE      NODE_HEADER+(NODE_CAPCITY*SLOT_SIZE)+SAFE_SPACE+(NODE_CAPCITY*KV_SIZE)
#define NODE_SIZE      PAGE_SIZE
#define DATA_OFFSET    NODE_SIZE     //data指向node尾部，从后面来存储数据

class AnchorNode : public AnchorNodeHeader{
    static constexpr uint64_t max_theorical_slots_capacity = (NODE_SIZE - sizeof(AnchorNodeHeader)) / (sizeof(Slot));
    static constexpr uint64_t left_space_to_waste = (NODE_SIZE - sizeof(AnchorNodeHeader)) - max_theorical_slots_capacity*(SLOT_SIZE);
    Slot slot[max_theorical_slots_capacity] __attribute__((aligned(1)));//这样定义的目的是占住内存空间而不是需要有这么多的slot
    Byte paddingSpace[left_space_to_waste] __attribute__((aligned(1)));
public:
    AnchorNode():AnchorNodeHeader(){data_offset_=static_cast<uint64_t>(NODE_SIZE);};
    AnchorNode(bid_t nid):AnchorNodeHeader(nid){data_offset_=static_cast<uint64_t>(NODE_SIZE);};
    AnchorNode(bid_t nid, bool is_pk):AnchorNodeHeader(nid,is_pk){data_offset_=static_cast<uint64_t>(NODE_SIZE);};
    ~AnchorNode(){static_cast<uint64_t>(NODE_SIZE);count_=0;};
    inline Byte* ptr();
    inline Byte* getKey(uint64_t slotId);
    inline uint64_t getKeyLen(uint64_t slotId);
    inline Byte* getPayload(uint64_t slotId);
    inline uint64_t getPayloadLen(uint64_t slotId);
    inline uint64_t getSlotKey(uint64_t slotId);

    inline bid_t nid(){ return this->nid_;};
    void nid2byte(Byte*);

    void setKey(uint64_t slodId, const Byte* key, uint64_t key_len);

    bool canInsert(uint64_t key_len, uint64_t payload_len);
    uint64_t freeSpace();
    uint64_t spaceNeeded(uint64_t key_len, uint64_t payload_len);

    uint64_t lowerBound(const Byte* key, uint64_t key_len);

    bool findInner(const Byte* key, uint64_t key_len, Byte*& payload, uint64_t& payload_len);
    bool findLeaf(const Byte* key, uint64_t key_len, Byte*& payload, uint64_t& payload_len);
    bool insert(const Byte* key, uint64_t key_len, const Byte* payload, uint64_t payload_len);
    bool remove(const Byte* key, uint64_t key_len);

    bool removeAllSlot();
    
    // 从本节点到目标节点的copy,不会影响当前及节点上的payload情况
    void copyKV(AnchorNode* dst, uint64_t dst_slot_id, uint64_t src_slot_id);
    void copyKVRange(AnchorNode* dst, uint64_t dst_slot_id, uint64_t src_slot_id, uint64_t count);

    // 本节点与右侧节点的payloadsplit,操作过后右侧节点和本节点上的count数量都会改变
    void payloadSplit(AnchorNode* nodeRight, Byte* split_bound_low, Byte* split_bound_high, uint64_t key_len);
    void payloadSplit(AnchorNode* nodeRight, Byte* split_bound_low, uint64_t key_len);

    void nodeRepaddiing();
};

class Anchor{
    public:
        Anchor();
        void set_nodeptr(AnchorNode* node);
        void set_empty();
        void update_min_max();
        // concurency
        void read_lock(){lock_.GetReadLock();};
        void read_unlock(){lock_.ReleaseReadLock();};
        void write_lock(){lock_.GetWriteLock();};
        void write_unlock(){lock_.ReleaseWriteLock();};
    // private:
        DBrwLock    lock_;    //16B
        uint64_t    min_;
        uint64_t    max_;
        AnchorNode* nodeptr_;
        bool        occupied_;
    };
    
}

#endif