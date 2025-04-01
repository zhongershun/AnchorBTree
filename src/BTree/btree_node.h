#ifndef BTREE_NODE_H
#define BTREE_NODE_H


#include "util/db_rw_lock.h"

#include "system/config.h"
#include <cstring>
#include <cstdlib>
#include <mutex>
#include <cstddef>
#include <vector>
using namespace std;

namespace btree{

enum NODE_LOCK_STATUS{
    FREE,
    READ_LOCK,
    WRITE_LOCK,
};

inline static bool node_lock_conflict(NODE_LOCK_STATUS node, NODE_LOCK_STATUS lock){
    if(node==FREE||lock==FREE){
        return false;
    }else if(node==WRITE_LOCK||lock==WRITE_LOCK){
        return true;
    }else{
        return false;
    }
}

typedef uint64_t bid_t; //每一个节点的id号

#define NID_NIL             ((bid_t)0)
#define NID_SCHEMA          ((bid_t)1)
#define NID_START           (NID_NIL + 2)
#define NID_LEAF_START      (bid_t)((1LL << 48) + 1)  // leaf node的id从2^48+1开始直到2^64-1
#define IS_LEAF(nid)        ((nid) >= NID_LEAF_START)

#define PAGE_SIZE      4096
#define PAGE_HEADER    0
#define NODE_SIZE      PAGE_SIZE-PAGE_HEADER

#define INFKEY_SIZE    sizeof(IndexKey)
#define INF_SIZE       4

typedef unsigned char Byte;

inline int cmpKey(const Byte* a, const uint64_t a_len, const Byte* b, const uint64_t b_len);

// 在原来的实现中树上的节点与节点之间的数据交换、分裂、合并与树无关
// split如果不再node中需要记录node属于的
class BTreeIndex;

class BTreeNodeHeader{
    public:
    bid_t       nid_;     //8 B
    DBrwLock    lock_;    //16B
    uint64_t    count_;   //8 B
    bool        is_pk_; // B
    uint64_t    node_key_;
    uint64_t    data_offset_ = static_cast<uint64_t>(NODE_SIZE);  //8 B
    BTreeNodeHeader(){
        nid_ = NID_NIL;
        count_ = 0;
        is_pk_ = true;
    }
    BTreeNodeHeader(bid_t nid){
        nid_ = NID_NIL;
        count_ = 0;
        nid_ = nid;
        is_pk_ = true;
    }
    BTreeNodeHeader(bid_t nid, bool is_pk){
        nid_ = NID_NIL;
        count_ = 0;
        nid_ = nid;
        is_pk_ = is_pk;
    }
};

struct __attribute__((packed)) Slot {
    uint64_t key;
    uint64_t offset;
    // IndexKey key;
    uint64_t key_len;
    uint64_t payload_len;
};

#define SLOT_SIZE      sizeof(Slot)
#define NODE_HEADER    sizeof(BTreeNodeHeader)
#define NODE_HEADER_NI (sizeof(uint64_t)+sizeof(uint64_t))
#define NODE_HEADER_UN NODE_HEADER-NODE_HEADER_NI
#define SLOT_OFFSET    NODE_HEADER
#define DATA_OFFSET    NODE_SIZE     //data指向node尾部，从后面来存储数据

#define SAFE_SPACE     4*SLOT_SIZE   //为slot和尾部来的插入的kv值保留4个slot的"safe space"
// 4K |page_header|node_header|slot[]|

class BTreeNode : public BTreeNodeHeader{
    
    static constexpr uint64_t max_theorical_slots_capacity = (NODE_SIZE - sizeof(BTreeNodeHeader)) / (SLOT_SIZE);
    static constexpr uint64_t left_space_to_waste = (NODE_SIZE - sizeof(BTreeNodeHeader)) - max_theorical_slots_capacity*(SLOT_SIZE);
    Slot slot[max_theorical_slots_capacity] __attribute__((aligned(1)));
    Byte paddingSpace[left_space_to_waste] __attribute__((aligned(1)));
public:
    BTreeNode():BTreeNodeHeader(){};
    BTreeNode(bid_t nid):BTreeNodeHeader(nid){};
    BTreeNode(bid_t nid,bool is_pk):BTreeNodeHeader(nid,is_pk){};
    inline Byte* ptr();
    inline Byte* getKey(uint64_t slotId);
    inline uint64_t getKeyLen(uint64_t slotId);
    inline Byte* getPayload(uint64_t slotId);
    inline uint64_t getPayloadLen(uint64_t slotId);
    inline uint64_t getSlotKey(uint64_t slotId);

    inline bid_t nid(){ return this->nid_; };
    void nid2byte(Byte*);

    void setKey(uint64_t slodId, const Byte* key, uint64_t key_len);
    
    uint64_t spaceNeeded(uint64_t key_len, uint64_t payload_len);

    bool canInsert(uint64_t key_len, uint64_t payload_len);

    uint64_t freeSpace();

    uint64_t lowerBound(const Byte* key, uint64_t key_len);

    bool findInner(const Byte* key, uint64_t key_len, Byte*& payload, uint64_t& payload_len);
    bool findLeaf(const Byte* key, uint64_t key_len, Byte*& payload, uint64_t& payload_len);
    bool insert(const Byte* key, uint64_t key_len, const Byte* payload, uint64_t payload_len);
    bool remove(const Byte* key, uint64_t key_len);

    void setNodeKey(uint64_t key){
        node_key_ = key;
    }

    bool removeAllSlot();
    
    // 从本节点到目标节点的copy,不会影响当前及节点上的payload情况
    void copyKV(BTreeNode* dst, uint64_t dst_slot_id, uint64_t src_slot_id);
    void copyKVRange(BTreeNode* dst, uint64_t dst_slot_id, uint64_t src_slot_id, uint64_t count);

    // 本节点与右侧节点的payloadsplit,操作过后右侧节点和本节点上的count数量都会改变
    void payloadSplit(BTreeNode* nodeRight);

    void nodeRepadding();
    

    // currency
    void read_lock(){lock_.GetReadLock();};
    void read_unlock(){lock_.ReleaseReadLock();};
    void write_lock(){lock_.GetWriteLock();};
    void write_unlock(){lock_.ReleaseWriteLock();};
};

// 在leanstore中payload就是swip
// payload -- pid/bf
}

#endif