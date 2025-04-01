#include "btree_node.h"
#include "btree_index.h"

namespace btree{

inline Byte* BTreeNode::ptr(){
    return reinterpret_cast<Byte*>(this);
}

inline Byte* BTreeNode::getKey(uint64_t slotId){
    return ptr()+slot[slotId].offset;
}

inline uint64_t BTreeNode::getKeyLen(uint64_t slotId){
    return slot[slotId].key_len;
}

inline Byte* BTreeNode::getPayload(uint64_t slotId){
    return ptr()+slot[slotId].offset+slot[slotId].key_len;
}

inline uint64_t BTreeNode::getPayloadLen(uint64_t slotId){
    return slot[slotId].payload_len;
}

inline uint64_t BTreeNode::getSlotKey(uint64_t slotId){
    return slot[slotId].key;
}

inline int cmpKey(const Byte* a, const uint64_t a_len, const Byte* b, const uint64_t b_len){
    // TODO: 换成byte的比较
    // uint64_t length = min(a_len,b_len);
    // int c = memcmp(a,b,length);
    // if(c!=0){
    //     return c;
    // }else{
    //     return a_len - b_len;
    // }
    // 在当前的例子里面可以转换为int比较
    uint64_t cmpa;
    memcpy(&cmpa,a,sizeof(uint64_t));
    uint64_t cmpb; 
    memcpy(&cmpb,b,sizeof(uint64_t));
    // uint64_t c = cmpa-cmpb;
    if(cmpa>cmpb){
        return 1;
    }else if(cmpa<cmpb){
        return -1;
    }else{
        return 0;
    }
}

// inline bid_t BTreeNode::nid(){
//     return this->nid_;
// }

void BTreeNode::nid2byte(Byte* nidByte){
    memcpy(nidByte,&nid_,sizeof(bid_t));
    return;
}

void BTreeNode::setKey(uint64_t slodId, const Byte* key, uint64_t key_len){
    memcpy(getKey(slodId),key,key_len);
}

uint64_t BTreeNode::spaceNeeded(uint64_t key_len, uint64_t payload_len){
    return SLOT_SIZE + key_len + payload_len;
}

bool BTreeNode::canInsert(uint64_t key_len, uint64_t payload_len){ // 当剩余的空间还大于等于safe space时返回treu
    // canInsert用来判断是否插入后进行分裂
    if(freeSpace()>=spaceNeeded(key_len,payload_len)){
        if(count_<5){
            nodeRepadding();
        }
        return true;
    }else{
        return false;
    }
    return false;
}


uint64_t BTreeNode::freeSpace(){
    return data_offset_ - (reinterpret_cast<Byte*>(slot+count_)-ptr());
}


uint64_t BTreeNode::lowerBound(const Byte* key, uint64_t key_len){
    uint64_t lower = 0;
    uint64_t higher = count_;
    while (lower<higher)
    {
        uint64_t mid = (lower + higher) >>1;
        int cmp = cmpKey(key, key_len, getKey(mid), getKeyLen(mid));
        if(cmp<0){
            higher = mid;
        }else if(cmp>0){
            lower = mid+1;
        }else{
            return mid;
        }
    }
    return lower;
}

// insert 不在意其他的执行，只负责插入数据
bool BTreeNode::insert(const Byte* key, uint64_t key_len, const Byte* payload, uint64_t payload_len){
    uint64_t slotId = lowerBound(key, key_len);
    if(is_pk_&&IS_LEAF(nid_)&&slotId!=count_&&cmpKey(getKey(slotId),getKeyLen(slotId),key,key_len)==0){
        return false;
    }
    memmove(slot+slotId+1, slot+slotId, SLOT_SIZE*(count_-slotId));
    uint64_t restorekey;
    memcpy(&restorekey,key,sizeof(uint64_t));
    slot[slotId].key = restorekey;
    slot[slotId].key_len = key_len;
    slot[slotId].payload_len = payload_len;
    uint64_t space = key_len+payload_len;
    data_offset_ -= space;
    slot[slotId].offset = data_offset_;
    memcpy(getKey(slotId), key, key_len);
    memcpy(getPayload(slotId), payload, payload_len);
    count_+=1;
    return true;
}

bool BTreeNode::remove(const Byte* key, uint64_t key_len){
    uint64_t slotId = lowerBound(key,key_len);
    if(slotId>=count_){
        return false;
    }
    if(cmpKey(key,key_len,getKey(slotId),getKeyLen(slotId))!=0){
        return false; // key not found
    }
    memmove(slot+slotId,slot+slotId+1,SLOT_SIZE*(count_-slotId-1));
    // TODO:这里只能删除slot,而实际kv内存占用没有删除，可以去删除，但是需要修改的kv的位置太多了，开销太大
    count_-=1;
    return true;
}

bool BTreeNode::removeAllSlot(){
    count_ = 0;
    data_offset_ = NODE_SIZE;
    return true;
}

// 将src(this)节点上的从src_slot_id开始的总共count个slot复制到dst从dst_slot_id开始的位置
void BTreeNode::copyKV(BTreeNode* dst, uint64_t dst_slot_id, uint64_t src_slot_id){
    memcpy(dst->slot+dst_slot_id, slot+src_slot_id, SLOT_SIZE);
    uint64_t key_len = getKeyLen(src_slot_id);
    uint64_t payload_len = getPayloadLen(src_slot_id);
    // 对应拷贝过去之后的偏移量需要重新计算
    dst->data_offset_-=(key_len+payload_len);
    dst->slot[dst_slot_id].offset = dst->data_offset_;
    memcpy(dst->ptr()+dst->data_offset_,ptr()+slot[src_slot_id].offset,(key_len+payload_len));
    dst->count_ += 1;
}

// 将src(this)节点上的从src_slot_id开始的总共count个slot复制到dst从dst_slot_id开始的位置
void BTreeNode::copyKVRange(BTreeNode* dst, uint64_t dst_slot_id, uint64_t src_slot_id, uint64_t count){
    memcpy(dst->slot+dst_slot_id, slot+src_slot_id, SLOT_SIZE*count);
    for (int i = 0; i < count; i++)
    {
        uint64_t key_len = getKeyLen(src_slot_id+i);
        uint64_t payload_len = getPayloadLen(src_slot_id+i);
        // 对应拷贝过去之后的偏移量需要重新计算
        dst->data_offset_-=(key_len+payload_len);
        dst->slot[dst_slot_id+i].offset = dst->data_offset_;
        memcpy(dst->ptr()+dst->data_offset_,ptr()+slot[src_slot_id+i].offset,(key_len+payload_len));
    }
    dst->count_ += count;
}

bool BTreeNode::findInner(const Byte* key, uint64_t key_len, Byte*& payload, uint64_t& payload_len){
    if(count_==0){
        payload=nullptr;
        payload_len=0;
        return false;
    }
    uint64_t slotId = lowerBound(key,key_len); //lowerBound返回的结果是>=key的slotId
    if(slotId==count_){ // 首先需要确保slotID可访问(此时的count!=0) // 1
        payload_len = getPayloadLen(count_-1);
        payload = (Byte*)malloc(payload_len);
        memcpy(payload,getPayload(count_-1),payload_len);
        return true;
    }
    int cmpRes = cmpKey(getKey(slotId),getKeyLen(slotId),key,key_len);
    assert(cmpRes>=0);
    if(cmpRes==0){
        payload_len = getPayloadLen(slotId);
        payload = (Byte*)malloc(payload_len);
        memcpy(payload,getPayload(slotId),payload_len);
        return true;
    }else if(cmpRes>0&&slotId!=0){
        payload_len = getPayloadLen(slotId-1);
        payload = (Byte*)malloc(payload_len);
        memcpy(payload,getPayload(slotId-1),payload_len);
        return true;
    }else{
        payload=nullptr;
        payload_len=0;
        return false;
    }
}

bool BTreeNode::findLeaf(const Byte* key, uint64_t key_len, Byte*& payload, uint64_t& payload_len){
    if(count_==0){
        payload=nullptr;
        payload_len=0;
        return false;
    }
    uint64_t slotId = lowerBound(key,key_len); //lowerBound返回的结果是>=key的slotId
    if(slotId==count_){ // 首先需要确保slotID可访问(此时的count!=0) // 1
        payload=nullptr;
        payload_len=0;
        return false;
    }
    int cmpRes = cmpKey(getKey(slotId),getKeyLen(slotId),key,key_len);
    assert(cmpRes>=0);
    if(cmpRes==0){                                              // 2
        payload_len = getPayloadLen(slotId);
        payload = (Byte*)malloc(payload_len);
        memcpy(payload,getPayload(slotId),payload_len);
        return true;
    }else { // 这里返回的应该都是大于等于
        payload=nullptr;
        payload_len=0;
        return false;
    }
}


void BTreeNode::payloadSplit(BTreeNode* nodeRight){
    BTreeNode left;  // left for this
    BTreeNode* nodeLeft = &left;
    int slot_id = count_/2;
    // IndexKey k = slot[slot_id].key; // for node right
    copyKVRange((BTreeNode*)(nodeLeft->ptr()),0,0,slot_id);
    copyKVRange((BTreeNode*)(nodeRight->ptr()),0,slot_id,count_ - nodeLeft->count_);
    memcpy(this->ptr()+NODE_HEADER,nodeLeft->ptr()+NODE_HEADER,NODE_SIZE-NODE_HEADER);
    data_offset_ = nodeLeft->data_offset_;
    count_ = nodeLeft->count_;
    nodeRight->setNodeKey(nodeRight->getSlotKey(0));
    return;
}

void BTreeNode::nodeRepadding(){
    BTreeNode newnode;
    BTreeNode* nodeNew = &newnode;
    copyKVRange((BTreeNode*)(nodeNew->ptr()),0,0,count_);
    memcpy(this->ptr()+NODE_HEADER,nodeNew->ptr()+NODE_HEADER,NODE_SIZE-NODE_HEADER);
    data_offset_ = nodeNew->data_offset_;
    count_ = nodeNew->count_;
    return;
}



}