#include "anchor_node.h"

namespace anchorBTree{

inline Byte* AnchorNode::ptr(){
    return reinterpret_cast<Byte*>(this);
}

inline Byte* AnchorNode::getKey(uint64_t slotId){
    return ptr()+slot[slotId].offset;
}

inline uint64_t AnchorNode::getKeyLen(uint64_t slotId){
    return slot[slotId].key_len;
}

inline Byte* AnchorNode::getPayload(uint64_t slotId){
    return ptr()+slot[slotId].offset+slot[slotId].key_len;
}

inline uint64_t AnchorNode::getPayloadLen(uint64_t slotId){
    return slot[slotId].payload_len;
}

inline uint64_t AnchorNode::getSlotKey(uint64_t slotId){
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

// inline bid_t AnchorNode::nid(){
//     return this->nid_;
// }

void AnchorNode::nid2byte(Byte* nidByte){
    memcpy(nidByte,&nid_,sizeof(bid_t));
    return;
}

void AnchorNode::setKey(uint64_t slodId, const Byte* key, uint64_t key_len){
    memcpy(getKey(slodId),key,key_len);
}

uint64_t AnchorNode::spaceNeeded(uint64_t key_len, uint64_t payload_len){
    // 计算插入一个key和payload需要的空间
    return SLOT_SIZE + key_len + payload_len;
}

bool AnchorNode::canInsert(uint64_t key_len, uint64_t payload_len){
    // canInsert用来判断是否插入后进行分裂
    if(count_<NODE_CAPCITY){
        if(freeSpace()<=spaceNeeded(key_len, payload_len)){
            nodeRepaddiing();
        }
        return true;
    }else{
        return false;
    }
    return false;
}

uint64_t AnchorNode::freeSpace(){
    return data_offset_ - (reinterpret_cast<Byte*>(slot+count_)-ptr());
}

uint64_t AnchorNode::lowerBound(const Byte* key, uint64_t key_len){
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
bool AnchorNode::insert(const Byte* key, uint64_t key_len, const Byte* payload, uint64_t payload_len){
    uint64_t slotId = lowerBound(key, key_len);
    if(is_pk_&&slotId!=count_&&cmpKey(getKey(slotId),getKeyLen(slotId),key,key_len)==0){
        return false;
    }
    // if(count_>=80){
    //     printf("insert data count over 80 before now data_offset_ is : %ld\n",data_offset_);
    // }
    memmove(slot+slotId+1, slot+slotId, SLOT_SIZE*(count_-slotId));
    uint64_t restorekey;
    memcpy(&restorekey,key,sizeof(uint64_t));
    slot[slotId].key = restorekey;
    slot[slotId].key_len = key_len;
    slot[slotId].payload_len = payload_len;
    uint64_t space = key_len+payload_len;
    uint64_t data_offset_old = data_offset_;
    data_offset_ -= space;
    slot[slotId].offset = data_offset_;
    memcpy(getKey(slotId), key, key_len);
    memcpy(getPayload(slotId), payload, payload_len);
    count_+=1;
    uint64_t data_offset_new = data_offset_;
    return true;
}

bool AnchorNode::remove(const Byte* key, uint64_t key_len){
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

bool AnchorNode::removeAllSlot(){
    count_ = 0;
    data_offset_ = NODE_SIZE;
    return true;
}

// 将src(this)节点上的从src_slot_id开始的总共count个slot复制到dst从dst_slot_id开始的位置
void AnchorNode::copyKV(AnchorNode* dst, uint64_t dst_slot_id, uint64_t src_slot_id){
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
void AnchorNode::copyKVRange(AnchorNode* dst, uint64_t dst_slot_id, uint64_t src_slot_id, uint64_t count){
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



// \param payload只需要传入指针，函数内部完成内存分配
// \return true为查询到节点内部有该key对应的value
// \return false为节点内部没有该key对应的value,但是会返回大于小于等于目标key的值
// 对于节点内部的查询
bool AnchorNode::findInner(const Byte* key, uint64_t key_len, Byte*& payload, uint64_t& payload_len){
// 进1则不可能进2
// 进1进3则此时节点内只有一个元素
// 进1进4则此时节点内只有一个元素且这个元素大于目标
// 进1则不可能进5,因为返回的slotID位置是大于等于目标的
// 进3则必定进1
// 1-3 1-4
// 2
// 4
// 5
    if(count_==0){
        payload=nullptr;
        payload_len=0;
        return false;
    }
    uint64_t slotId = lowerBound(key,key_len); //lowerBound返回的结果是>=key的slotId
    if(slotId==count_){ // 首先需要确保slotID可访问(此时的count!=0) // 1
        slotId--;
    }
    int cmpRes = cmpKey(getKey(slotId),getKeyLen(slotId),key,key_len);
    if(cmpRes==0){                                              // 2
        payload_len = getPayloadLen(slotId);
        payload = (Byte*)malloc(payload_len);
        memcpy(payload,getPayload(slotId),payload_len);
        // payload = getPayload(slotId);
        return true;
    }else { // 这里返回的应该都是大于等于
        if(cmpRes<0){ // 初次返回的slotID==count                  // 3
            payload_len = getPayloadLen(slotId);
            payload = (Byte*)malloc(payload_len);
            memcpy(payload,getPayload(slotId),payload_len);
            // payload = getPayload(slotId);
        }else{
            if(slotId==0){ // 节点上最小的都大于目标                // 4
                payload = nullptr;
            }else{                                              // 5
                slotId--;
                payload_len = getPayloadLen(slotId);
                payload = (Byte*)malloc(payload_len);
                memcpy(payload,getPayload(slotId),payload_len);
                // payload = getPayload(slotId);
            }
        }
        return false;
    }
}

bool AnchorNode::findLeaf(const Byte* key, uint64_t key_len, Byte*& payload, uint64_t& payload_len){
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


void AnchorNode::payloadSplit(AnchorNode* nodeRight, Byte* split_bound_low, Byte* split_bound_high, uint64_t key_len){
    AnchorNode left;  // left for this
    AnchorNode* nodeLeft = &left;
    uint64_t slot_id_low = lowerBound(split_bound_low,key_len); // >=key
    uint64_t slot_id_high = lowerBound(split_bound_high,key_len);
    if (slot_id_low >= count_) {
        // 全部保留在左节点，右节点为空
        return;
    }

    if (slot_id_high >= count_) {
        slot_id_high = count_ - 1;
    } else if (cmpKey(getKey(slot_id_high), getKeyLen(slot_id_high), 
                     split_bound_high, key_len) > 0) {
        if (slot_id_high == 0) {
            return; // 无数据需要分裂
        }
        slot_id_high--;
    }
    if (slot_id_high < slot_id_low) {
        return;
    }
    uint64_t split_count = slot_id_high-slot_id_low+1;
    // IndexKey k = slot[slot_id].key; // for node right
    copyKVRange((AnchorNode*)(nodeLeft->ptr()),0,0,slot_id_low);
    copyKVRange((AnchorNode*)(nodeRight->ptr()),0,slot_id_low,split_count);
    copyKVRange((AnchorNode*)(nodeLeft->ptr()),slot_id_low,slot_id_high+1,count_-slot_id_high-1);
    memcpy(this->ptr()+NODE_HEADER,nodeLeft->ptr()+NODE_HEADER,NODE_SIZE-NODE_HEADER);
    data_offset_ = nodeLeft->data_offset_;
    count_ = nodeLeft->count_;
    return;
}

void AnchorNode::payloadSplit(AnchorNode* nodeRight, Byte* split_bound_low, uint64_t key_len){
    AnchorNode left;  // left for this
    AnchorNode* nodeLeft = &left;
    uint64_t slot_id_low = lowerBound(split_bound_low,key_len); // >=key
    if(slot_id_low>=count_){ // split_bound_low>key[-1] 这种情况说明插入的kv大于当前所有的kv,就是把要插入的kv分裂出去就行
        return;
    }
    uint64_t split_count = count_-slot_id_low;
    // IndexKey k = slot[slot_id].key; // for node right
    copyKVRange((AnchorNode*)(nodeLeft->ptr()),0,0,slot_id_low);
    copyKVRange((AnchorNode*)(nodeRight->ptr()),0,slot_id_low,split_count);
    memcpy(this->ptr()+NODE_HEADER,nodeLeft->ptr()+NODE_HEADER,NODE_SIZE-NODE_HEADER);
    data_offset_ = nodeLeft->data_offset_;
    count_ = nodeLeft->count_;
    return;
}

void AnchorNode::nodeRepaddiing(){
    AnchorNode newnode;
    AnchorNode* nodeNew = &newnode;
    copyKVRange((AnchorNode*)(nodeNew->ptr()),0,0,count_);
    memcpy(this->ptr()+NODE_HEADER,nodeNew->ptr()+NODE_HEADER,NODE_SIZE-NODE_HEADER);
    data_offset_ = nodeNew->data_offset_;
    count_ = nodeNew->count_;
    return;
}

Anchor::Anchor(){
    nodeptr_=nullptr;
    occupied_=false;
}

void Anchor::set_nodeptr(AnchorNode* node){
    occupied_=true;
    nodeptr_ = node;
}

void Anchor::set_empty(){
    nodeptr_=nullptr;
    occupied_=false;
}

void Anchor::update_min_max(){
    assert(occupied_==true);
    assert(nodeptr_!=nullptr);
    // min_ = *(uint64_t*)nodeptr_->getKey(0);
    // max_ = *(uint64_t*)nodeptr_->getKey(nodeptr_->count_-1);
    min_ = nodeptr_->getSlotKey(0);
    max_ = nodeptr_->getSlotKey(nodeptr_->count_-1);
    // printf("min:%lu,max:%lu\n",min_,max_);
    // if(min_>max_){
    //     printf("error\n");
    // }
    return;
    // memcpy(&min_,nodeptr_->getKey(0),nodeptr_->getKeyLen(0));
    // memcpy(&max_,nodeptr_->getKey(nodeptr_->count_-1),nodeptr_->getKeyLen(nodeptr_->count_-1));
}


}