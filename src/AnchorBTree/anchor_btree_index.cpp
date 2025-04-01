#include "anchor_btree_index.h"
#include "anchor_btree.h"

namespace anchorBTree{

AnchorBTreeIndex::AnchorBTreeIndex(TableID table_id, bool is_pk_index, IndexID index_id){
    nodepool_ = new AnchorNodePool();
    nodepool_->setIndex(index_id);
    anchortree_ = new BTreeAnchor();
}

AnchorBTreeIndex::AnchorBTreeIndex(TableID table_id, bool is_pk_index, IndexID index_id, uint64_t anchor_pow){
    nodepool_ = new AnchorNodePool();
    nodepool_->setIndex(index_id);
    anchortree_ = new BTreeAnchor(anchor_pow);
}

RC AnchorBTreeIndex::IndexInsert(IndexKey key, Tuple* tuple){
    Byte* keyByte;
    Byte* tupleByte;
    uint64_t key_len = IndexKey2Byte(key,keyByte);
    uint64_t tuple_len = Tuple2Byte(tuple,tupleByte);
    anchorThdGuard* guard = new anchorThdGuard();
    int retry = 0;
    bool res = false;
    guard->setretry();
    while (true)
    {
        res = anchortree_->insert_kv(keyByte,key_len,tupleByte,tuple_len,nodepool_,guard);
        if(res||guard->getRetry()==0||retry==RETYR_COUNT){
            break;
        }
        retry++;
    }
    free(keyByte);
    free(tupleByte);
    delete guard;
    return res?RC_OK:RC_NULL;
}

RC AnchorBTreeIndex::IndexRead(IndexKey key, Tuple* &tuple){
    Byte* keyByte;
    Byte* tupleByte;
    uint64_t key_len = IndexKey2Byte(key,keyByte);
    uint64_t tuple_len;
    anchorThdGuard* guard = new anchorThdGuard();
    int retry = 0;
    bool res = false;
    guard->setretry();
    while (true)
    {
        res = anchortree_->find_kv(keyByte,key_len,tupleByte,tuple_len,nodepool_,guard);
        if(res||guard->getRetry()==0||retry==RETYR_COUNT){
            break;
        }
        retry++;
    }
    if(res){
        Byte2Tuple(tupleByte,tuple);
        free(tupleByte);
    }
    free(keyByte);
    delete guard;
    return res?RC_OK:RC_NULL;
}

RC AnchorBTreeIndex::IndexRemove(IndexKey key){
    Byte* keyByte;
    uint64_t key_len = IndexKey2Byte(key,keyByte);
    anchorThdGuard* guard = new anchorThdGuard();
    int retry = 0;
    bool res = false;
    guard->setretry();
    while (true)
    {
        res = anchortree_->remove_kv(keyByte,key_len,nodepool_,guard);
        if(res||guard->getRetry()==0||retry==RETYR_COUNT){
            break;
        }
        retry++;
    }
    free(keyByte);
    delete guard;
    return res?RC_OK:RC_NULL;
}

inline uint64_t AnchorBTreeIndex::IndexKey2Byte(IndexKey key, Byte* &keyByte){
    uint64_t key_len = sizeof(uint64_t);
    keyByte = (Byte*)malloc(key_len);
    memcpy(keyByte,&key,sizeof(IndexKey));
    return key_len;
}

inline uint64_t AnchorBTreeIndex::Tuple2Byte(Tuple* tuple, Byte* &tupleByte){
    uint64_t tuple_len = sizeof(uint64_t)+tuple->tuple_size_;
    tupleByte = (Byte*)malloc(sizeof(uint64_t)+tuple->tuple_size_);
    memcpy(tupleByte,&(tuple->tuple_size_),sizeof(uint64_t));
    memcpy(tupleByte+sizeof(uint64_t),tuple->tuple_data_,tuple->tuple_size_);
    return tuple_len;
}

inline void AnchorBTreeIndex::Byte2IndexKey(Byte* keyByte, IndexKey &key){
    memcpy(&key,keyByte,sizeof(IndexKey));
}

inline void AnchorBTreeIndex::Byte2Tuple(Byte* tupleByte, Tuple* &tuple){
    uint64_t tuple_size;
    tuple = new Tuple();
    memcpy(&tuple_size,tupleByte,sizeof(uint64_t));
    tuple->tuple_size_ = tuple_size;
    tuple->tuple_data_ = (TupleData)malloc(tuple_size * sizeof(char));
    memcpy(tuple->tuple_data_,tupleByte+sizeof(uint64_t),tuple_size);
}

}