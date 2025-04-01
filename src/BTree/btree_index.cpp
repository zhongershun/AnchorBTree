#include "btree_index.h"
#include "btree_generic.h"

namespace btree{

BTreeIndex::BTreeIndex(TableID table_id, bool is_pk_index, IndexID index_id){
    nodepool_ = new BTreeNodePool();
    nodepool_->setIndex(index_id);
    nodepool_->is_pk(is_pk_index);
    tree_ = new BTreeGeneric(table_id);
    tree_->init(nodepool_);
    tree_->initIterator(nodepool_);
}

RC BTreeIndex::IndexInsert(IndexKey key, Tuple* tuple){
    tree_->initIterator(nodepool_);
    Byte* keyByte;
    Byte* tupleByte;
    uint64_t key_len = IndexKey2Byte(key,keyByte);
    uint64_t tuple_len = Tuple2Byte(tuple,tupleByte);
    btreeThdGuard* guard = new btreeThdGuard();
    bool res = tree_->insert_leaf(keyByte,key_len,tupleByte,tuple_len,nodepool_,guard);
    free(keyByte);
    free(tupleByte);
    delete guard;
    return res?RC_OK:RC_NULL;
}

RC BTreeIndex::IndexRead(IndexKey key, Tuple* &tuple){
    Byte* keyByte;
    Byte* tupleByte;
    uint64_t key_len = IndexKey2Byte(key,keyByte);
    uint64_t tuple_len;
    btreeThdGuard* guard = new btreeThdGuard();
    bool res = tree_->find_kv(keyByte,key_len,tupleByte,tuple_len,nodepool_,guard);
    if(res){
        Byte2Tuple(tupleByte,tuple);
        free(tupleByte);
    }
    free(keyByte);
    delete guard;
    return res?RC_OK:RC_NULL;
}

RC BTreeIndex::IndexRemove(IndexKey key){
    Byte* keyByte;
    uint64_t key_len = IndexKey2Byte(key,keyByte);
    btreeThdGuard* guard = new btreeThdGuard();
    bool res = tree_->remove_kv(keyByte,key_len,nodepool_,guard);
    free(keyByte);
    delete guard;
    return res?RC_OK:RC_NULL;
}

inline uint64_t BTreeIndex::IndexKey2Byte(IndexKey key, Byte* &keyByte){
    uint64_t key_len = sizeof(uint64_t);
    keyByte = (Byte*)malloc(key_len);
    memcpy(keyByte,&key,sizeof(IndexKey));
    return key_len;
}

inline uint64_t BTreeIndex::Tuple2Byte(Tuple* tuple, Byte* &tupleByte){
    uint64_t tuple_len = sizeof(uint64_t)+tuple->tuple_size_;
    tupleByte = (Byte*)malloc(sizeof(uint64_t)+tuple->tuple_size_);
    memcpy(tupleByte,&(tuple->tuple_size_),sizeof(uint64_t));
    memcpy(tupleByte+sizeof(uint64_t),tuple->tuple_data_,tuple->tuple_size_);
    return tuple_len;
}

inline void BTreeIndex::Byte2IndexKey(Byte* keyByte, IndexKey &key){
    memcpy(&key,keyByte,sizeof(IndexKey));
}

inline void BTreeIndex::Byte2Tuple(Byte* tupleByte, Tuple* &tuple){
    uint64_t tuple_size;
    tuple = new Tuple();
    memcpy(&tuple_size,tupleByte,sizeof(uint64_t));
    tuple->tuple_size_ = tuple_size;
    tuple->tuple_data_ = (TupleData)malloc(tuple_size * sizeof(char));
    memcpy(tuple->tuple_data_,tupleByte+sizeof(uint64_t),tuple_size);
}

}