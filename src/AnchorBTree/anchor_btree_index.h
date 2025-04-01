#ifndef ANCHOR_BTREE_INDEX_H_
#define ANCHOR_BTREE_INDEX_H_

#include "system/config.h"
#include "util/tuple.h"
#include "anchor_btree.h"
// #include "anchor_nodepool.h"
// #include "anchor_thdGuard.h"

using namespace std;

namespace anchorBTree{

class AnchorBTreeIndex{
public:
    // AnchorBTreeIndex(TableID table_id, BTreeNodePool *nodepool, bool is_pk_index, IndexID index_id);
    AnchorBTreeIndex(TableID table_id, bool is_pk_index, IndexID index_id);
    AnchorBTreeIndex(TableID table_id, bool is_pk_index, IndexID index_id, uint64_t anchor_pow);
    ~AnchorBTreeIndex(){};

    // RC IndexInsert(IndexKey key, Tuple* tuple, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP);
    RC IndexInsert(IndexKey key, Tuple* tuple);

    // the following call returns a single tuple
    // RC IndexRead(IndexKey key, Tuple* &tuple, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP);
    RC IndexRead(IndexKey key, Tuple* &tuple);

    //TODO
    //the following call return a set of tuple that indexkey equals to key
    //use for secondary index
    // RC IndexRead(IndexKey key, Tuple** &tuples, uint64_t& tuple_count, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP);
    RC IndexRead(IndexKey key, Tuple** &tuples, uint64_t& tuple_count){return RC_OK;};

    //need to free memory that index data structure occupied
    //not free tuple. The lifecycle of a tuple is determined by the transaction component
    // RC IndexRemove(IndexKey key, PartitionKey part_key = 0, Timestamp visible_ts = MAX_TIMESTAMP);
    RC IndexRemove(IndexKey key);

private:
    BTreeAnchor* anchortree_;
    AnchorNodePool* nodepool_;

protected:
// public:
    // Byte:
    // tuple_size + tuple_data
    // uint64_t + tuple_data(len)
    inline uint64_t IndexKey2Byte(IndexKey key, Byte* &keyByte);
    inline uint64_t Tuple2Byte(Tuple* tuple, Byte* &tupleByte);
    inline void Byte2IndexKey(Byte* keyByte, IndexKey &key);
    inline void Byte2Tuple(Byte* tupleByte, Tuple* &tuple);
};
}

#endif