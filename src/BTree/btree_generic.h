#ifndef BTREE_GENERIC_H_
#define BTREE_GENERIC_H_

#include "util/db_rw_lock.h"
#include "system/config.h"
#include "btree_node.h"
#include "btree_nodepool.h"
#include "btree_thdGuard.h"

namespace btree{


// BTree自身不去考虑节点的创建管理，只负责树上的功能实现
class BTreeGeneric{

struct TreeSchema{
    bid_t           root_node_id;
    bid_t           next_inner_node_id;
    bid_t           next_leaf_node_id;
        // size_t          tree_depth;
    DBrwLock        lock_;
    TreeSchema():root_node_id(NID_NIL),
            next_inner_node_id(NID_START),
            next_leaf_node_id(NID_LEAF_START){}
};
    TreeSchema          schema_;
    TableID             table_id_;
public:
    // BTreeNode*          root_;
    // BTreeNodePool*      nodepool_;


    // BTreeGeneric(TableID table_id,BTreeNodePool* nodepool_,bool is_pk_index);
    BTreeGeneric(TableID table_id);
    ~BTreeGeneric();

    void init(BTreeNodePool* nodepool);
    void initIterator(BTreeNodePool* nodepool);

    inline bid_t root_id();
    inline bid_t next_leaf();
    inline bid_t next_inner();
    inline void  update_root(bid_t newrootid);

    inline BTreeNode* get_root(BTreeNodePool* nodepool);
    bool pipeup(BTreeNode* target,const Byte* key,const uint64_t key_len,const Byte* payload, uint64_t payload_len, BTreeNodePool* nodepool);

    BTreeNode* lock_to_leaf(const Byte* key,const uint64_t key_len, const NODE_LOCK_STATUS lock_type, BTreeNodePool* nodepool, btreeThdGuard* &guard);
    BTreeNode* search_parent(const bid_t nid, BTreeNodePool* nodepool, btreeThdGuard* &guard);

    bool insert_leaf(const Byte* key,const uint64_t key_len,const Byte* tuple, uint64_t tuple_len, BTreeNodePool* nodepool, btreeThdGuard* &guard);
    bool insert_inner(BTreeNode* target,const Byte* key,const uint64_t key_len,const Byte* payload, uint64_t payload_len, BTreeNodePool* nodepool, btreeThdGuard* &guard);

    bool find_kv(const Byte* key, const uint64_t key_len, Byte*& payload, uint64_t& payload_len, BTreeNodePool* nodepool, btreeThdGuard* &guard);
    
    bool remove_kv(const Byte* key, const uint64_t key_len, BTreeNodePool* nodepool, btreeThdGuard* &guard);
    bool remove_kv_inner(BTreeNode* target, const Byte* key, const uint64_t key_len, const Byte* chidByte, BTreeNodePool* nodepool, btreeThdGuard* &guard);

};

}

#endif