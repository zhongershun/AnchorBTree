#include "btree_nodepool.h"

using namespace std;

namespace btree{

BTreeNodePool::BTreeNodePool(){
    // index_id_ = index_id;
    newnodes_.clear();
};

BTreeNodePool::~BTreeNodePool(){
    for (auto it = newnodes_.begin();it!=newnodes_.end();it++)
    {
        delete it->second;
        // newnodes_.erase(it);
    }
    newnodes_.clear();
};

void BTreeNodePool::setIndex(IndexID index_id){
    index_id_ = index_id;
}

void BTreeNodePool::is_pk(bool is_pk){
    is_pk_ = is_pk;
}

void BTreeNodePool::put_node(bid_t nid, BTreeNode* node){
    newnodes_[nid]=node;
}

BTreeNode* BTreeNodePool::get_node(bid_t nid) {
    return newnodes_[nid];
}

BTreeNode* BTreeNodePool::new_node(bid_t nid){
    BTreeNode* node = new BTreeNode(nid,is_pk_);
    put_node(nid,node);
    return node;
}

}