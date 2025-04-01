#include "anchor_nodepool.h"

using namespace std;

namespace anchorBTree{

AnchorNodePool::AnchorNodePool(){
    // index_id_ = index_id;
    newnodes_.clear();
};

AnchorNodePool::~AnchorNodePool(){
    for (auto it = newnodes_.begin();it!=newnodes_.end();it++)
    {
        delete it->second;
        // newnodes_.erase(it);
    }
    newnodes_.clear();
};

void AnchorNodePool::setIndex(IndexID index_id){
    index_id_ = index_id;
}

void AnchorNodePool::put_node(bid_t nid, AnchorNode* node){
    newnodes_[nid]=node;
}

AnchorNode* AnchorNodePool::get_node(bid_t nid) {
    return newnodes_[nid];
}

AnchorNode* AnchorNodePool::new_node(bid_t nid){
    AnchorNode* node = new AnchorNode(nid);
    put_node(nid,node);
    return node;
}

}