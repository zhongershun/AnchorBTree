#pragma once

#ifndef BTREE_NODEPOOL_H
#define BTREE_NODEPOOL_H

#include "btree_node.h"
// #include "config.h"
#include <unordered_map>
#include <map>
#include <stdint.h>
#include <cstddef>
#include <cassert>

using namespace std;

namespace btree{

class BTreeNodePool {
public:
    BTreeNodePool();
    ~BTreeNodePool();
    void setIndex(IndexID index_id);
    void is_pk(bool is_pk);
    BTreeNode* new_node(bid_t nid);
    void put_node(bid_t nid, BTreeNode* node);
    BTreeNode* get_node(bid_t nid);
    uint64_t get_node_count(){return newnodes_.size();}
private:
    // unordered_map<bid_t,BTreeNode*>newnodes_;
    map<bid_t,BTreeNode*>newnodes_;
    IndexID index_id_;
    bool is_pk_;
};

}

#endif