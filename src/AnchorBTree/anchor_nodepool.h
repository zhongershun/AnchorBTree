#pragma once

#ifndef ANCHOR_NODEPOOL_H
#define ANCHOR_NODEPOOL_H

#include "anchor_node.h"
// #include "config.h"
#include <unordered_map>
#include <map>
#include <stdint.h>
#include <cstddef>
#include <cassert>

using namespace std;

namespace anchorBTree{

class AnchorNodePool {
public:
    AnchorNodePool();
    ~AnchorNodePool();
    void setIndex(IndexID index_id);
    AnchorNode* new_node(bid_t nid);
    void put_node(bid_t nid, AnchorNode* node);
    AnchorNode* get_node(bid_t nid);
    uint64_t get_node_count(){return newnodes_.size();}
private:
    // unordered_map<bid_t,AnchorNode*>newnodes_;
    map<bid_t,AnchorNode*>newnodes_;
    IndexID index_id_;
};

}

#endif