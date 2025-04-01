#ifndef ANCHOR_BTREE_H_
#define ANCHOR_BTREE_H_

#include <iostream>
#include <cstdint>
#include <vector>
#include <list>
#include <cassert>
#include "system/config.h"
#include "anchor_node.h"
#include "anchor_nodepool.h"
#include "anchor_thdGuard.h"

using namespace std;

namespace anchorBTree{

// ---------------------------------------------------------------------------------

//需要确保anchor_cnt为2^x

class BTreeAnchor
{
private:
    // TOOD : anchor_cnt 初始化
    // static constexpr uint64_t anchor_cnt_ = ANCHOR_CNT;
    // Anchor anchors_[anchor_cnt_];
    uint64_t anchor_cnt_;
    Anchor** anchors_;
    vector<list<uint64_t>> split_map_;

    void split_map_init();

    uint64_t find_source(uint64_t idx);
    uint64_t find_split_target(uint64_t idx,IndexKey key);

    uint64_t lowbound_of_anchor(uint64_t idx);
    uint64_t highbound_of_anchor(uint64_t idx);

    bool check_incurrent(uint64_t idx, uint64_t chidx, anchorThdGuard* &guard);

    bool fast_find_node(IndexKey key, AnchorNode* &node, uint64_t& idx, anchorThdGuard* &guard, NODE_LOCK_STATUS lock_type);
    bool slow_find_node(IndexKey key, AnchorNode* &node, uint64_t& idx, uint64_t& childidx, anchorThdGuard* &guard, NODE_LOCK_STATUS lock_type);

    AnchorNode* new_node(uint64_t nid);
public:
    BTreeAnchor();
    BTreeAnchor(uint64_t anchor_pow);
    ~BTreeAnchor();

    bool find_kv(const Byte* key, const uint64_t key_len, Byte*& payload, uint64_t& payload_len, AnchorNodePool* nodepool, anchorThdGuard* &guard);
    bool insert_kv(const Byte* key,const uint64_t key_len,const Byte* tuple, uint64_t tuple_len, AnchorNodePool* nodepool, anchorThdGuard* &guard);
    bool remove_kv(const Byte* key, const uint64_t key_len, AnchorNodePool* nodepool, anchorThdGuard* &guard);
    
//DeBug:
    void prinfEdge(){
        cout << "edge:\n";
        for (uint64_t i = 0; i < anchor_cnt_; i++) {
            cout << i << ": ";
            for (auto it : split_map_[i]) {
                cout << it << ",";
            }
            cout << "\n";
        }
    }

    bool find_node_debug(IndexKey key, AnchorNode* &node, uint64_t& idx){
        anchorThdGuard* guard = new anchorThdGuard();
        uint64_t childidx = 0;
        return slow_find_node(key, node, idx, childidx, guard, READ_LOCK);
        delete guard;
    }

public:
};

}

#endif