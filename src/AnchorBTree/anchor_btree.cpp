#include "anchor_btree.h"
namespace anchorBTree{

BTreeAnchor::BTreeAnchor()
{
    // 默认
    anchor_cnt_=16;
    split_map_init();
    anchors_ = new Anchor*[anchor_cnt_];
    for (uint64_t i = 0; i < anchor_cnt_; i++)
    {
        anchors_[i] = new Anchor();
    }
    anchors_[0]->nodeptr_ = new_node(0);
    anchors_[0]->occupied_ = true;
    anchors_[0]->min_ = 0;
    anchors_[0]->max_ = 0;
}

BTreeAnchor::BTreeAnchor(uint64_t anchor_pow)
{
    anchor_cnt_=(1<<anchor_pow);
    split_map_init();
    anchors_ = new Anchor*[anchor_cnt_];
    for (uint64_t i = 0; i < anchor_cnt_; i++)
    {
        anchors_[i] = new Anchor();
    }
    anchors_[0]->nodeptr_ = new_node(0);
    anchors_[0]->occupied_ = true;
    anchors_[0]->min_ = 0;
    anchors_[0]->max_ = 0;
}

BTreeAnchor::~BTreeAnchor()
{
    split_map_.clear();
}

uint64_t BTreeAnchor::find_source(uint64_t idx){
    uint64_t src = idx - (idx & -idx);
    return src;
}

void BTreeAnchor::split_map_init(){
    split_map_.resize(anchor_cnt_);
    for (uint64_t dst = 1; dst < anchor_cnt_; dst++) {
        uint64_t src = find_source(dst);
        split_map_[src].push_back(dst);
    }
}

uint64_t BTreeAnchor::lowbound_of_anchor(uint64_t idx){
    return idx*NODE_CAPCITY+1;
}

uint64_t BTreeAnchor::highbound_of_anchor(uint64_t idx){
    return (idx+1)*NODE_CAPCITY;
}

bool BTreeAnchor::check_incurrent(uint64_t idx, uint64_t chidx, anchorThdGuard* &guard){
    if(idx==chidx){
        return true;
    }else{
        guard->guardNode(anchors_[chidx],READ_LOCK);
        bool res = anchors_[chidx]->occupied_;
        guard->deguardNode(anchors_[chidx]);
        return !res;
    }
}

// 进行分裂时可以保证max_必大于NODE_CAPCITY
uint64_t BTreeAnchor::find_split_target(uint64_t idx, IndexKey key){
    uint64_t keymax = (anchors_[idx]->max_>key)?(anchors_[idx]->max_):(key);
    for(auto it=split_map_[idx].rbegin();it!=split_map_[idx].rend();it++){
        if(keymax>=lowbound_of_anchor(*it)){
            return *it;
        }
    }
}

// fast_find_node一定是找到最后的位置，不会
bool BTreeAnchor::fast_find_node(IndexKey key, AnchorNode* &node, uint64_t& idx, anchorThdGuard* &guard, NODE_LOCK_STATUS lock_type){ 
    idx = key/NODE_CAPCITY;
    if(idx>anchor_cnt_){
        node = nullptr;
        return false;
    }
    if(key%NODE_CAPCITY==0&&idx!=0){
        idx--;
    }
    guard->guardNode(anchors_[idx],lock_type);
    if(anchors_[idx]->occupied_){
        node = anchors_[idx]->nodeptr_;
        return true;
    }else{
        node=nullptr; // 这个节点还没有开始分裂占有，只会在之前的节点中
        return false;
        // 3. 节点还没有分裂到，在之前的节点中 false nullptr
    }
}

bool BTreeAnchor::slow_find_node(IndexKey key, AnchorNode* &node, uint64_t& idx, uint64_t& chidx, anchorThdGuard* &guard, NODE_LOCK_STATUS lock_type){
    bool fast_find = fast_find_node(key,node,idx,guard,lock_type);
    if(idx>=anchor_cnt_){
        node = nullptr;
        return false;
    }
    chidx = idx; //此时anchor[idx]已经被锁住
    if(fast_find){
        return true;
        // 1. 在范围之内同时操作的就是这个节点 true nodeptr_
    }else{
        if(node==nullptr){
            // 需要从idx的source处去找
            bool find = false;
            while (idx!=0)
            {
                chidx = idx;
                idx = find_source(chidx);
                guard->deguardNode(anchors_[chidx]);
                guard->guardNode(anchors_[idx],lock_type);
                if(anchors_[idx]->occupied_){
                    node = anchors_[idx]->nodeptr_;
                    find = true;
                    break;
                }
            }
            if(find){
                return true;
            }else{ // anchor0 此时idx==0
                if(anchors_[idx]->occupied_){// 3. anchor0都没有了
                    node = anchors_[idx]->nodeptr_;
                    return true;
                }else{
                    return false;
                }
            }
            // 2. 就是操作目标节点
        }else{
            // 确定occupied
            // 2. 就是操作目标节点
            return true;
        }
    }
}


AnchorNode* BTreeAnchor::new_node(uint64_t nid){
    AnchorNode* node = new AnchorNode(nid);
    return node;
}

bool BTreeAnchor::find_kv(const Byte* key, const uint64_t key_len, Byte*& payload, uint64_t& payload_len, AnchorNodePool* nodepool, anchorThdGuard* &guard){
    IndexKey indexKey;
    AnchorNode* node =nullptr;
    uint64_t idx = 0;
    uint64_t chidx = 0;
    memcpy(&indexKey,key,sizeof(IndexKey));
    if(!slow_find_node(indexKey,node,idx,chidx,guard,READ_LOCK)){
        guard->deguardAllNode();
        return false;
    }
    // 在溯源的过程中因为是先放锁然后再加锁，所以这里需要添加check_incurrent来判断是否正确找到了目标操作的节点
    if(!check_incurrent(idx,chidx,guard)){
        guard->deguardAllNode();
        guard->retry();
        return false;
    }
    if(node->findLeaf(key,key_len,payload,payload_len)){
        guard->deguardAllNode();
        return true;
    }else{
        guard->deguardAllNode();
        return false;
    }
}

bool BTreeAnchor::insert_kv(const Byte* key,const uint64_t key_len,const Byte* tuple, uint64_t tuple_len, AnchorNodePool* nodepool, anchorThdGuard* &guard){
    IndexKey indexKey;
    AnchorNode* node =nullptr;
    uint64_t idx = 0;
    uint64_t chidx = 0;
    memcpy(&indexKey,key,sizeof(IndexKey));
    if(!slow_find_node(indexKey,node,idx,chidx,guard,WRITE_LOCK)){
        guard->deguardAllNode();
        return false;
    }
    // 在溯源的过程中因为是先放锁然后再加锁，所以这里需要添加check_incurrent来判断是否正确找到了目标操作的节点
    if(!check_incurrent(idx,chidx,guard)){
        guard->deguardAllNode();
        guard->retry();
        return false;
    }
    Byte* checkin_byte;
    uint64_t checkin_len;
    if(node->canInsert(key_len,tuple_len)){
        bool res = node->insert(key,key_len,tuple,tuple_len);
        anchors_[idx]->update_min_max();
        guard->deguardAllNode();
        return res;
    }else{
        uint64_t rightnid = chidx;
        AnchorNode* nodeRight;
        IndexKey split_bound_low_key;
        Byte* split_bound_low = (Byte*)malloc(sizeof(uint64_t));
        IndexKey split_bound_high_key;
        Byte* split_bound_high = (Byte*)malloc(sizeof(uint64_t));
        if(chidx!=idx){ // 溯源来到的idx
            rightnid = chidx;
            guard->guardNode(anchors_[rightnid],WRITE_LOCK);
            if(anchors_[rightnid]->occupied_){
                guard->deguardAllNode();
                guard->retry();
                delete split_bound_high;
                delete split_bound_low;
                return false;
            }
            nodeRight = nodepool->new_node(rightnid);
            split_bound_low_key = lowbound_of_anchor(rightnid);
            split_bound_high_key = highbound_of_anchor(rightnid);
            memcpy(split_bound_low,&split_bound_low_key,sizeof(uint64_t));
            memcpy(split_bound_high,&split_bound_high_key,sizeof(uint64_t));
            node->payloadSplit(nodeRight,split_bound_low,split_bound_high,sizeof(uint64_t));
            // 这个情况必定是到nodeRight中插入
            if(nodeRight->canInsert(key_len,tuple_len)){
                bool res = nodeRight->insert(key,key_len,tuple,tuple_len);
                anchors_[idx]->update_min_max();
                anchors_[rightnid]->set_nodeptr(nodeRight);
                anchors_[rightnid]->update_min_max();
                guard->deguardAllNode();
                delete split_bound_high;
                delete split_bound_low;
                return res;
            }else{// 衍生分配的nodeRight如果满了则说明必定全部是在nodeRight上下界中的
                if(cmpKey(key,key_len,split_bound_low,sizeof(uint64_t))>=0&&cmpKey(key,key_len,split_bound_high,sizeof(uint64_t))<=0){
                    bool res = nodeRight->insert(key,key_len,tuple,tuple_len);
                    assert(res==false);// 这里因为是只考虑主键索引，所以必然是false
                    anchors_[idx]->update_min_max();
                    anchors_[rightnid]->set_nodeptr(nodeRight);
                    anchors_[rightnid]->update_min_max();
                    guard->deguardAllNode();
                    delete split_bound_high;
                    delete split_bound_low;
                    return res;
                }
                uint64_t secondaryRightnid = find_split_target(rightnid,indexKey);
                AnchorNode* secondaryNodeRight;
                guard->guardNode(anchors_[secondaryRightnid],WRITE_LOCK);
                if(anchors_[secondaryRightnid]->occupied_){
                    guard->deguardAllNode();
                    guard->retry();
                    delete split_bound_high;
                    delete split_bound_low;
                    return false;
                }
                secondaryNodeRight = nodepool->new_node(secondaryRightnid);
                bool res = secondaryNodeRight->insert(key,key_len,tuple,tuple_len);
                anchors_[idx]->update_min_max();
                anchors_[rightnid]->set_nodeptr(nodeRight);
                anchors_[rightnid]->update_min_max();
                anchors_[secondaryRightnid]->set_nodeptr(secondaryNodeRight);
                anchors_[secondaryRightnid]->update_min_max();
                guard->deguardAllNode();
                delete split_bound_high;
                delete split_bound_low;
                return res;
            }
        }else{
            rightnid = find_split_target(idx,indexKey);
            if(rightnid>=anchor_cnt_){
                rightnid = find_split_target(idx,indexKey);
            }
            guard->guardNode(anchors_[rightnid],WRITE_LOCK);
            nodeRight = nodepool->new_node(rightnid);
            split_bound_low_key = lowbound_of_anchor(rightnid);
            split_bound_high_key = highbound_of_anchor(rightnid);
            memcpy(split_bound_low,&split_bound_low_key,sizeof(uint64_t));
            memcpy(split_bound_high,&split_bound_high_key,sizeof(uint64_t));
            node->payloadSplit(nodeRight,split_bound_low,sizeof(uint64_t));
            // 没有溯源直接得到的新的nodeRight
            // 判断归属来区别插入到node还是nodeRight
            if(cmpKey(key,key_len,split_bound_low,sizeof(uint64_t))>=0){
                assert(nodeRight->canInsert(key_len,tuple_len));
                bool res = nodeRight->insert(key,key_len,tuple,tuple_len);
                anchors_[idx]->update_min_max();
                anchors_[rightnid]->set_nodeptr(nodeRight);
                anchors_[rightnid]->update_min_max();
                guard->deguardAllNode();
                delete split_bound_high;
                delete split_bound_low;
                return res;
            }
        }
    }
}

bool BTreeAnchor::remove_kv(const Byte* key, const uint64_t key_len, AnchorNodePool* nodepool, anchorThdGuard* &guard){
    IndexKey indexKey;
    AnchorNode* node =nullptr;
    uint64_t idx = 0;
    uint64_t chidx = 0;
    memcpy(&indexKey,key,sizeof(IndexKey));
    if(!slow_find_node(indexKey,node,idx,chidx,guard,WRITE_LOCK)){
        guard->deguardAllNode();
        return false;
    }
    // 在溯源的过程中因为是先放锁然后再加锁，所以这里需要添加check_incurrent来判断是否正确找到了目标操作的节点
    if(!check_incurrent(idx,chidx,guard)){
        guard->deguardAllNode();
        guard->retry();
        return false;
    }
    if(node->count_==0){
        guard->deguardAllNode();
        return false;
    }else if(node->count_==1){
        // assert(memcmp(key,node->getKey(0),key_len)==0);
        bool res = node->remove(key,key_len);
        if(res==false){
            guard->deguardAllNode();
            return false;
        }
        if(node->count_==0){
            // delete node;
            // anchors_[idx]->set_empty();
            guard->deguardAllNode();
            return res;
        }
        anchors_[idx]->update_min_max();
        guard->deguardAllNode();
        return res;
    }else{
        bool res = node->remove(key,key_len);
        anchors_[idx]->update_min_max();
        guard->deguardAllNode();
        return res;
    }
}

}