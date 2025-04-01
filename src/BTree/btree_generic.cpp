#include "btree_generic.h"
#include <iostream>
namespace btree{


BTreeGeneric::BTreeGeneric(TableID table_id){
    table_id_ = table_id;
    // root_ = nullptr;
}

BTreeGeneric::~BTreeGeneric(){
    
}

//初始化b+树结构为一个3层结构，每一层一个节点
void BTreeGeneric::init(BTreeNodePool* nodepool){
    const Byte infKey_data[INFKEY_SIZE]={0};
    const Byte inf_data[INF_SIZE]={0};
    const Byte* infKey = infKey_data;
    const Byte* inf = inf_data;
    BTreeNode* initroot = nodepool->new_node(root_id());
    BTreeNode* initleaf = nodepool->new_node(next_leaf());
    BTreeNode* initinner = nodepool->new_node(next_inner());
    initleaf->insert(infKey,INFKEY_SIZE,inf,INF_SIZE);
    initleaf->setNodeKey(initleaf->getSlotKey(0));

    Byte* nidByte = (Byte*)malloc(sizeof(bid_t));
    initleaf->nid2byte(nidByte);
    initinner->insert(infKey,INFKEY_SIZE,nidByte,sizeof(bid_t));
    initinner->setNodeKey(initinner->getSlotKey(0));
    initinner->nid2byte(nidByte);
    initroot->insert(infKey,INFKEY_SIZE,nidByte,sizeof(bid_t));
    initroot->setNodeKey(initroot->getSlotKey(0));
    free(nidByte);
}

void BTreeGeneric::initIterator(BTreeNodePool* nodepool){
    BTreeNode* leaf = get_root(nodepool);
    BTreeNode* parent = leaf;
    while (true)
    {
        parent = leaf;
        Byte* nid_payload = nullptr;
        uint64_t nid_len = sizeof(bid_t);
        nid_payload = parent->getPayload(0);
        nid_len = parent->getPayloadLen(0);
        // bid_t chid = *(bid_t*)(nid_payload);
        bid_t chid;
        memcpy(&chid,nid_payload,sizeof(bid_t));
        leaf = nodepool->get_node(chid);
        if(IS_LEAF(leaf->nid_)){
            break;
        }else{
        }
    }
}

inline bid_t BTreeGeneric::next_leaf(){
    schema_.lock_.GetWriteLock();
    bid_t nid = schema_.next_leaf_node_id;
    schema_.next_leaf_node_id++;
    schema_.lock_.ReleaseWriteLock();
    return nid;
}

inline bid_t BTreeGeneric::next_inner(){
    schema_.lock_.GetWriteLock();
    bid_t nid = schema_.next_inner_node_id;
    schema_.next_inner_node_id++;
    schema_.lock_.ReleaseWriteLock();
    return nid;
}

inline bid_t BTreeGeneric::root_id(){
    schema_.lock_.GetReadLock();
    bid_t root_id = this->schema_.root_node_id;
    schema_.lock_.ReleaseReadLock();
    return root_id;
}

inline void BTreeGeneric::update_root(bid_t newrootid){
    schema_.lock_.GetWriteLock();
    this->schema_.root_node_id = newrootid;
    schema_.lock_.ReleaseWriteLock();
}

inline BTreeNode* BTreeGeneric::get_root(BTreeNodePool* nodepool){
    return nodepool->get_node(this->schema_.root_node_id);
}

// 仅仅去找到leafnode并对其加锁
BTreeNode* BTreeGeneric::lock_to_leaf(const Byte* key, const uint64_t key_len, const NODE_LOCK_STATUS lock_type, BTreeNodePool* nodepool, btreeThdGuard* &guard){
    BTreeNode* leaf = get_root(nodepool);
    BTreeNode* parent = leaf;
    guard->guardNode(parent,lock_type);
    while (true)
    {
        parent = leaf;
        Byte* nid_payload = nullptr;
        uint64_t nid_len = sizeof(bid_t);
        if(!parent->findInner(key,key_len,nid_payload,nid_len)){
            parent = guard->path_parent(leaf);
            parent->findInner(key,key_len,nid_payload,nid_len);
            guard->deguardAllNode();
            return nullptr;
        }
        // bid_t chid = *(bid_t*)(nid_payload);
        bid_t chid;
        memcpy(&chid,nid_payload,sizeof(bid_t));
        leaf = nodepool->get_node(chid);
        if(IS_LEAF(leaf->nid_)){
            guard->guardNode(leaf,lock_type);
            break;
        }else{
            guard->guardNode(leaf,lock_type);
        }
    }
    return leaf;
}

// 对于lock_parent来说只对于insert和remove有关
BTreeNode* BTreeGeneric::search_parent(const bid_t nid, BTreeNodePool* nodepool, btreeThdGuard* &guard){
    // BTreeNode* target = nodepool->get_node(nid);
    // Byte* key = target->getKey(0);
    // uint64_t key_len = target->getKeyLen(0);
    // BTreeNode* leaf = get_root(nodepool);
    // BTreeNode* parent = leaf;
    // while(true){
    //     parent = leaf;
    //     Byte* nid_payload = nullptr;
    //     uint64_t nid_len = sizeof(bid_t);
    //     if((!parent->findInner(key,key_len,nid_payload,nid_len))&&nid_payload==nullptr){
    //         return nullptr;
    //     }
    //     bid_t chid = *(bid_t*)(nid_payload);
    //     leaf = nodepool->get_node(chid);
    //     if(leaf->nid_==nid){// 此时parent是锁住的
    //         break;
    //     }
    // }
    // return parent;
    BTreeNode* target = nodepool->get_node(nid);
    BTreeNode* parent = guard->path_parent(target);
    if(parent==nullptr){
        assert(parent!=nullptr);
    }
    return parent;
}

bool BTreeGeneric::insert_leaf(const Byte* key,const uint64_t key_len,const Byte* tuple, uint64_t tuple_len,BTreeNodePool* nodepool, btreeThdGuard* &guard){
    // initIterator(nodepool);
    assert(key_len>0);
    assert(tuple_len>0);
    BTreeNode* leaf = lock_to_leaf(key,key_len,WRITE_LOCK,nodepool,guard);
    // debug 用
    if(leaf==nullptr){
        // leaf = lock_to_leaf(key,key_len,WRITE_LOCK,nodepool);
        printf("bug : no target leaf\n");
        assert(leaf!=nullptr);
        guard->deguardAllNode();
        return false;
    }
    //
    if(leaf->canInsert(key_len,tuple_len)){
        guard->deguard_to_Node(leaf);
        bool res = leaf->insert(key,key_len,tuple,tuple_len);
        //DEBUG res
        if(res==false){
            //printf("here\n");
        }
        guard->deguardAllNode();
        return res;
    }else{
        BTreeNode* parent = search_parent(leaf->nid(), nodepool, guard);
        bid_t rightnid = next_leaf();
        BTreeNode* nodeRight = nodepool->new_node(rightnid);
        leaf->payloadSplit(nodeRight); //这里有可能会触发分裂机制但是实际上后续不会进行插入（pk）
        if(leaf->count_==0&&nodeRight->count_==0){
            // payload之后必定是可以插入的
            assert(leaf->canInsert(key_len,tuple_len));
            guard->deguard_to_Node(leaf);
            bool res = leaf->insert(key,key_len,tuple,tuple_len);
            if(res==false){
                //printf("here\n");
            }
            guard->deguardAllNode();
            return res;
        }else if(nodeRight->count_==0){
            //由于payload的大小可能导致一个node里面只能存下一个payload,所以导致产生的nodeRight里面可能为空
            if(leaf->canInsert(key_len,tuple_len)){
                guard->deguard_to_Node(leaf);
                bool res = leaf->insert(key,key_len,tuple,tuple_len);
                guard->deguardAllNode();
                return res;
            }else{
                bool res = nodeRight->insert(key,key_len,tuple,tuple_len);
                if(!res){
                    guard->deguardAllNode();
                    return res;
                }
                nodeRight->setNodeKey(nodeRight->getSlotKey(0));
                Byte* nidByte = (Byte*)malloc(sizeof(bid_t));
                nodeRight->nid2byte(nidByte);
                res = insert_inner(parent,nodeRight->getKey(0),nodeRight->getKeyLen(0),nidByte,sizeof(bid_t),nodepool,guard);
                if(res==false){
                    //printf("here\n");
                }
                free(nidByte);
                guard->deguardAllNode();
                return res;
            }
        }else if(leaf->count_==0){
            assert(nodeRight->canInsert(key_len,tuple_len));
            // 这里的nodeRight是可以插入的
            guard->deguard_to_Node(leaf);
            bool res = nodeRight->insert(key,key_len,tuple,tuple_len);
            if(res==false){
                //printf("here\n");
            }
            leaf->removeAllSlot();
            nodeRight->copyKVRange(leaf,0,0,nodeRight->count_);
            guard->deguardAllNode();
            return res;
        }
        if(cmpKey(key,key_len,nodeRight->getKey(0),nodeRight->getKeyLen(0))>=0){
            nodeRight->insert(key,key_len,tuple,tuple_len);
        }else{
            leaf->insert(key,key_len,tuple,tuple_len);
        }
        Byte* nidByte = (Byte*)malloc(sizeof(bid_t));
        nodeRight->nid2byte(nidByte);
        bool res = insert_inner(parent,nodeRight->getKey(0),nodeRight->getKeyLen(0),nidByte,sizeof(bid_t),nodepool,guard);
        if(res==false){
            //printf("here\n");
        }
        free(nidByte);
        guard->deguardAllNode();
        return res;
    }
}

bool BTreeGeneric::pipeup(BTreeNode* target,const Byte* key,const uint64_t key_len,const Byte* payload, uint64_t payload_len, BTreeNodePool* nodepool){
    
    // root              root
    //                     |
    //             root_Copy  root_split
    // step 1 : root 分一半给nodesplit;
    bid_t newnodenid = next_inner();
    BTreeNode* nodeSplit = nodepool->new_node(newnodenid);
    Byte* nidByte = (Byte*)malloc(sizeof(bid_t));
    target->payloadSplit(nodeSplit);
    newnodenid = next_inner();
    BTreeNode* rootCopy  = nodepool->new_node(newnodenid);
    target->copyKVRange(rootCopy,0,0,target->count_);
    target->removeAllSlot();
    if(cmpKey(key,key_len,nodeSplit->getKey(0),nodeSplit->getKeyLen(0))>=0){
        nodeSplit->insert(key,key_len,payload,payload_len);
    }else{
        rootCopy->insert(key,key_len,payload,payload_len);
    }
    rootCopy->nid2byte(nidByte);
    rootCopy->setNodeKey(rootCopy->getSlotKey(0));
    bool res = target->insert(rootCopy->getKey(0),rootCopy->getKeyLen(0),nidByte,sizeof(bid_t));
    nodeSplit->nid2byte(nidByte);
    res = target->insert(nodeSplit->getKey(0),nodeSplit->getKeyLen(0),nidByte,sizeof(bid_t));
    free(nidByte);
    return res;
}

bool BTreeGeneric::insert_inner(BTreeNode* target,const Byte* key,const uint64_t key_len,const Byte* payload, uint64_t payload_len, BTreeNodePool* nodepool, btreeThdGuard* &guard){
    assert(key_len>0);
    assert(payload_len>0);
    if(target->canInsert(key_len,payload_len)){
        guard->deguard_to_Node(target);
        bool res = target->insert(key,key_len,payload,payload_len);
        if(res==false){
            //printf("here\n");
        }
        return res;
    }else{
        if(target->nid_==root_id()){
            //pipeup
            // bid_t newnodenid = next_inner();
            // BTreeNode* nodeRoot = nodepool->new_node(newnodenid);
            // Byte* nidByte = (Byte*)malloc(sizeof(bid_t));

            // target->nid2byte(nidByte);
            // bool res = nodeRoot->insert(key,key_len,payload,payload_len);
            // res = nodeRoot->insert(target->getKey(0),target->getKeyLen(0),nidByte,sizeof(bid_t));
            // update_root(newnodenid);
            // free(nidByte);
            bool res = pipeup(target,key,key_len,payload,payload_len,nodepool);
            guard->deguard_to_Node(target);
            return res;
        }
        BTreeNode* parent = search_parent(target->nid_,nodepool,guard);
        bid_t rightnid = next_inner();
        BTreeNode* nodeRight = nodepool->new_node(rightnid);
        target->payloadSplit(nodeRight);
        bool res = false;
        if(cmpKey(key,key_len,nodeRight->getKey(0),nodeRight->getKeyLen(0))>=0){
            res = nodeRight->insert(key,key_len,payload,payload_len);
        }else{
            res = target->insert(key,key_len,payload,payload_len);
        }
        if(res==false){
            //printf("here\n");
        }
        Byte* nidByte = (Byte*)malloc(sizeof(bid_t));
        nodeRight->nid2byte(nidByte);
        res = insert_inner(parent,nodeRight->getKey(0),nodeRight->getKeyLen(0),nidByte,sizeof(bid_t),nodepool,guard);
        if(res==false){
            //printf("here\n");
        }
        free(nidByte);
        guard->deguard_to_Node(target);
        return res;
    }
}

bool BTreeGeneric::find_kv(const Byte* key, const uint64_t key_len, Byte*& payload, uint64_t& payload_len, BTreeNodePool* nodepool, btreeThdGuard* &guard){
    BTreeNode* leaf = lock_to_leaf(key,key_len,READ_LOCK,nodepool,guard);
    if(leaf==nullptr){
        guard->deguardAllNode();
        return false;
    }
    if(leaf->findLeaf(key,key_len,payload,payload_len)){
        guard->deguardAllNode();
        return true;
    }else{
        guard->deguardAllNode();
        return false;
    }
}

bool BTreeGeneric::remove_kv(const Byte* key, const uint64_t key_len, BTreeNodePool* nodepool, btreeThdGuard* &guard){
    BTreeNode* leaf = lock_to_leaf(key,key_len,WRITE_LOCK,nodepool,guard);
    // debug 用
    if(leaf==nullptr){
        // leaf = lock_to_leaf(key,key_len,WRITE_LOCK,nodepool);
        printf("bug : no target leaf\n");
        assert(leaf!=nullptr);
        guard->deguardAllNode();
        return false;
    }
    //
    if(leaf->count_==0){
        // printf("bug : no content leaf\n");
        guard->deguardAllNode();
        return false;
    }
    else if(leaf->count_==1){
        // assert(memcmp(key,leaf->getKey(0),key_len)==0);
        bool res = leaf->remove(key,key_len);
        if(res==false){
            guard->deguardAllNode();
            return false;
        }
        // 不对中间节点进行删除
        // BTreeNode* parent = search_parent(leaf->nid(),nodepool,guard);
        // Byte* leafidByte = (Byte*)malloc(sizeof(bid_t));
        // leaf->nid2byte(leafidByte);
        // Byte* leafNodeKey = (Byte*)malloc(sizeof(uint64_t));
        // memcpy(leafNodeKey,&(leaf->node_key_),sizeof(uint64_t));
        // res = remove_kv_inner(parent,leafNodeKey,sizeof(uint64_t),leafidByte,nodepool,guard);
        // free(leafNodeKey);
        // free(leafidByte);
        guard->deguardAllNode();
        return res;
    }else{
        guard->deguard_to_Node(leaf);
        bool res = leaf->remove(key,key_len);
        if(res==false){
            //printf("here\n");
        }
        assert(leaf->count_>=1);
        guard->deguardAllNode();
        return res;
    }
}

bool BTreeGeneric::remove_kv_inner(BTreeNode* target, const Byte* key, const uint64_t key_len, const Byte* chidByte, BTreeNodePool* nodepool, btreeThdGuard* &guard){
    if(target->count_==0){
        return false;
    }else if(target->count_==1){ // 这个节点也要删除了
        if(target->nid_==root_id()){
            printf("tree clean");
        }else{
            if(memcmp(chidByte,target->getPayload(0),target->getPayloadLen(0))==0){
                target->remove(target->getKey(0),target->getKeyLen(0));
                BTreeNode* parent = search_parent(target->nid(),nodepool,guard);
                Byte* targetidByte = (Byte*)malloc(sizeof(bid_t));
                target->nid2byte(targetidByte);
                Byte* innerNodeKey = (Byte*)malloc(sizeof(uint64_t));
                memcpy(innerNodeKey,&(target->node_key_),sizeof(uint64_t));
                bool res = remove_kv_inner(parent,innerNodeKey,sizeof(uint64_t),targetidByte,nodepool,guard);
                free(innerNodeKey);
                free(targetidByte);
                guard->deguard_to_Node(target);
                return true;
            }else{
                guard->deguard_to_Node(target);
                return false;
            }
        }
        return true;
    }else{
        guard->deguard_to_Node(target);
        uint64_t chidx = 0;
        bool find = false;
        for (chidx = 0; chidx < target->count_; chidx++)
        {
            if(memcmp(chidByte,target->getPayload(chidx),target->getPayloadLen(chidx))==0){ // TODO: 优化
                find = true;
                break;
            }
        }
        if(find){
            target->remove(target->getKey(chidx),target->getKeyLen(chidx));
            return true;
        }else{
            return false;
        }
    }
}

}