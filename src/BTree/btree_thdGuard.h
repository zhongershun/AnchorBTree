#ifndef BTREE_THDGUARD_H
#define BTREE_THDGUARD_H

#include <vector>
#include <cassert>
#include "btree_node.h"

namespace btree{


#define MAX_TREE_PATH_LEN 5

class btreeThdGuard{
public:
    struct GuardLock{
        NODE_LOCK_STATUS lock_type_;
        BTreeNode* nodeptr_;
    };
    btreeThdGuard();
    ~btreeThdGuard();
    void guardNode(BTreeNode* node, NODE_LOCK_STATUS lock_type);
    void deguardAllNode();
    void deguardNode(BTreeNode* lock_type);
    // 只会对write lock有用
    void deguard_to_Node(BTreeNode* target);
    BTreeNode* path_parent(BTreeNode* target){
        int idx = 0;
        BTreeNode* parent = nullptr;
        BTreeNode* child = lock_path_[idx].nodeptr_;
        bool res = false;
        while (idx<guardCount_)
        {
            if(child->nid()==target->nid()){
                res = true;
                break;
            }
            parent = child;
            idx++;
            child = lock_path_[idx].nodeptr_;
        }
        if(res){
            return parent;
        }else{
            return nullptr;
        }
    };
    BTreeNode* get(int idx){return lock_path_[idx].nodeptr_;};
    int getSize(){return guardCount_;};

    //debug
    bool checkGuard(BTreeNode* anchor,NODE_LOCK_STATUS lock_type){
        for (int i = 0; i < guardCount_; i++)
        {
            if(lock_path_[i].nodeptr_==anchor && lock_path_[i].lock_type_==lock_type){
                return true;
            }
        }
        return false;
    }
    bool checkGuard(BTreeNode* anchor){
        for (int i = 0; i < guardCount_; i++)
        {
            if(lock_path_[i].nodeptr_==anchor){
                return true;
            }
        }
        return false;
    }
private:
    GuardLock lock_path_[MAX_TREE_PATH_LEN];
    int  guardCount_;
protected:
    inline void push_back(BTreeNode* node, NODE_LOCK_STATUS lock_type);
};

}

#endif