#ifndef ANCHOR_THDGUARD_H
#define ANCHOR_THDGUARD_H

#include <vector>
#include "anchor_node.h"

namespace anchorBTree{

#define RETYR_COUNT 3
#define MAX_TREE_PATH_LEN 10

enum NODE_LOCK_STATUS{
    FREE,
    READ_LOCK,
    WRITE_LOCK,
};

class anchorThdGuard{
public:
    struct GuardLock{
        NODE_LOCK_STATUS lock_type_;
        Anchor* anchorptr_;
    };
    anchorThdGuard();
    ~anchorThdGuard();
    void guardNode(Anchor* anchor, NODE_LOCK_STATUS lock_type);
    void deguardAllNode();
    // 只会对write lock有用
    void deguardNode(Anchor* target);
    Anchor* get(int idx){return lock_path_[idx].anchorptr_;};
    int getSize(){return guardCount_;};
    int getRetry(){return retryCount_;};
    void retry(){retryCount_++;};
    void setretry(){retryCount_=0;};

    //debug
    bool checkGuard(Anchor* anchor,NODE_LOCK_STATUS lock_type){
        for (int i = 0; i < guardCount_; i++)
        {
            if(lock_path_[i].anchorptr_==anchor && lock_path_[i].lock_type_==lock_type){
                return true;
            }
        }
        return false;
    }
    bool checkGuard(Anchor* anchor){
        for (int i = 0; i < guardCount_; i++)
        {
            if(lock_path_[i].anchorptr_==anchor){
                return true;
            }
        }
        return false;
    }
private:
    GuardLock lock_path_[MAX_TREE_PATH_LEN];
    int  guardCount_;
    int  retryCount_;
protected:
    inline void push_back(Anchor* anchor, NODE_LOCK_STATUS lock_type);
};

}

#endif