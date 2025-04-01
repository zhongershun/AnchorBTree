#include "anchor_thdGuard.h"

namespace anchorBTree{

anchorThdGuard::anchorThdGuard(){
    guardCount_=0;
    retryCount_=0;
}

anchorThdGuard::~anchorThdGuard(){
    for (int i = 0; i < guardCount_; i++)
    {
        if(lock_path_[i].lock_type_==WRITE_LOCK){
            lock_path_[i].anchorptr_->write_unlock();
        }else if(lock_path_[i].lock_type_==READ_LOCK){
            lock_path_[i].anchorptr_->read_unlock();
        }
    }
    guardCount_=0;
    guardCount_=0;
    retryCount_=0;
}

inline void anchorThdGuard::push_back(Anchor* anchor, NODE_LOCK_STATUS lock_type){
    lock_path_[guardCount_].anchorptr_ = anchor;
    lock_path_[guardCount_].lock_type_ = lock_type;
    guardCount_++;
}

void anchorThdGuard::guardNode(Anchor* anchor, NODE_LOCK_STATUS lock_type){
    // assert(checkGuard(anchor,lock_type)==false);
    if(lock_type==FREE){
        // do nothing
    }else if(lock_type==WRITE_LOCK){
        // lock all path
        anchor->write_lock();
        push_back(anchor,lock_type);
    }else if(lock_type==READ_LOCK){
        // lock child and release parent
        anchor->read_lock();
        push_back(anchor,lock_type);
    }
}

void anchorThdGuard::deguardAllNode(){
    for (int i = 0; i < guardCount_; i++)
    {
        if(lock_path_[i].lock_type_==WRITE_LOCK){
            lock_path_[i].anchorptr_->write_unlock();
        }else if(lock_path_[i].lock_type_==READ_LOCK){
            lock_path_[i].anchorptr_->read_unlock();
        }
    }
    guardCount_=0;
    // while (guardCount_!=0)
    // {
    //     if(lock_path_[guardCount_-1].lock_type_==WRITE_LOCK){
    //         lock_path_[guardCount_-1].anchorptr_->write_unlock();
    //     }else if(lock_path_[guardCount_-1].lock_type_==READ_LOCK){
    //         lock_path_[guardCount_-1].anchorptr_->read_unlock();
    //     }
    //     guardCount_--;
    // }
}

void anchorThdGuard::deguardNode(Anchor* target){
    // assert(checkGuard(target)==true);
    int i = 0;
    while (true)
    {
        if(lock_path_[i].anchorptr_==target){
            if(lock_path_[i].lock_type_==WRITE_LOCK){
                lock_path_[i].anchorptr_->write_unlock();
            }else if(lock_path_[i].lock_type_==READ_LOCK){
                lock_path_[i].anchorptr_->read_unlock();
            }
            memmove(lock_path_+i,lock_path_+i+1,sizeof(GuardLock)*(guardCount_-i-1));
            guardCount_--;
            break;       
        }
        i++;
    }
}

}