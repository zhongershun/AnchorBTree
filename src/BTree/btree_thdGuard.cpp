#include "btree_thdGuard.h"

namespace btree{

btreeThdGuard::btreeThdGuard(){
    guardCount_=0;
}

btreeThdGuard::~btreeThdGuard(){
    for (int i = 0; i < guardCount_; i++)
    {
        if(lock_path_[i].lock_type_==WRITE_LOCK){
            lock_path_[i].nodeptr_->write_unlock();
        }else if(lock_path_[i].lock_type_==READ_LOCK){
            lock_path_[i].nodeptr_->read_unlock();
        }
    }
    guardCount_=0;
}

inline void btreeThdGuard::push_back(BTreeNode* node, NODE_LOCK_STATUS lock_type){
    lock_path_[guardCount_].nodeptr_ = node;
    lock_path_[guardCount_].lock_type_ = lock_type;
    guardCount_++;
}

void btreeThdGuard::guardNode(BTreeNode* node, NODE_LOCK_STATUS lock_type){
    assert(checkGuard(node,lock_type)==false);
    if(lock_type==FREE){
        // do nothing
    }else if(lock_type==WRITE_LOCK){
        // lock all path
        node->write_lock();
        push_back(node,lock_type);
    }else if(lock_type==READ_LOCK){
        // lock child and release parent
        node->read_lock();
        push_back(node,lock_type);
    }
}

void btreeThdGuard::deguardAllNode(){
    for (int i = 0; i < guardCount_; i++)
    {
        if(lock_path_[i].lock_type_==WRITE_LOCK){
            lock_path_[i].nodeptr_->write_unlock();
        }else if(lock_path_[i].lock_type_==READ_LOCK){
            lock_path_[i].nodeptr_->read_unlock();
        }
    }
    guardCount_=0;
}

void btreeThdGuard::deguardNode(BTreeNode* target){
    assert(checkGuard(target)==true);
    int i = 0;
    while(true){
        if((lock_path_[guardCount_-1].nodeptr_)==target){
            if(lock_path_[guardCount_-1].lock_type_==WRITE_LOCK){
                lock_path_[guardCount_-1].nodeptr_->write_unlock();
            }else if(lock_path_[guardCount_-1].lock_type_==READ_LOCK){
                lock_path_[guardCount_-1].nodeptr_->read_unlock();
            }
            guardCount_--;
            break;
        }
        i++;
    }
}

void btreeThdGuard::deguard_to_Node(BTreeNode* target){
    while (((lock_path_[0]).nodeptr_!=target)&&(guardCount_!=0))
    {
        if(lock_path_[0].lock_type_==WRITE_LOCK){
            (lock_path_[0].nodeptr_)->write_unlock();
        }else if(lock_path_[0].lock_type_==READ_LOCK){
            (lock_path_[0].nodeptr_)->read_unlock();
        }
        memmove(lock_path_,lock_path_+1,sizeof(GuardLock)*(guardCount_-1));
        guardCount_--;
    }
}

}