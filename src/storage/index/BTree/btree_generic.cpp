#include "storage/index/BTree/btree_generic.h"
#include "util/daset_debug_logger.h"
namespace daset{

auto BTreeContext::IsRootPage(page_id_t page_id) -> bool{
    return page_id == root_page_id_;
}

void BTreeContext::CtxReadPage(ReadPageGuard&& read_page_guard){
  read_set_.emplace_back(std::move(read_page_guard));
}

void BTreeContext::CtxWritePage(WritePageGuard&& write_page_guard){
  write_set_.emplace_back(std::move(write_page_guard));
}

void BTreeContext::CtxPopBackReadPage(){
  read_set_.pop_back();
}

void BTreeContext::CtxPopBackWritePage(){
  write_set_.pop_back();
}

void BTreeContext::CtxPopFrontReadPage(page_id_t page_id){
  while (read_set_.front().GetPageID()!=page_id)
  {
    read_set_.pop_front();
  }
}

void BTreeContext::CtxPopFrontWritePage(page_id_t page_id){
  while (write_set_.front().GetPageID()!=page_id)
  {
    write_set_.pop_front();
  }
}

auto BTreeContext::ParentPage(){
  int size = write_set_.size();
  return write_set_[size-2].AsMut<BTreePage>();
}

BTreeContext::~BTreeContext(){
    while (!read_set_.empty())
    {
        read_set_.pop_back();
    }
    while (!write_set_.empty())
    {
        write_set_.pop_back();
    }
    #if DASET_DEBUG==true
    path_.clear();
    #endif
}

BTreeGeneric::BTreeGeneric(TableID table_id, page_id_t header_page_id,
    BufferPoolInstance* bpm, const PageKeyCompator comparator)
    : table_id_(table_id),header_page_id_(header_page_id),
    bpm_(bpm),comparator_(comparator){
    WritePageGuard guard = bpm_->WritePage(header_page_id_);
    auto root_page = guard.AsMut<BTreeSchemaPage>();
    root_page->root_page_id_ = DASET_INVALID_PAGE_ID;
}
BTreeGeneric::~BTreeGeneric(){
    header_page_id_ = DASET_INVALID_PAGE_ID;
}

auto BTreeGeneric::IsEmpty() const -> bool{
    auto header_guard = bpm_->ReadPage(header_page_id_);
    auto header_page = header_guard.As<BTreeSchemaPage>();
    page_id_t root_page_id = header_page->root_page_id_;
    return root_page_id==DASET_INVALID_PAGE_ID;
}

auto BTreeGeneric::GetRootPageId() -> page_id_t{
    auto header_guard = bpm_->ReadPage(header_page_id_);
    auto header_page = header_guard.As<BTreeSchemaPage>();
    page_id_t root_page_id = header_page->root_page_id_;
    return root_page_id;
}

auto BTreeGeneric::Search(const byte* key,const size_t key_len, byte*& payload, size_t& payload_len) -> bool{
    BTreeContext ctx;
    ctx.op_type_=TREE_OP_TYPE::READ;
    auto leaf = tracerseToLeaf(key,key_len,payload_len,ctx);
    if(leaf==nullptr){
        return false;
    }else{
        if(leaf->search(key,key_len,payload,payload_len,comparator_)){
            return true;
        }
        return false;
    }
}

auto BTreeGeneric::Insert(const byte* key,const size_t key_len,const byte* tuple, size_t tuple_len) -> bool{
    BTreeContext ctx;
    ctx.op_type_=TREE_OP_TYPE::INSERT;
    return InsertLeaf(key,key_len,tuple,tuple_len,ctx);
}

auto BTreeGeneric::Remove(const byte* key,const size_t key_len) -> bool{
    BTreeContext ctx;
    ctx.op_type_=TREE_OP_TYPE::DELETE;
    return RemoveLeaf(key,key_len,ctx);
}

auto BTreeGeneric::tracerseToLeafMut(const byte* &key, const size_t key_len, const size_t payload_len, BTreeContext &context) -> BTreePage*{
    page_id_t page_id;
    {
        auto header_guard = bpm_->ReadPage(header_page_id_);
        auto header_page = header_guard.As<BTreeSchemaPage>();
        page_id = header_page->root_page_id_;
        context.root_page_id_ = page_id;
    }
    // header not lock
    if(page_id==DASET_INVALID_PAGE_ID){
        return nullptr;
    }else{
        if(context.op_type_==TREE_OP_TYPE::INSERT){
            // size_t payload_len_mut;
            while (true)
            {
                auto guard = bpm_->WritePage(page_id);
                auto page = guard.AsMut<BTreePage>();
                #if DASET_DEBUG == true
                context.path_.push_back(page);
                if(page->page_type_==IndexPageType::INVALID_INDEX_PAGE){
                    LOG_ERROR("INVALID_INDEX_PAGE, page_id : "+std::to_string(page_id));
                    #if DASET_DEBUG
                    while (true){}
                    #endif
                }
                #endif
                context.CtxWritePage(std::move(guard));
                // if(page->page_type_==IndexPageType::LEAF_PAGE){
                //     payload_len_mut = DASET_LONG_PAYLOAD_LEN;
                // }else{
                //     payload_len_mut = sizeof(page_id_t);
                // }
                if(page->canInsert(key_len,payload_len)){
                    context.CtxPopFrontWritePage(page_id);
                }
                if(page->page_type_==IndexPageType::LEAF_PAGE){
                    return page;
                }else{
                    page_id_t inner_val;
                    // byte* inner_val_byte = new byte[sizeof(page_id_t)];
                    // page->search(key,key_len,inner_val_byte,sizeof(page_id_t),comparator_);
                    // memcpy(&inner_val,inner_val_byte,sizeof(page_id_t));
                    page->search(key,key_len,reinterpret_cast<byte*>(&inner_val),sizeof(page_id_t),comparator_);
                    // delete inner_val_byte;
                    page_id = inner_val;
                    continue;
                }
            }
        }else if(context.op_type_==TREE_OP_TYPE::DELETE){
            size_t payload_len_mut;
            while (true)
            {
                auto guard = bpm_->WritePage(page_id);
                auto page = guard.AsMut<BTreePage>();
                #if DASET_DEBUG == true
                context.path_.push_back(page);
                if(page->page_type_==IndexPageType::INVALID_INDEX_PAGE){
                    LOG_ERROR("INVALID_INDEX_PAGE, page_id : "+std::to_string(page_id));
                    #if DASET_DEBUG
                    while (true){}
                    #endif
                }
                #endif
                context.CtxWritePage(std::move(guard));
                if(page->page_type_==IndexPageType::LEAF_PAGE){// 删除操作不会考虑payload的长度，需要自行处理
                    payload_len_mut = DASET_LONG_PAYLOAD_LEN;
                }else{
                    payload_len_mut = sizeof(page_id_t);
                }
                if(page->canRemove(key_len,payload_len_mut)){
                    context.CtxPopFrontWritePage(page_id);
                }
                if(page->page_type_==IndexPageType::LEAF_PAGE){
                    return page;
                }else{
                    page_id_t inner_val;
                    // byte* inner_val_byte = new byte[sizeof(page_id_t)];
                    // page->search(key,key_len,inner_val_byte,sizeof(page_id_t),comparator_);
                    // memcpy(&inner_val,inner_val_byte,sizeof(page_id_t));
                    page->search(key,key_len,reinterpret_cast<byte*>(&inner_val),sizeof(page_id_t),comparator_);
                    // delete inner_val_byte;
                    page_id = inner_val;
                    continue;
                }
            }
        }
    }
}



auto BTreeGeneric::tracerseToLeaf(const byte* &key, const size_t key_len, const size_t payload_len, BTreeContext &context) const -> const BTreePage*{
    page_id_t page_id;
    {
        auto header_guard = bpm_->ReadPage(header_page_id_);
        auto header_page = header_guard.As<BTreeSchemaPage>();
        page_id = header_page->root_page_id_;
        context.root_page_id_ = page_id;
    }
    // header not lock
    if(page_id==DASET_INVALID_PAGE_ID){
        return nullptr;
    }else{
        while (true)
        {
            auto guard = bpm_->ReadPage(page_id);
            auto page = guard.As<BTreePage>();
            #if DASET_DEBUG ==true
            context.path_.push_back(page);
            if(page->page_type_==IndexPageType::INVALID_INDEX_PAGE){
                LOG_ERROR("INVALID_INDEX_PAGE, page_id : "+std::to_string(page_id));
                #if DASET_DEBUG
                return nullptr;
                #endif
            }
            #endif
            context.CtxReadPage(std::move(guard));
            context.CtxPopFrontReadPage(page_id);
            if(page->page_type_==IndexPageType::LEAF_PAGE){
                return page;
            }else{
                page_id_t inner_val;
                // byte* inner_val_byte = new byte[sizeof(page_id_t)];
                // page->search(key,key_len,inner_val_byte,sizeof(page_id_t),comparator_);
                // memcpy(&inner_val,inner_val_byte,sizeof(page_id_t));
                page->search(key,key_len,reinterpret_cast<byte*>(&inner_val),sizeof(page_id_t),comparator_);
                // delete inner_val_byte;
                page_id = inner_val;
                continue;
            }
        }
    }
}

auto BTreeGeneric::tracerseToLeaf(bool left, BTreeContext &context) const -> const BTreePage*{
// TODO: 范围查询时使用
    page_id_t page_id;
    {
        auto header_guard = bpm_->ReadPage(header_page_id_);
        auto header_page = header_guard.As<BTreeSchemaPage>();
        page_id = header_page->root_page_id_;
        context.root_page_id_ = page_id;
    }
    // header not lock
    if(page_id==DASET_INVALID_PAGE_ID){
        return nullptr;
    }else{
        while (true)
        {
            auto guard = bpm_->ReadPage(page_id);
            auto page = guard.As<BTreePage>();
            #if DASET_DEBUG ==true
            context.path_.push_back(page);
            if(page->page_type_==IndexPageType::INVALID_INDEX_PAGE){
                LOG_ERROR("INVALID_INDEX_PAGE, page_id : "+std::to_string(page_id));
                // guard.Drop();
                // guard = bpm_->ReadPage(page_id);
                // page = guard.As<BTreePage>();
                return nullptr;
            }
            #endif
            context.CtxReadPage(std::move(guard));
            context.CtxPopFrontReadPage(page_id);
            if(page->page_type_==IndexPageType::LEAF_PAGE){
                return page;
            }else{
                page_id_t inner_val;
                // byte* inner_val_byte = new byte[sizeof(page_id_t)];
                // page->search(key,key_len,inner_val_byte,sizeof(page_id_t),comparator_);
                // memcpy(&inner_val,inner_val_byte,sizeof(page_id_t));
                memcpy(&inner_val,page->getPayload(0),sizeof(page_id_t));
                // page->search(key,key_len,reinterpret_cast<byte*>(&inner_val),sizeof(page_id_t),comparator_);
                // delete inner_val_byte;
                page_id = inner_val;
                continue;
            }
        }
    }
    return nullptr;
}

auto BTreeGeneric::InsertLeaf(const byte* key,const size_t key_len,const byte* tuple, size_t tuple_len,BTreeContext& ctx) -> bool{
    auto leaf = tracerseToLeafMut(key,key_len,tuple_len,ctx);
    if(leaf==nullptr){
        // no node in tree
        auto header_guard = bpm_->WritePage(header_page_id_);
        auto header_page = header_guard.AsMut<BTreeSchemaPage>();
        page_id_t root_id = bpm_->NewPage();
        auto root_guard = bpm_->WritePage(root_id);
        auto root_page = root_guard.AsMut<BTreePage>();
        // ctx.CtxWritePage(std::move(root_guard));
        // ctx.root_page_id_ = root_id;
        root_page->init(root_id,IndexPageType::LEAF_PAGE);
        header_page->root_page_id_=root_id;
        if(!root_page->canInsert(key_len,tuple_len)){
            return false;
        }else{
            return root_page->insert(key,key_len,tuple,tuple_len,comparator_);
        }
    }
    #if DASET_DEBUG
    if(comparator_(key,leaf->getKey(0))<0){
        // printf("Debug pause");
        LOG_DEBUG("To insert key lower than page key0");
    }
    #endif
    page_id_t leaf_pid = ctx.write_set_.back().GetPageID();
    if(!leaf->canInsert(key_len,tuple_len)){
        if(!ctx.IsRootPage(leaf_pid)){
            #if DASET_DEBUG
            std::string info_str = "";
            info_str += "original : [";
            // printf("original : [");
            for (size_t i = 0; i < leaf->count_; i++)
            {
                page_key_t key;
                memcpy(&key,leaf->getKey(i),sizeof(page_key_t));
                // printf("%d ",key);
                info_str += std::to_string(key);
                info_str += " ";
            }
            // printf("]\n");
            info_str += "]";
            LOG_DEBUG(info_str);
            #endif
            page_id_t right_pid =  bpm_->NewPage();
            auto right_guard = bpm_->WritePage(right_pid);
            auto right_page = right_guard.AsMut<BTreePage>();
            ctx.CtxWritePage(std::move(right_guard));
            right_page->init(right_pid,IndexPageType::LEAF_PAGE);
            // page_key_t right_key;
            page_key_t right_key = leaf->payloadSplit(right_page);
            right_page->next_page_id_ = leaf->next_page_id_;
            leaf->next_page_id_ = right_pid;
            if(comparator_(key,reinterpret_cast<byte*>(&right_key))>=0){
                right_page->insert(key,key_len,tuple,tuple_len,comparator_);
            }else{
                leaf->insert(key,key_len,tuple,tuple_len,comparator_);
            }
            #if DASET_DEBUG == true
            info_str = "";
            info_str += "left : [";
            // printf("original : [");
            for (size_t i = 0; i < leaf->count_; i++)
            {
                page_key_t key;
                memcpy(&key,leaf->getKey(i),sizeof(page_key_t));
                // printf("%d ",key);
                info_str += std::to_string(key);
                info_str += " ";
            }
            info_str += "]";
            // printf("right : [");
            info_str += "right : [";
            for (size_t i = 0; i < right_page->count_; i++)
            {
                page_key_t key;
                memcpy(&key,right_page->getKey(i),sizeof(page_key_t));
                // printf("%d ",key);
                info_str += std::to_string(key);
                info_str += " ";
            }
            // printf("]\n");
            info_str += "]";
            LOG_DEBUG(info_str);
            #endif
            ctx.CtxPopBackWritePage(); //remove right guard
            ctx.CtxPopBackWritePage(); //remove leaf guard
            return InsertInner(right_page->getKey(0),DASET_PAGE_KEY_LEN,reinterpret_cast<byte*>(&right_pid),sizeof(page_id_t),ctx);
        }else{
            page_key_t right_key = Pipeup(leaf_pid,ctx);
            // [new_root_page, right_page]
            if(comparator_(key,reinterpret_cast<byte*>(&right_key))>=0){
                auto target_page = ctx.write_set_.back().AsMut<BTreePage>();
                return target_page->insert(key,key_len,tuple,tuple_len,comparator_);
            }else{
                ctx.CtxPopBackWritePage();
                auto target_page = ctx.write_set_.back().AsMut<BTreePage>();
                return target_page->insert(key,key_len,tuple,tuple_len,comparator_);
            }
        }
    }else{
        return leaf->insert(key,key_len,tuple,tuple_len,comparator_);
    }
    return false;
}

auto BTreeGeneric::InsertInner(const byte* key,const size_t key_len,const byte* tuple, size_t tuple_len,BTreeContext& ctx) -> bool{
    // write_set_ [... (parent), target]
    auto target = ctx.write_set_.back().AsMut<BTreePage>();
    auto target_pid = ctx.write_set_.back().GetPageID();
    if(!target->canInsert(key_len,tuple_len)){
        if(!ctx.IsRootPage(target_pid)){
            page_id_t right_pid =  bpm_->NewPage();
            auto right_guard = bpm_->WritePage(right_pid);
            auto right_page = right_guard.AsMut<BTreePage>();
            ctx.CtxWritePage(std::move(right_guard));
            right_page->init(right_pid,IndexPageType::INNER_PAGE);
            // write_set_ [... (parent), target, right]
            page_key_t right_key = target->payloadSplit(right_page);
            if(comparator_(key,reinterpret_cast<byte*>(&right_key))>=0){
                right_page->insert(key,key_len,tuple,tuple_len,comparator_);
            }else{
                target->insert(key,key_len,tuple,tuple_len,comparator_);
            }
            ctx.CtxPopBackWritePage();
            ctx.CtxPopBackWritePage();
            // write_set_ [... (parent)]
            return InsertInner(right_page->getKey(0),DASET_PAGE_KEY_LEN,reinterpret_cast<byte*>(&right_pid),sizeof(page_id_t),ctx);
        }else{
            page_key_t right_key = Pipeup(target_pid,ctx);
            // [new_root_page, right_page]
            if(comparator_(key,reinterpret_cast<byte*>(&right_key))>=0){
                auto right_page = ctx.write_set_.back().AsMut<BTreePage>();
                return right_page->insert(key,key_len,tuple,tuple_len,comparator_);
            }else{
                ctx.CtxPopBackWritePage();
                auto new_root_page = ctx.write_set_.back().AsMut<BTreePage>();
                return new_root_page->insert(key,key_len,tuple,tuple_len,comparator_);
            }
        }
    }else{
        return target->insert(key,key_len,tuple,tuple_len,comparator_);
    }
}

auto BTreeGeneric::Pipeup(page_id_t root_page_id, BTreeContext& ctx) -> page_key_t{
    // [rootpage]
    auto root_page = ctx.write_set_.back().AsMut<BTreePage>();
    auto header_guard = bpm_->WritePage(header_page_id_);
    auto header_page = header_guard.AsMut<BTreeSchemaPage>();
    page_id_t new_root_id = bpm_->NewPage();
    page_id_t right_pid = bpm_->NewPage();
    auto right_guard = bpm_->WritePage(right_pid);
    auto new_root_guard = bpm_->WritePage(new_root_id);
    // 这里pipe up需要保证root_id始终不变，应为header_page在获取到rootid之后立马防锁了，并发就需要逻辑上保证rootid不变
    auto right_page = right_guard.AsMut<BTreePage>();
    auto new_root_page = new_root_guard.AsMut<BTreePage>();
    if(root_page->page_type_==IndexPageType::LEAF_PAGE){
        right_page->init(right_pid,IndexPageType::LEAF_PAGE);
        new_root_page->init(new_root_id,IndexPageType::LEAF_PAGE);
        page_key_t right_key = root_page->payloadSplit(right_page);
        root_page->copyKVRange(new_root_page,0,0,root_page->count_);
        byte* root_key = new_root_page->getKey(0);
        root_page->removeAllSlot();
        right_page->next_page_id_ = root_page->next_page_id_;
        new_root_page->next_page_id_ = right_pid;
        root_page->next_page_id_=DASET_INVALID_PAGE_ID;
        root_page->page_type_=IndexPageType::INNER_PAGE;
        // 暗示对于空的根节点来讲插入两个是绝对允许的
        root_page->insert(root_key,DASET_PAGE_KEY_LEN,reinterpret_cast<byte*>(&new_root_id),sizeof(page_id_t),comparator_);
        root_page->insert(reinterpret_cast<byte*>(&right_key),DASET_PAGE_KEY_LEN,reinterpret_cast<byte*>(&right_pid),sizeof(page_id_t),comparator_);
        // [rootpage]
        ctx.write_set_.clear();
        ctx.CtxWritePage(std::move(new_root_guard));
        ctx.CtxWritePage(std::move(right_guard));
        return right_key;
    }else{
        right_page->init(right_pid,IndexPageType::INNER_PAGE);
        new_root_page->init(new_root_id,IndexPageType::INNER_PAGE);
        page_key_t right_key = root_page->payloadSplit(right_page);
        root_page->copyKVRange(new_root_page,0,0,root_page->count_);
        byte* root_key = new_root_page->getKey(0);
        root_page->removeAllSlot();
        // 暗示对于空的根节点来讲插入两个是绝对允许的
        root_page->insert(root_key,DASET_PAGE_KEY_LEN,reinterpret_cast<byte*>(&new_root_id),sizeof(page_id_t),comparator_);
        root_page->insert(reinterpret_cast<byte*>(&right_key),DASET_PAGE_KEY_LEN,reinterpret_cast<byte*>(&right_pid),sizeof(page_id_t),comparator_);
        // [rootpage]
        ctx.write_set_.clear();
        ctx.CtxWritePage(std::move(new_root_guard));
        ctx.CtxWritePage(std::move(right_guard));
        return right_key;
    }
}

auto BTreeGeneric::RemoveLeaf(const byte* key,const size_t key_len, BTreeContext& ctx) -> bool{
    if(IsEmpty()){
        return false;
    }
    auto leaf = tracerseToLeafMut(key,key_len,0,ctx);
    if(leaf==nullptr||leaf->count_==0){
        // no node in tree
        return false;
    }
    // write_set_ [...(parent), leaf]
    // size_t payload_len = leaf->getPayloadLen(0);
    // byte* payload = new byte[payload_len];
    int to_del_slotId = leaf->exist(key,key_len,comparator_);
    if(to_del_slotId==-1){
        return false;
    }
    if(!leaf->canRemove(key_len,leaf->getPayloadLen(to_del_slotId))){
        leaf->remove(key,key_len,comparator_);
        return CoalesceOrRedistribute(ctx);
    }else{
        return leaf->remove(key,key_len,comparator_);
    }
}

auto BTreeGeneric::CoalesceOrRedistribute(BTreeContext& ctx) -> bool{
    // write_set_ [...(parent), target]
    auto target_page = ctx.write_set_.back().AsMut<BTreePage>();
    page_id_t target_pid = ctx.write_set_.back().GetPageID();
    if(target_page->fillFactor()>=DASET_MERGE_FACTOR){
        return true;
    }
    if(ctx.IsRootPage(target_pid)){
        if(target_page->count_!=0){
            return true;
        }else{
            auto header_guard = bpm_->WritePage(header_page_id_);
            auto header_page = header_guard.AsMut<BTreeSchemaPage>();
            header_page->root_page_id_=DASET_INVALID_PAGE_ID;
            return true;
        }
    }
        // 当前节点不是根节点
        // write_set_ [(...)parent, target]
    auto parent_page = ctx.ParentPage();
    int target_idx = parent_page->valueIndex(reinterpret_cast<byte*>(&target_pid));
    // if(target_page->count_==0){
    //     // 似乎这里是没有比较进行操作的。后续会进行处理
    //     // 从parent中删除target需要考虑neighbor调整next_page_id
    //     if(target_idx==0){ // 是parent的第一个元素但是不一定是树上的最左的元素
    //         //do nothing
    //     }else{
    //         page_id_t neighbor_pid;
    //         memcpy(&neighbor_pid,parent_page->getPayload(target_idx-1),parent_page->getPayloadLen(target_idx-1));
    //         {// 没必要将neighbor_guard放入ctx中
    //             auto neighbor_guard = bpm_->WritePage(neighbor_pid);
    //             auto neighbor_page = neighbor_guard.AsMut<BTreePage>();
    //             neighbor_page->next_page_id_ = target_page->next_page_id_;
    //         }
    //     }
    //     if(parent_page->canRemove(DASET_PAGE_KEY_LEN,sizeof(page_id_t))){
    //         parent_page->remove(target_idx);
    //         ctx.CtxPopBackWritePage();
    //         // TODO : 删除页面的功能存在问题，跨父节点的相邻节点之间的next_page_id更新存在问题
    //         // bpm_->DeletePage(target_pid);
    //         return true;
    //     }else{
    //         parent_page->remove(target_idx);
    //         ctx.CtxPopBackWritePage();
    //         // bpm_->DeletePage(target_pid);
    //         return CoalesceOrRedistribute(ctx);
    //     }
    // }
    int neighbor_idx;
    bool from_prev = true;
    if(target_idx==0){
        neighbor_idx = target_idx+1;
        from_prev = false;
    }else{
        neighbor_idx = target_idx-1;
        from_prev = true;
    }
    if(neighbor_idx>=parent_page->count_){
        // only when parent->count_==1
        if(target_page->count_==0){
            parent_page->remove(target_idx);
            ctx.CtxPopBackWritePage();
            bpm_->DeletePage(target_pid);
        }else{
            ctx.CtxPopBackWritePage();
        }
        return CoalesceOrRedistribute(ctx);
    }
    page_id_t neighbor_pid;
    memcpy(&neighbor_pid,parent_page->getPayload(neighbor_idx),parent_page->getPayloadLen(neighbor_idx));
    auto neighbor_guard = bpm_->WritePage(neighbor_pid);
    auto neighbor_page = neighbor_guard.AsMut<BTreePage>();
    ctx.CtxWritePage(std::move(neighbor_guard));
    // bool neighbor_page_can_remove = true;
    // if(from_prev){
    //     neighbor_page_can_remove = neighbor_page->canRemove(DASET_PAGE_KEY_LEN,neighbor_page->getPayloadLen(neighbor_page->count_-1));
    // }else{
    //     neighbor_page_can_remove = neighbor_page->canRemove(DASET_PAGE_KEY_LEN,neighbor_page->getPayloadLen(0));
    // }
    // if(neighbor_page->canRemove(DASET_PAGE_KEY_LEN,target_page->getPayloadLen(0))){
        // write_set_ [(...)parent, target, neighbor]
    if(from_prev&&neighbor_page->canRemove(DASET_PAGE_KEY_LEN,neighbor_page->getPayloadLen(neighbor_page->count_-1))){
        parent_page->setKeyAt(target_idx,neighbor_page->getKey(neighbor_page->count_-1));
        return Redistribute(ctx,from_prev);
    }else if(!from_prev&&neighbor_page->canRemove(DASET_PAGE_KEY_LEN,neighbor_page->getPayloadLen(0))){
        parent_page->setKeyAt(neighbor_idx,neighbor_page->getKey(1));
        return Redistribute(ctx,from_prev);
    // }
        // return Redistribute(ctx,from_prev);
    }else{
        // write_set_ [(...)parent, target, neighbor]
        Coalesce(ctx, from_prev);
        if(from_prev){ // remove target
            parent_page->remove(target_idx);
            ctx.CtxPopBackWritePage();
            ctx.CtxPopBackWritePage();
            bpm_->DeletePage(target_pid);
        }else{
            parent_page->remove(neighbor_idx);
            ctx.CtxPopBackWritePage();
            ctx.CtxPopBackWritePage();
            bpm_->DeletePage(neighbor_pid);
        }
        // write_set_ [(...)parent]
        if(parent_page->fillFactor()>=DASET_MERGE_FACTOR){
            return true;
        }else{
            return CoalesceOrRedistribute(ctx);
        }
    }
    return true;
}

void BTreeGeneric::Coalesce(BTreeContext& ctx, bool from_prev){
    // write_set_ [(...)parent, target, neighbor]
    auto neighbor_page = ctx.write_set_.back().AsMut<BTreePage>();
    auto target_page = ctx.ParentPage();
    // 确保都是向前合并
    if(from_prev){
        // [neighbor] --- [target]
        target_page->copyKVRange(neighbor_page,neighbor_page->count_,0,target_page->count_);
        target_page->removeAllSlot();
        if(target_page->page_type_==IndexPageType::LEAF_PAGE){
            neighbor_page->next_page_id_=target_page->next_page_id_;
        }
    }else{
        // [target] --- [neighbor]
        neighbor_page->copyKVRange(target_page,target_page->count_,0,neighbor_page->count_);
        neighbor_page->removeAllSlot();
        if(target_page->page_type_==IndexPageType::LEAF_PAGE){
            target_page->next_page_id_=neighbor_page->next_page_id_;
        }
    }
}

auto BTreeGeneric::Redistribute(BTreeContext& ctx, bool from_prev) -> bool{
    // write_set_ [(...)parent, target, neighbor]
    auto neighbor_page = ctx.write_set_.back().AsMut<BTreePage>();
    auto target_page = ctx.ParentPage();
    if(from_prev){
        neighbor_page->MoveLastToFrontOf(target_page);
    }else{
        neighbor_page->MoveFirstToEndOf(target_page);
    }
    return true;
}

#if DASET_DEBUG
void BTreeGeneric::PrintfAllLeaf(){
    BTreeContext ctx;
    ctx.op_type_=TREE_OP_TYPE::READ;
    auto leaf = tracerseToLeaf(true,ctx);
    std::string info_str = "";
    while (leaf!=nullptr)
    {
        info_str = "";
        // printf("page id: %d [", leaf->page_id_);
        info_str += "page id :";
        info_str += std::to_string(leaf->page_id_);
        info_str += " [";
        for (size_t i = 0; i < leaf->count_; i++)
        {
            page_key_t key;
            memcpy(&key,leaf->getKey(i),sizeof(page_key_t));
            // printf("%d ",key);
            info_str += std::to_string(key);
            info_str += " ";
        }
        page_id_t next_page_id = leaf->next_page_id_;
        if(next_page_id!=DASET_INVALID_PAGE_ID){
            auto next_leaf_guard = bpm_->ReadPage(next_page_id);
            leaf = next_leaf_guard.As<BTreePage>();
        }else{
            leaf = nullptr;
        }
        // printf("]\n");
        info_str += "]";
        LOG_DEBUG(info_str);
    }
    return;
}
#endif

}