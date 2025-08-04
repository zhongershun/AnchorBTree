#ifndef STORAGE_INDEX_BTREE_GENERIC_H_
#define STORAGE_INDEX_BTREE_GENERIC_H_

#include "config/config.h"
#include "storage/page/BTreePage/btree_page.h"
#include "buffer/buffer_pool_instance.h"

namespace daset{

enum TREE_OP_TYPE {READ = 0, INSERT, DELETE};

class BTreeContext{
public:
    page_id_t root_page_id_{DASET_INVALID_PAGE_ID};
    std::deque<WritePageGuard> write_set_;

    std::deque<ReadPageGuard> read_set_;

    TREE_OP_TYPE op_type_{TREE_OP_TYPE::READ};

    auto IsRootPage(page_id_t page_id) -> bool;

    void CtxReadPage(ReadPageGuard&& read_page_guard);
    void CtxWritePage(WritePageGuard&& write_page_guard);

    void CtxPopBackReadPage();
    void CtxPopBackWritePage();
    void CtxPopFrontReadPage(page_id_t page_id);
    void CtxPopFrontWritePage(page_id_t page_id);

    auto ParentPage();

    #if DASET_DEBUG==true
    std::vector<const BTreePage*> path_;
    #endif

    ~BTreeContext();
};

class BTreeSchemaPage{
public:
  // Delete all constructor / destructor to ensure memory safety
  BTreeSchemaPage() = delete;
  BTreeSchemaPage(const BTreeSchemaPage &other) = delete;
//   page_id_t page_id_;
  page_id_t root_page_id_;
};

class BTreeGeneric{
    TableID             table_id_;
    BufferPoolInstance  *bpm_;
    PageKeyCompator     comparator_;
    page_id_t           header_page_id_;

public:
    // BTreeNode*          root_;
    // BTreeNodePool*      nodepool_;


    // BTreeGeneric(TableID table_id,BTreeNodePool* nodepool_,bool is_pk_index);
    BTreeGeneric(TableID table_id, page_id_t header_page_id,
        BufferPoolInstance* bpm, const PageKeyCompator comparator);
    ~BTreeGeneric();

    auto IsEmpty() const -> bool;
    auto GetRootPageId() -> page_id_t;

    auto Insert(const byte* key,const size_t key_len,const byte* tuple, size_t tuple_len) -> bool;
    auto Remove(const byte* key,const size_t key_len) -> bool;
    auto Search(const byte* key,const size_t key_len, byte*& payload, size_t& payload_len) -> bool;

    #if DASET_DEBUG == true
    void PrintfAllLeaf();
    #endif

private:
    auto tracerseToLeafMut(const byte* &key, const size_t key_len, const size_t payload_len, BTreeContext &context) -> BTreePage*;
    auto tracerseToLeaf(const byte* &key, const size_t key_len, const size_t payload_len, BTreeContext &context) const -> const BTreePage*;
    auto tracerseToLeaf(bool left, BTreeContext &context) const -> const BTreePage*;

    auto InsertLeaf(const byte* key,const size_t key_len,const byte* tuple, size_t tuple_len,BTreeContext& ctx) -> bool;
    auto InsertInner(const byte* key,const size_t key_len,const byte* tuple, size_t tuple_len,BTreeContext& ctx) -> bool;
    auto Pipeup(page_id_t root_page_id, BTreeContext& ctx) -> page_key_t;
    auto RemoveLeaf(const byte* key,const size_t key_len, BTreeContext& ctx) -> bool;
    auto CoalesceOrRedistribute(BTreeContext& ctx) -> bool;
    void Coalesce(BTreeContext& ctx, bool from_prev);
    auto Redistribute(BTreeContext& ctx, bool from_prev) -> bool;
};


}


#endif