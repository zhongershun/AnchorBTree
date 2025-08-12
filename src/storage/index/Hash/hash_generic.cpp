#include "storage/index/Hash/hash_generic.h"
#include "util/daset_debug_logger.h"
namespace daset{

HashGeneric::HashGeneric(TableID table_id, page_id_t schema_page_id,
    BufferPoolInstance* bpm, const PageKeyCompator comparator, size_t bucket_cnt)
    : table_id_(table_id),schema_page_id_(schema_page_id),
    bpm_(bpm),comparator_(comparator),bucket_cnt_(bucket_cnt){
    buckets_ = new page_id_t[bucket_cnt_];
    page_id_t first_page_id = bpm_->NewPage();
    page_id_t cur_page_id = first_page_id;
    for (size_t i = 0; i < bucket_cnt_; i++)
    {
        buckets_[i] = cur_page_id;
        auto cur_page_guard = bpm_->WritePage(cur_page_id);
        auto cur_page = cur_page_guard.AsMut<BucketPage>();
        cur_page->init(cur_page_id);
        if(i==bucket_cnt_-1){
            cur_page_id = DASET_INVALID_PAGE_ID;
        }else{
            cur_page_id = bpm_->NewPage();
        }
        cur_page->next_page_id_ = cur_page_id;
    }
    WritePageGuard guard = bpm_->WritePage(schema_page_id_);
    auto root_page = guard.AsMut<HashSchemaPage>();
    root_page->first_page_id_ = first_page_id;
}

HashGeneric::~HashGeneric(){
    schema_page_id_ = DASET_INVALID_PAGE_ID;
    // for (size_t i = 0; i < bucket_cnt_; i++)
    // {
        // bpm_->DeletePage(buckets_[i]);
        // buckets_[i] = DASET_INVALID_PAGE_ID;
    // }
}

auto HashGeneric::GetFirstPageId() -> page_id_t{
    auto header_guard = bpm_->ReadPage(schema_page_id_);
    auto header_page = header_guard.As<HashSchemaPage>();
    page_id_t first_page_id = header_page->first_page_id_;
    return first_page_id;
}

auto HashGeneric::GetFirstPage() const -> const BucketPage*{
    auto header_guard = bpm_->ReadPage(schema_page_id_);
    auto header_page = header_guard.As<HashSchemaPage>();
    page_id_t first_page_id = header_page->first_page_id_;
    auto first_guard = bpm_->ReadPage(first_page_id);
    auto first_page = header_guard.As<BucketPage>();
    return first_page;
}

auto HashGeneric::HashFunc(page_key_t key) -> size_t{
    return (size_t)(key) % bucket_cnt_;
}

auto HashGeneric::Search(const byte* key,const size_t key_len, byte*& payload, size_t& payload_len) -> bool{
    page_key_t indexKey;
    memcpy(&indexKey,key,key_len);
    size_t bkt_idx = HashFunc(indexKey);
    page_id_t cur_page_id = buckets_[bkt_idx];
    while (cur_page_id!=DASET_INVALID_PAGE_ID)
    {
        auto page_guard = bpm_->ReadPage(cur_page_id);
        auto page = page_guard.As<BucketPage>();
        if(comparator_(page->getKey(0),key)==0){
            return page->search(key,key_len,payload,payload_len,comparator_);
        }
        cur_page_id = page->next_page_id_;
    }
    return false;
}

auto HashGeneric::Insert(const byte* key,const size_t key_len,const byte* tuple, size_t tuple_len) -> bool{
    page_key_t indexKey;
    memcpy(&indexKey,key,key_len);
    size_t bkt_idx = HashFunc(indexKey);
    page_id_t cur_page_id = buckets_[bkt_idx];
    auto page_guard = bpm_->WritePage(cur_page_id);
    auto page = page_guard.AsMut<BucketPage>();
    return page->insert(key,key_len,tuple,tuple_len,comparator_);
}

auto HashGeneric::Remove(const byte* key,const size_t key_len) -> bool{
    page_key_t indexKey;
    memcpy(&indexKey,key,key_len);
    size_t bkt_idx = HashFunc(indexKey);
    page_id_t cur_page_id = buckets_[bkt_idx];
    auto page_guard = bpm_->WritePage(cur_page_id);
    auto page = page_guard.AsMut<BucketPage>();
    return page->remove(key,key_len,comparator_);
}

#if DASET_DEBUG
void HashGeneric::PrintfAllLeaf(){
    std::string info_str = "";
    // auto first_pid = GetFirstPageId();
    // auto leaf = GetFirstPage();
    for (size_t i = 0; i < bucket_cnt_; i++)
    {
        info_str = "";
        info_str += "page id :";
        info_str += std::to_string(buckets_[i]);
        info_str += " [";
        auto page_guard = bpm_->ReadPage(buckets_[i]);
        auto page = page_guard.As<BucketPage>();
        for (size_t i = 0; i < page->node_cnt_; i++)
        {
            page_key_t key;
            memcpy(&key,page->getKey(i),sizeof(page_key_t));
            // printf("%d ",key);
            info_str += std::to_string(key);
            info_str += " ";
        }
        page_id_t next_page_id = page->next_page_id_;
        // if(next_page_id!=DASET_INVALID_PAGE_ID){
        //     auto next_leaf_guard = bpm_->ReadPage(next_page_id);
        //     leaf = next_leaf_guard.As<BucketPage>();
        // }else{
        //     leaf = nullptr;
        // }
        // printf("]\n");
        info_str += "]";
        LOG_DEBUG(info_str);
    }
    
}
#endif

}