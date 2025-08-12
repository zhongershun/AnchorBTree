#ifndef STORAGE_INDEX_HASH_GENERIC_H_
#define STORAGE_INDEX_HASH_GENERIC_H_

#include "config/config.h"
#include "storage/page/HashPage/hash_page.h"
#include "buffer/buffer_pool_instance.h"

namespace daset{

class HashContext{

};

class HashSchemaPage{
public:
    HashSchemaPage() = delete;
    HashSchemaPage(const HashSchemaPage &other) = delete;
    page_id_t first_page_id_;
};

class HashGeneric{
    BufferPoolInstance  *bpm_;
    PageKeyCompator     comparator_;
    TableID             table_id_;
    size_t              bucket_cnt_;
    page_id_t           schema_page_id_;
    page_id_t           *buckets_;
    
    auto HashFunc(page_key_t key) -> size_t;
public:
    HashGeneric(TableID table_id, page_id_t schema_page_id, BufferPoolInstance* bpm, const PageKeyCompator comparator, size_t bucket_cnt);
    ~HashGeneric();

    auto IsEmpty() const -> bool;
    auto GetFirstPageId() -> page_id_t;
    auto GetFirstPage() const -> const BucketPage*;

    auto Insert(const byte* key, const size_t key_len, const byte* tuple, size_t tuple_len) -> bool;
    auto Remove(const byte* key, const size_t key_len) -> bool;
    auto Search(const byte* key, const size_t key_len, byte*& payload, size_t& payload_len) -> bool;

    #if DASET_DEBUG == true
    void PrintfAllLeaf();
    #endif
};

}

#endif