#include <iostream>
#include <cstring>
#include <cassert>
#include <thread>
#include "BTree/btree_node.h"
#include "BTree/btree_nodepool.h"
#include "BTree/btree_generic.h"
#include "BTree/btree_index.h"
#include "BTree/btree_thdGuard.h"
#include "util/tuple.h"
using namespace std;
using namespace btree;

Byte* key1;
Byte* payload1;
Byte* key2;
Byte* payload2;
Byte* key3;
Byte* payload3;
Byte* key4;
Byte* payload4;

void pre_kv();
void free_kv();
void dev_node_insert(BTreeNode* node);
void dev_node_remove(BTreeNode* node_copy);
void dev_node_copy(BTreeNode* node, BTreeNode* node_copy);
void dev_node_payloadSplit(BTreeNode* node, BTreeNode* node_copy);
void dev_node_nodepool();
void dev_tree_init();
void dev_tree_lock_to_leaf();
void dev_tree_lock_parent();
void dev_tree_insert();
void dev_tree_delete();
void dev_tuple();
void dev_index();
void pk_index();
void del_final();
void MultiThreadInseration();

int main(){
    printf("size of node : %d\n",sizeof(BTreeNode));
    printf("return\n");
    // BTreeNode* node = new BTreeNode();
    // dev_node_insert(node);
    // dev_node_remove(node);
    // dev_node_copy();
    // dev_node_payloadSplit();
    // dev_node_nodepool();
    // dev_tree_init();
    // dev_tree_lock_to_leaf();
    // dev_tree_lock_parent();
    // dev_tree_insert();
    // dev_tree_delete();
    // dev_tuple();
    // dev_index();
    // pk_index();
    // del_final();
    MultiThreadInseration();
    // delete node;
    return 0;
}

void pre_kv(){
    key1 = (Byte*)malloc(sizeof(IndexKey));
    payload1 = (Byte*)malloc(sizeof(Byte)*(4));
    key2 = (Byte*)malloc(sizeof(IndexKey));
    payload2 = (Byte*)malloc(sizeof(Byte)*(5));
    key3 = (Byte*)malloc(sizeof(IndexKey));
    payload3 = (Byte*)malloc(sizeof(Byte)*(6));
    key4 = (Byte*)malloc(sizeof(IndexKey));
    payload4 = (Byte*)malloc(sizeof(Byte)*(7));
    uint64_t _1 = 1;
    uint64_t _2 = 2;
    uint64_t _3 = 3;
    uint64_t _4 = 4;
    memcpy(key1,&_1,sizeof(IndexKey));
    memset(payload1,'1',4);
    memcpy(key2,&_2,sizeof(IndexKey));
    memset(payload2,'2',5);
    memcpy(key3,&_3,sizeof(IndexKey));
    memset(payload3,'3',6);
    memcpy(key4,&_4,sizeof(IndexKey));
    memset(payload4,'4',7);
}

void free_kv(){
    free(key1);
    free(payload1);
    free(key2);
    free(payload2);
    free(key3);
    free(payload3);
    free(key4);
    free(payload4);
}

void dev_node_insert(BTreeNode* node){
    printf("node insert test\n");
    
    pre_kv();
    Byte* payload_find = (Byte*)malloc(8);
    uint64_t payload_len;
    printf("free space : %d\n",node->freeSpace());
    assert(node->findInner(key1,sizeof(IndexKey),payload_find,payload_len)==false);
    if(node->canInsert(sizeof(IndexKey),4)){
        node->insert(key1,sizeof(IndexKey),payload1,4);
    }
    printf("free space : %d\n",node->freeSpace());
    assert(node->findInner(key1,sizeof(IndexKey),payload_find,payload_len)==true);
    assert(memcmp(payload_find,payload1,payload_len)==0);

    assert(node->findInner(key2,sizeof(IndexKey),payload_find,payload_len)==false);
    if(node->canInsert(sizeof(IndexKey),5)){
        node->insert(key2,sizeof(IndexKey),payload2,5);
    }
    printf("free space : %d\n",node->freeSpace());
    assert(node->findInner(key2,sizeof(IndexKey),payload_find,payload_len)==true);
    assert(memcmp(payload_find,payload2,payload_len)==0);
    
    assert(node->findInner(key3,sizeof(IndexKey),payload_find,payload_len)==false);
    if(node->canInsert(sizeof(IndexKey),6)){
        node->insert(key3,sizeof(IndexKey),payload3,6);
    }
    printf("free space : %d\n",node->freeSpace());
    assert(node->findInner(key3,sizeof(IndexKey),payload_find,payload_len)==true);
    assert(memcmp(payload_find,payload3,payload_len)==0);

    assert(node->findInner(key4,sizeof(IndexKey),payload_find,payload_len)==false);
    if(node->canInsert(sizeof(IndexKey),7)){
        node->insert(key4,sizeof(IndexKey),payload4,7);
    }
    printf("free space : %d\n",node->freeSpace());
    assert(node->findInner(key4,sizeof(IndexKey),payload_find,payload_len)==true);
    assert(memcmp(payload_find,payload4,payload_len)==0);
    free_kv();
    printf("node insert test passed!\n");
}

void dev_node_remove(BTreeNode* node){
    printf("node remove test\n");
    
    pre_kv();
    Byte* payload_find = (Byte*)malloc(8);
    uint64_t payload_len;
    assert(node->findInner(key1,sizeof(IndexKey),payload_find,payload_len)==false);
    if(node->canInsert(sizeof(IndexKey),4)){
        node->insert(key1,sizeof(IndexKey),payload1,4);
    }
    assert(node->findInner(key1,sizeof(IndexKey),payload_find,payload_len)==true);
    assert(memcmp(payload_find,payload1,payload_len)==0);

    assert(node->findInner(key2,sizeof(IndexKey),payload_find,payload_len)==false);
    if(node->canInsert(sizeof(IndexKey),5)){
        node->insert(key2,sizeof(IndexKey),payload2,5);
    }
    assert(node->findInner(key2,sizeof(IndexKey),payload_find,payload_len)==true);
    assert(memcmp(payload_find,payload2,payload_len)==0);
    
    assert(node->findInner(key3,sizeof(IndexKey),payload_find,payload_len)==false);
    if(node->canInsert(sizeof(IndexKey),6)){
        node->insert(key3,sizeof(IndexKey),payload3,6);
    }
    assert(node->findInner(key3,sizeof(IndexKey),payload_find,payload_len)==true);
    assert(memcmp(payload_find,payload3,payload_len)==0);

    assert(node->findInner(key4,sizeof(IndexKey),payload_find,payload_len)==false);
    if(node->canInsert(sizeof(IndexKey),7)){
        node->insert(key4,sizeof(IndexKey),payload4,7);
    }
    assert(node->findInner(key4,sizeof(IndexKey),payload_find,payload_len)==true);
    assert(memcmp(payload_find,payload4,payload_len)==0);
    
    assert(node->remove(key1,sizeof(IndexKey))==true);
    assert(node->findInner(key1,sizeof(IndexKey),payload_find,payload_len)==false);
    assert(node->remove(key1,sizeof(IndexKey))==false);

    assert(node->remove(key2,sizeof(IndexKey))==true);
    assert(node->findInner(key2,sizeof(IndexKey),payload_find,payload_len)==false);
    assert(node->remove(key2,sizeof(IndexKey))==false);

    assert(node->remove(key3,sizeof(IndexKey))==true);
    assert(node->findInner(key3,sizeof(IndexKey),payload_find,payload_len)==false);
    assert(node->remove(key3,sizeof(IndexKey))==false);

    assert(node->remove(key4,sizeof(IndexKey))==true);
    assert(node->findInner(key4,sizeof(IndexKey),payload_find,payload_len)==false);
    assert(node->remove(key4,sizeof(IndexKey))==false);
    
    free_kv();
    printf("node remove test passed!\n");
}

void dev_node_copy(BTreeNode* node, BTreeNode* node_copy){
    printf("node copy test\n");
    
    Byte* payload_find = (Byte*)malloc(8);
    uint64_t payload_len;

    pre_kv();

    node->insert(key1,sizeof(IndexKey),payload1,4);
    assert(node->findInner(key1,sizeof(IndexKey),payload_find,payload_len)==true);
    node->insert(key2,sizeof(IndexKey),payload2,5);
    assert(node->findInner(key2,sizeof(IndexKey),payload_find,payload_len)==true);
    node->insert(key3,sizeof(IndexKey),payload3,6);
    assert(node->findInner(key3,sizeof(IndexKey),payload_find,payload_len)==true);
    node->insert(key4,sizeof(IndexKey),payload4,7);
    assert(node->findInner(key4,sizeof(IndexKey),payload_find,payload_len)==true);

    node->copyKV(node_copy,0,2);
    assert(node_copy->findInner(key3,sizeof(IndexKey),payload_find,payload_len)==true);
    node->copyKV(node_copy,1,3);
    assert(node_copy->findInner(key4,sizeof(IndexKey),payload_find,payload_len)==true);

    printf("node copy passed\n");

    node_copy->remove(key3,sizeof(IndexKey));
    node_copy->remove(key4,sizeof(IndexKey));
    assert(node_copy->findInner(key3,sizeof(IndexKey),payload_find,payload_len)==false);
    assert(node_copy->findInner(key4,sizeof(IndexKey),payload_find,payload_len)==false);
    node->copyKVRange(node_copy,0,2,2);
    assert(node_copy->findInner(key3,sizeof(IndexKey),payload_find,payload_len)==true);
    assert(node_copy->findInner(key4,sizeof(IndexKey),payload_find,payload_len)==true);

    free_kv();
    printf("node copy  range passed!\n");
}

void dev_node_payloadSplit(BTreeNode* node, BTreeNode* node_copy){
    printf("node payloadSplit test\n");
    
    Byte* payload_find = (Byte*)malloc(8);
    uint64_t payload_len;

    pre_kv();

    node->insert(key1,sizeof(IndexKey),payload1,4);
    assert(node->findInner(key1,sizeof(IndexKey),payload_find,payload_len)==true);
    node->insert(key2,sizeof(IndexKey),payload2,5);
    assert(node->findInner(key2,sizeof(IndexKey),payload_find,payload_len)==true);
    node->insert(key3,sizeof(IndexKey),payload3,6);
    assert(node->findInner(key3,sizeof(IndexKey),payload_find,payload_len)==true);
    node->insert(key4,sizeof(IndexKey),payload4,7);
    assert(node->findInner(key4,sizeof(IndexKey),payload_find,payload_len)==true);

    assert(node->count_==4);
    assert(node_copy->count_==0);
    node->payloadSplit(node_copy);

    assert(node->count_==2);
    assert(node_copy->count_==2);
    assert(node->findInner(key3,sizeof(IndexKey),payload_find,payload_len)==false);
    assert(node->findInner(key4,sizeof(IndexKey),payload_find,payload_len)==false);

    assert(node_copy->findInner(key3,sizeof(IndexKey),payload_find,payload_len)==true);
    assert(node_copy->findInner(key4,sizeof(IndexKey),payload_find,payload_len)==true);

    free_kv();
    printf("node payloadSplit test passed!\n");
}

void dev_node_nodepool(){
    BTreeNodePool* nodepool = new BTreeNodePool();
    BTreeNode* node = nodepool->new_node(0);
    BTreeNode* node_copy = nodepool->new_node(1);
    
    printf("nodepool test\n");

    dev_node_copy(nodepool->get_node(0),nodepool->get_node(1));
    // dev_node_payloadSplit(node,node_copy);
    assert(nodepool->get_node_count()==2);
    assert(node==nodepool->get_node(0));
    assert(node_copy==nodepool->get_node(1));
    
    printf("nodepool test passed\n");
    delete nodepool;
}


void dev_tree_init(){
    TableID tid = 0;
    BTreeNodePool* nodepool = new BTreeNodePool();
    BTreeGeneric* btree = new BTreeGeneric(tid);
    btree->init(nodepool);
    BTreeNode* initroot = nodepool->get_node(NID_NIL);
    BTreeNode* initleaf = nodepool->get_node(NID_LEAF_START);
    BTreeNode* initinner = nodepool->get_node(NID_START);
    assert(initroot->count_==1);
    assert(initleaf->count_==1);
    assert(initinner->count_==1);
    delete nodepool;
    delete btree;
}

void dev_tree_lock_to_leaf(){
    TableID tid = 0;
    BTreeNodePool* nodepool = new BTreeNodePool();
    BTreeGeneric* btree = new BTreeGeneric(tid);
    pre_kv();
    btree->init(nodepool);
    BTreeNode* initroot = nodepool->get_node(NID_NIL);
    BTreeNode* initleaf = nodepool->get_node(NID_LEAF_START);
    BTreeNode* initinner = nodepool->get_node(NID_START);
    initleaf->insert(key1,sizeof(IndexKey),payload1,4);
    initleaf->insert(key2,sizeof(IndexKey),payload2,5);
    initleaf->insert(key3,sizeof(IndexKey),payload3,6);
    initleaf->insert(key4,sizeof(IndexKey),payload2,7);
    assert(initroot->count_==1);
    assert(initleaf->count_==5);
    assert(initinner->count_==1);
    const Byte infKey_data[INFKEY_SIZE]={0};
    const Byte* infKey = infKey_data;
    btreeThdGuard* guard = new btreeThdGuard();
    BTreeNode* res = btree->lock_to_leaf(infKey,INFKEY_SIZE,FREE,nodepool,guard);
    Byte* payload_find = (Byte*)malloc(8);
    uint64_t payload_len;
    assert(res->findInner(key1,sizeof(IndexKey),payload_find,payload_len)==true);
    free_kv();
    delete guard;
    delete nodepool;
    delete btree;
}

void dev_tree_lock_parent(){
    TableID tid = 0;
    BTreeNodePool* nodepool = new BTreeNodePool();
    BTreeGeneric* btree = new BTreeGeneric(tid);
    pre_kv();
    btree->init(nodepool);
    BTreeNode* initroot = nodepool->get_node(NID_NIL);
    BTreeNode* initleaf = nodepool->get_node(NID_LEAF_START);
    BTreeNode* initinner = nodepool->get_node(NID_START);
    initleaf->insert(key1,sizeof(IndexKey),payload1,4);
    initleaf->insert(key2,sizeof(IndexKey),payload2,5);
    initleaf->insert(key3,sizeof(IndexKey),payload3,6);
    initleaf->insert(key4,sizeof(IndexKey),payload2,7);
    assert(initroot->count_==1);
    assert(initleaf->count_==5);
    assert(initinner->count_==1);
    const Byte infKey_data[INFKEY_SIZE]={0};
    const Byte* infKey = infKey_data;
    btreeThdGuard* guard = new btreeThdGuard();
    BTreeNode* res = btree->search_parent(initleaf->nid_,nodepool,guard);
    Byte* payload_find = (Byte*)malloc(8);
    uint64_t payload_len;
    assert(res==initinner);
    assert(res->findInner(infKey,sizeof(IndexKey),payload_find,payload_len)==true);
    assert((*(uint64_t*)payload_find)==initleaf->nid_);
    free_kv();
    delete nodepool;
    delete btree;
}

void dev_tree_insert(){
    TableID tid = 0;
    BTreeNodePool* nodepool = new BTreeNodePool();
    BTreeGeneric* btree = new BTreeGeneric(tid);
    btree->init(nodepool);
    BTreeNode* initroot = nodepool->get_node(NID_NIL);
    BTreeNode* initleaf = nodepool->get_node(NID_LEAF_START);
    BTreeNode* initinner = nodepool->get_node(NID_START);
    
    pre_kv();
    btreeThdGuard* guard = new btreeThdGuard();
    for (uint64_t i = 1; i <= 1000; i++)
    {
        Byte* key = (Byte*)malloc(sizeof(IndexKey));
        Byte* value = (Byte*)malloc(sizeof(Byte)*(i%4+4));
        memcpy(key,&i,sizeof(IndexKey));
        memset(value,'1',sizeof(Byte)*(i%4+4));
        btree->insert_leaf(key,sizeof(IndexKey),value,sizeof(Byte)*(i%4+4),nodepool,guard);
        free(key);
        free(value);
    }
    for (uint64_t i = 1; i <= 1000; i++)
    {
        Byte* key = (Byte*)malloc(sizeof(IndexKey));
        Byte* value = (Byte*)malloc(sizeof(Byte)*(i%4+4));
        memcpy(key,&i,sizeof(IndexKey));
        // memset(value,'1',sizeof(Byte)*(i%4+4));
        uint64_t value_len = 0;
        if(!btree->find_kv(key,sizeof(IndexKey),value,value_len,nodepool,guard)){
            btree->find_kv(key,sizeof(IndexKey),value,value_len,nodepool,guard);
        }
        assert(btree->find_kv(key,sizeof(IndexKey),value,value_len,nodepool,guard)==true);
        // btree->insert_leaf(key,sizeof(IndexKey),value,sizeof(Byte)*(i%4+4),nodepool);
        free(key);
        free(value);
    }

    
    free_kv();
    // assert(initroot->count_==1);
    // assert(initleaf->count_==1);
    // assert(initinner->count_==1);
    delete guard;
    delete nodepool;
    delete btree;
}

void dev_tree_delete(){
    TableID tid = 0;
    BTreeNodePool* nodepool = new BTreeNodePool();
    BTreeGeneric* btree = new BTreeGeneric(tid);
    btree->init(nodepool);
    BTreeNode* initroot = nodepool->get_node(NID_NIL);
    BTreeNode* initleaf = nodepool->get_node(NID_LEAF_START);
    BTreeNode* initinner = nodepool->get_node(NID_START);
    btreeThdGuard* guard = new btreeThdGuard();
    
    pre_kv();
    for (uint64_t i = 1; i <= 1000; i++)
    {
        Byte* key = (Byte*)malloc(sizeof(IndexKey));
        Byte* value = (Byte*)malloc(sizeof(Byte)*(i%4+4));
        memcpy(key,&i,sizeof(IndexKey));
        memset(value,'1',sizeof(Byte)*(i%4+4));
        btree->insert_leaf(key,sizeof(IndexKey),value,sizeof(Byte)*(i%4+4),nodepool,guard);
        free(key);
        free(value);
    }
    for (uint64_t i = 1; i <= 1000; i++)
    {
        Byte* key = (Byte*)malloc(sizeof(IndexKey));
        Byte* value = (Byte*)malloc(sizeof(Byte)*(i%4+4));
        memcpy(key,&i,sizeof(IndexKey));
        // memset(value,'1',sizeof(Byte)*(i%4+4));
        uint64_t value_len = 0;
        // if(!btree->find_kv(key,sizeof(IndexKey),value,value_len,nodepool)){
        //     btree->find_kv(key,sizeof(IndexKey),value,value_len,nodepool);
        // }
        assert(btree->find_kv(key,sizeof(IndexKey),value,value_len,nodepool,guard)==true);
        // btree->insert_leaf(key,sizeof(IndexKey),value,sizeof(Byte)*(i%4+4),nodepool);
        free(key);
        free(value);
    }
    for (uint64_t i = 1; i <= 1000; i++)
    {
        Byte* key = (Byte*)malloc(sizeof(IndexKey));
        Byte* value = (Byte*)malloc(sizeof(Byte)*(i%4+4));
        memcpy(key,&i,sizeof(IndexKey));
        // memset(value,'1',sizeof(Byte)*(i%4+4));
        uint64_t value_len = 0;
        // if(!btree->find_kv(key,sizeof(IndexKey),value,value_len,nodepool)){
        //     btree->find_kv(key,sizeof(IndexKey),value,value_len,nodepool);
        // }
        assert(btree->remove_kv(key,sizeof(IndexKey),nodepool,guard)==true);
        // btree->insert_leaf(key,sizeof(IndexKey),value,sizeof(Byte)*(i%4+4),nodepool);
        free(key);
        free(value);
    }
    
    
    free_kv();
    // assert(initroot->count_==1);
    // assert(initleaf->count_==1);
    // assert(initinner->count_==1);
    delete guard;
    delete nodepool;
    delete btree;
}

void dev_tuple(){
    // BTreeIndex index(0,false,0);
    // Tuple* tuple = new Tuple(4);
    // Byte* byte;
    // index.Tuple2Byte(tuple,byte);
    // Tuple* tuple2;
    // index.Byte2Tuple(byte,tuple2);
    // printf("%d\n",tuple2->tuple_data_);
    // return;
}

void dev_index(){
    BTreeIndex* index = new BTreeIndex(1,true,1);
    for (IndexKey i = 1; i <= 1000; i++)
    {
        int tuple_size = 8+i%4;
        Tuple* tuple = new Tuple(tuple_size);
        index->IndexInsert(i,tuple);
        Tuple* find = nullptr;
        assert(index->IndexRead(i,find)==RC_OK);
        assert(find->tuple_size_=tuple_size);
        assert(memcmp(find->tuple_data_,tuple->tuple_data_,tuple_size)==0);
        delete tuple;
        delete find;
    }
    for (IndexKey i = 1; i <= 1000; i++)
    {
        int tuple_size = 8+i%4;
        index->IndexRemove(i);
        Tuple* find = nullptr;
        assert(index->IndexRead(i,find)==RC_NULL);
        delete find;
    }
    return;
}

void pk_index(){
    BTreeIndex* index = new BTreeIndex(1,true,1);
    for (IndexKey i = 1; i <= 1000; i++)
    {
        int tuple_size = 8+i%4;
        Tuple* tuple = new Tuple(tuple_size);
        index->IndexInsert(i,tuple);
        Tuple* find = nullptr;
        assert(index->IndexRead(i,find)==RC_OK);
        assert(find->tuple_size_=tuple_size);
        assert(memcmp(find->tuple_data_,tuple->tuple_data_,tuple_size)==0);
        delete tuple;
        delete find;
    }
    for (IndexKey i = 1; i <= 1000; i++)
    {
        int tuple_size = 8+i%4;
        Tuple* tuple = new Tuple(tuple_size);
        assert(index->IndexInsert(i,tuple)==RC_NULL);
        delete tuple;
    }
    for (IndexKey i = 1; i <= 1000; i++)
    {
        int tuple_size = 8+i%4;
        index->IndexRemove(i);
        Tuple* find = nullptr;
        assert(index->IndexRead(i,find)==RC_NULL);
        delete find;
    }
    return;
}

void del_final(){
    BTreeIndex* index = new BTreeIndex(1,true,1);
    for (IndexKey i = 1; i <= 1000; i++)
    {
        int tuple_size = 8+i%4;
        Tuple* tuple = new Tuple(tuple_size);
        index->IndexInsert(i,tuple);
        Tuple* find = nullptr;
        assert(index->IndexRead(i,find)==RC_OK);
        assert(find->tuple_size_=tuple_size);
        assert(memcmp(find->tuple_data_,tuple->tuple_data_,tuple_size)==0);
        delete tuple;
        delete find;
    }
    for (IndexKey i = 1; i <= 1000; i++)
    {
        int tuple_size = 8+i%4;
        Tuple* tuple = new Tuple(tuple_size);
        assert(index->IndexInsert(i,tuple)==RC_NULL);
        delete tuple;
    }

    for (IndexKey i = 1000; i >= 1; i--)
    {
        int tuple_size = 8+i%4;
        index->IndexRemove(i);
        Tuple* find = nullptr;
        assert(index->IndexRead(i,find)==RC_NULL);
        delete find;
    }
    return;
}

// 定义线程函数：插入操作
void insertTask(BTreeIndex* index, IndexKey start, IndexKey end) {
    for (IndexKey i = start; i <= end; i++) {
        uint64_t tuple_size = 8 + i % 4;
        Tuple* tuple = new Tuple(tuple_size);
        index->IndexInsert(i, tuple);
        Tuple* find = nullptr;
        assert(index->IndexRead(i, find) == RC_OK);
        // if(index->IndexRead(i, find) != RC_OK){
        //     index->IndexRead(i, find);
        // }
        assert(find->tuple_size_ == tuple_size);
        assert(memcmp(find->tuple_data_, tuple->tuple_data_, tuple_size) == 0);
        delete tuple;
        delete find;
    }
}

// 定义线程函数：删除操作
void removeTask(BTreeIndex* index, IndexKey start, IndexKey end) {
    for (IndexKey i = start; i <= end; i++) {
        uint64_t tuple_size = 8 + i % 4;
        index->IndexRemove(i);
        Tuple* find = nullptr;
        assert(index->IndexRead(i, find) == RC_NULL);
        delete find;
    }
}

void MultiThreadInseration() {
    BTreeIndex* index = new BTreeIndex(1, true, 1);
    // 定义线程数
    const int num_threads = 4;
    std::vector<std::thread> threads;

    // 分配任务给线程
    IndexKey keys_per_thread = 10000 / num_threads;
    for (int i = 0; i < num_threads; i++) {
        IndexKey start = i * keys_per_thread + 1;
        IndexKey end = (i + 1) * keys_per_thread;
        threads.emplace_back(insertTask, index, start, end);
    }

    // 等待所有线程完成
    for (auto& t : threads) {
        t.join();
    }

    // 清空线程容器
    threads.clear();

    // 分配删除任务给线程
    for (int i = 0; i < num_threads; i++) {
        IndexKey start = i * keys_per_thread + 1;
        IndexKey end = (i + 1) * keys_per_thread;
        threads.emplace_back(removeTask, index, start, end);
    }

    // 等待所有线程完成
    for (auto& t : threads) {
        t.join();
    }

    delete index;
    return;
}