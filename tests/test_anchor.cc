#include "gtest/gtest.h"
#include "AnchorBTree/anchor_btree.h"
#include "util/tuple.h"
#include "AnchorBTree/anchor_btree_index.h"
#include <thread>

using namespace anchorBTree;

Byte* key1;
Byte* payload1;
Byte* key2;
Byte* payload2;
Byte* key3;
Byte* payload3;
Byte* key4;
Byte* payload4;

int insert_abort_cnt = 0;
int find_abort_cnt = 0;
int remove_abort_cnt = 0;

void pre_kv(){
    key1 = (Byte*)malloc(sizeof(IndexKey));
    payload1 = (Byte*)malloc(sizeof(Byte)*(PAYLOAD_LEN-100));
    key2 = (Byte*)malloc(sizeof(IndexKey));
    payload2 = (Byte*)malloc(sizeof(Byte)*(PAYLOAD_LEN-100));
    key3 = (Byte*)malloc(sizeof(IndexKey));
    payload3 = (Byte*)malloc(sizeof(Byte)*(PAYLOAD_LEN-100));
    key4 = (Byte*)malloc(sizeof(IndexKey));
    payload4 = (Byte*)malloc(sizeof(Byte)*(PAYLOAD_LEN-100));
    uint64_t _1 = 1;
    uint64_t _2 = 4;
    uint64_t _3 = 7;
    uint64_t _4 = 15;
    memcpy(key1,&_1,sizeof(IndexKey));
    memset(payload1,'1',PAYLOAD_LEN-100);
    memcpy(key2,&_2,sizeof(IndexKey));
    memset(payload2,'1',PAYLOAD_LEN-100);
    memcpy(key3,&_3,sizeof(IndexKey));
    memset(payload3,'1',PAYLOAD_LEN-100);
    memcpy(key4,&_4,sizeof(IndexKey));
    memset(payload4,'1',PAYLOAD_LEN-100);
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

TEST(tAnchorInit, test_anchor_init_1) {
    BTreeAnchor *btreeAnchor = new BTreeAnchor();
    btreeAnchor->prinfEdge();
    printf("NODE SIZE : %ld\n",NODE_SIZE);
    printf("size of AnchorNode : %ld\n",sizeof(AnchorNode));
    delete btreeAnchor;
}

TEST(tAnchorNodeFind, test_anchor_find_1) {
    BTreeAnchor *btreeAnchor = new BTreeAnchor();
    // btreeAnchor->prinfEdge();
    AnchorNode* node = nullptr;
    uint64_t idx = 0;
    btreeAnchor->find_node_debug(1,node,idx);
    EXPECT_EQ(idx,0);
    EXPECT_TRUE(node!=nullptr);
    btreeAnchor->find_node_debug(1024,node,idx);
    EXPECT_EQ(idx,0);
    EXPECT_TRUE(node!=nullptr);
    btreeAnchor->find_node_debug(512,node,idx);
    EXPECT_EQ(idx,0);
    EXPECT_TRUE(node!=nullptr);
    btreeAnchor->find_node_debug(63,node,idx);
    EXPECT_EQ(idx,0);
    EXPECT_TRUE(node!=nullptr);
    btreeAnchor->find_node_debug(112,node,idx);
    EXPECT_EQ(idx,0);
    EXPECT_TRUE(node!=nullptr);
    btreeAnchor->find_node_debug(888,node,idx);
    EXPECT_EQ(idx,0);
    EXPECT_TRUE(node!=nullptr);
    delete btreeAnchor;
}

TEST(tAnchorSplit, test_anchor_split_1) {
    AnchorNode* node = new AnchorNode();
    AnchorNode* noderight = new AnchorNode();
    pre_kv();
    node->insert(key1,sizeof(IndexKey),payload1,PAYLOAD_LEN-100);
    node->insert(key2,sizeof(IndexKey),payload2,PAYLOAD_LEN-100);
    node->insert(key3,sizeof(IndexKey),payload3,PAYLOAD_LEN-100);
    node->insert(key4,sizeof(IndexKey),payload4,PAYLOAD_LEN-100);
    uint64_t low = 3;
    uint64_t high = 10;
    memcpy(key2,&low,sizeof(IndexKey));
    memcpy(key3,&high,sizeof(IndexKey));
    node->payloadSplit(noderight,key2,key3,PAYLOAD_LEN-100);
    free_kv();
    delete node;
    delete noderight;
}


TEST(tAnchorInsert, test_anchor_insert){
// void test(){
    AnchorNodePool* nodepool = new AnchorNodePool();
    BTreeAnchor* btreeAnchor = new BTreeAnchor();
    // btreeAnchor->prinfEdge();
    pre_kv();
    for (uint64_t i = 1; i <= 1000; i++)
    {
        Byte* key = (Byte*)malloc(sizeof(IndexKey));
        Byte* value = (Byte*)malloc(sizeof(Byte)*(PAYLOAD_LEN-100));
        memcpy(key,&i,sizeof(IndexKey));
        memset(value,'1',sizeof(Byte)*(PAYLOAD_LEN-100));
        anchorThdGuard* guard = new anchorThdGuard();
        EXPECT_TRUE(btreeAnchor->insert_kv(key,sizeof(IndexKey),value,sizeof(Byte)*(PAYLOAD_LEN-100),nodepool,guard));
        delete guard;
        free(key);
        free(value);
    }
    for (uint64_t i = 1; i <= 1000; i++)
    {
        Byte* key = (Byte*)malloc(sizeof(IndexKey));
        Byte* value = (Byte*)malloc(sizeof(Byte)*(PAYLOAD_LEN-100));
        memcpy(key,&i,sizeof(IndexKey));
        uint64_t value_len = 0;
        anchorThdGuard* guard = new anchorThdGuard();
        EXPECT_TRUE(btreeAnchor->find_kv(key,sizeof(IndexKey),value,value_len,nodepool,guard));
        delete guard;
        free(key);
        free(value);
    }
    free_kv();
    delete nodepool;
    delete btreeAnchor;
}

// 定义线程函数：插入操作
void insertTask(AnchorBTreeIndex* index, IndexKey start, IndexKey end) {
    for (IndexKey i = start; i <= end; i++) {
        int tuple_size = PAYLOAD_LEN-100;
        Tuple* tuple = new Tuple(tuple_size);
        // if(i==385){
        //     printf("here\n");
        // }
        if(index->IndexInsert(i, tuple)!=RC_OK){
            ATOM_ADD_FETCH(insert_abort_cnt, 1);
        }
        Tuple* find = nullptr;
        // if(index->IndexRead(i, find) != RC_OK){
        //     index->IndexRead(i, find);
        // }
        if(index->IndexRead(i, find) != RC_OK){
            ATOM_ADD_FETCH(find_abort_cnt, 1);
        }else{
            assert(find->tuple_size_ == tuple_size);
            assert(memcmp(find->tuple_data_, tuple->tuple_data_, tuple_size) == 0);
        }

        // index->IndexRead(i, find);
        // if(index->IndexRead(i, find) != RC_OK){
        //     index->IndexRead(i, find);
        // }
        delete tuple;
        delete find;
    }
}

// 定义线程函数：删除操作
void removeTask(AnchorBTreeIndex* index, IndexKey start, IndexKey end) {
    for (IndexKey i = start; i <= end; i++) {
        int tuple_size = PAYLOAD_LEN-100;
        index->IndexRemove(i);
        Tuple* find = nullptr;
        assert(index->IndexRead(i, find) == RC_NULL);
        delete find;
    }
}

void MultiThreadInseration() {
    uint64_t kv_cnt = 1000;
    uint64_t anchor_cnt = kv_cnt / NODE_CAPCITY;
    if(kv_cnt % NODE_CAPCITY != 0){
        anchor_cnt++;
    }
    uint64_t anchor_pow = 1;
    while (anchor_pow < anchor_cnt)
    {
        anchor_pow *= 2;
    }
    AnchorBTreeIndex* index = new AnchorBTreeIndex(1, true, 1, anchor_pow);
    // 定义线程数
    const int num_threads = 20;
    std::vector<std::thread> threads;

    // 分配任务给线程
    IndexKey keys_per_thread = kv_cnt / num_threads;
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

    // // 分配删除任务给线程
    // for (int i = 0; i < num_threads; i++) {
    //     IndexKey start = i * keys_per_thread + 1;
    //     IndexKey end = (i + 1) * keys_per_thread;
    //     threads.emplace_back(removeTask, index, start, end);
    // }

    // // 等待所有线程完成
    // for (auto& t : threads) {
    //     t.join();
    // }
    printf("insert txn abort: %d\n",insert_abort_cnt);
    printf("find txn abort: %d\n",find_abort_cnt);

    delete index;
    return;
}

int main(int argc, char** argv){
    ::testing::InitGoogleTest(&argc, argv);
    RUN_ALL_TESTS();
    MultiThreadInseration();
}