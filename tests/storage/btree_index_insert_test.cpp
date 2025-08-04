#include <algorithm>
#include <cstdio>

#include "buffer/buffer_pool_instance.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"
#include "storage/index/BTree/btree_generic.h"
#include "util/daset_debug_logger.h"

namespace daset {

static std::string db_fname = "testdb.txt";
static std::string test_log_file = "daset.log";

const size_t FRAMES = 25;

TEST(BPlusTreeTests, BasicInsertTest) {
  DebugLogger::getInstance().setLevel(DebugLogger::Level::DEBUG);
  DebugLogger::getInstance().setOutputFile(test_log_file);
  remove(db_fname.c_str());
  TableID tid = 0;  
  auto disk_manager = std::make_unique<DiskManager>(db_fname);
  auto *bpm = new BufferPoolInstance(0, FRAMES, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  PageKeyCompator comparator;
  BTreeGeneric tree(tid,page_id,bpm,comparator);

  page_key_t key = 42;
  uint64_t value = key & 0xFFFFFFFF;
  tree.Insert(reinterpret_cast<byte*>(&key),DASET_PAGE_KEY_LEN,reinterpret_cast<byte*>(&value),sizeof(uint64_t));

  auto root_page_id = tree.GetRootPageId();
  auto root_page_guard = bpm->ReadPage(root_page_id);
  auto root_page = root_page_guard.As<BTreePage>();
  ASSERT_NE(root_page, nullptr);
  ASSERT_TRUE(root_page->page_type_==IndexPageType::LEAF_PAGE);

  auto root_as_leaf = root_page_guard.As<BTreePage>();
  ASSERT_EQ(root_as_leaf->count_, 1);
  ASSERT_EQ(comparator(root_as_leaf->getKey(0), reinterpret_cast<byte*>(&key)), 0);
  remove(db_fname.c_str());
  delete bpm;
}

TEST(BPlusTreeTests, InsertTest1NoIterator) {
  DebugLogger::getInstance().setLevel(DebugLogger::Level::DEBUG);
  DebugLogger::getInstance().setOutputFile(test_log_file);
  // create KeyComparator and index schema
  remove(db_fname.c_str());
  TableID tid = 1;
  auto disk_manager = std::make_unique<DiskManager>(db_fname);
  auto *bpm = new BufferPoolInstance(0 ,FRAMES, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  PageKeyCompator comparator;
  BTreeGeneric tree(tid,page_id,bpm,comparator);

  int insert_cnt = 20;
  std::vector<page_key_t> keys = {};
  for (int i = 1; i <= insert_cnt; i++)
  {
    keys.push_back(i);
  }  
  for (auto key : keys) {
    uint64_t value = key & 0xFFFFFFFF;
    tree.Insert(reinterpret_cast<byte*>(&key),DASET_PAGE_KEY_LEN,reinterpret_cast<byte*>(&value),sizeof(uint64_t));
  }

  bool is_present;
  byte* value = new byte[sizeof(uint64_t)];
  size_t value_len;

  tree.PrintfAllLeaf();
  for (auto key : keys) {
    is_present = tree.Search(reinterpret_cast<byte*>(&key),DASET_PAGE_KEY_LEN,value,value_len);

    EXPECT_EQ(is_present, true);
  }
  // tree.PrintfAllLeaf();
  remove(db_fname.c_str());
  delete value;
  delete bpm;
}

TEST(BPlusTreeTests, ConcurrentInsertTest) {
    DebugLogger::getInstance().setLevel(DebugLogger::Level::DEBUG);
    DebugLogger::getInstance().setOutputFile(test_log_file);
    // 创建数据库文件和B+树
    remove(db_fname.c_str());
    TableID tid = 1;
    auto disk_manager = std::make_unique<DiskManager>(db_fname);
    auto *bpm = new BufferPoolInstance(0, FRAMES, disk_manager.get());
    page_id_t page_id = bpm->NewPage();
    PageKeyCompator comparator;
    BTreeGeneric tree(tid, page_id, bpm, comparator);

    // 测试参数配置
    const int num_threads = 20;         // 线程数
    const int keys_per_thread = 10;   // 每个线程插入的键数
    const int total_keys = num_threads * keys_per_thread;
    
    // 用于收集所有插入的键
    std::vector<page_key_t> all_keys;
    std::mutex keys_mutex;  // 保护all_keys的互斥锁

    // 线程函数 - 每个线程插入一组唯一的键
    auto insert_task = [&](int thread_id) {
        for (int i = 1; i <= keys_per_thread; ++i) {
            page_key_t key = thread_id * keys_per_thread + i;
            uint64_t value = key & 0xFFFFFFFF;
            
            // 执行插入
            bool res = tree.Insert(reinterpret_cast<byte*>(&key), DASET_PAGE_KEY_LEN, reinterpret_cast<byte*>(&value), sizeof(uint64_t));
            EXPECT_TRUE(res);
            
            // 记录插入的键(需要加锁)
            if(res){
                byte* value_tmp = new byte[sizeof(uint64_t)];
                size_t value_len;
                bool is_pres = tree.Search(reinterpret_cast<byte*>(&key), DASET_PAGE_KEY_LEN, value_tmp, value_len);
                if(!is_pres){
                  tree.Search(reinterpret_cast<byte*>(&key), DASET_PAGE_KEY_LEN, value_tmp, value_len);
                }
                std::lock_guard<std::mutex> lock(keys_mutex);
                all_keys.push_back(key);
            }
        }
    };

    // 创建并启动所有线程
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(insert_task, i);
    }

    // 等待所有线程完成
    for (auto &t : threads) {
        t.join();
    }

    sleep(1);
    tree.PrintfAllLeaf();
    // 验证所有键都能被正确检索
    byte* value = new byte[sizeof(uint64_t)];
    size_t value_len;
    bool is_present;

    // 首先检查总数是否正确
    EXPECT_EQ(all_keys.size(), total_keys);

    // 检查每个键是否存在
    for (auto key : all_keys) {
        is_present = tree.Search(reinterpret_cast<byte*>(&key), DASET_PAGE_KEY_LEN, 
                               value, value_len);

        EXPECT_TRUE(is_present) << "Key " << key << " not found after concurrent insertion";
        if(!is_present){
          tree.Search(reinterpret_cast<byte*>(&key), DASET_PAGE_KEY_LEN, value, value_len);
        }
        
        // 可选: 验证值是否正确
        // if (is_present) {
        //     uint64_t retrieved_value = *reinterpret_cast<uint64_t*>(value);
        //     // EXPECT_EQ(retrieved_value, key & 0xFFFFFFFF);
        // }
    }

    // 清理资源
    remove(db_fname.c_str());
    delete value;
    delete bpm;
}

}  // namespace bustub
