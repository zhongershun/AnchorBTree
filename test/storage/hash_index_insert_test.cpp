#include <vector>
#include <unordered_set>
#include <algorithm>
#include <random>
#include <chrono>

#include "buffer/buffer_pool_instance.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"
#include "storage/index/Hash/hash_generic.h"
#include "util/daset_debug_logger.h"

namespace daset {

static std::string db_fname = "testdb.txt";
static std::string test_log_file = "daset.log";

const size_t FRAMES = 1000;
// const size_t bucket_cnt = 500;


std::vector<page_key_t> generateRandomUniqueKeys(size_t n) {
    // 1. 生成不重复的随机数（大于0）
    std::unordered_set<page_key_t> unique_keys;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<page_key_t> dist(1, n * 2); // 确保足够大的范围避免重复

    while (unique_keys.size() < n) {
        page_key_t key = dist(gen);
        unique_keys.insert(key);
    }

    // 2. 将 set 转为 vector
    std::vector<page_key_t> keys(unique_keys.begin(), unique_keys.end());

    // 3. 打乱顺序
    std::shuffle(keys.begin(), keys.end(), gen);

    return keys;
}

TEST(HashTreeTests, InsertTest1NoIterator) {
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
  size_t bucket_cnt = 5;
  HashGeneric tree(tid,page_id,bpm,comparator,bucket_cnt);
  tree.PrintfAllLeaf();

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

TEST(HashTreeTests, InsertTestDesc) {
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
  size_t bucket_cnt = 5;
  HashGeneric tree(tid,page_id,bpm,comparator,bucket_cnt);

  int insert_cnt = 10;
  std::vector<page_key_t> keys = {10,9,8,7,6,5,4,3,2,1};
  // for (int i = 1; i <= insert_cnt; i++)
  // {
  //   keys.push_back(i);
  // }  
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

TEST(HashTreeTests, ConcurrentInsertTest) {
    DebugLogger::getInstance().setLevel(DebugLogger::Level::DEBUG);
    DebugLogger::getInstance().setOutputFile(test_log_file);
    // 创建数据库文件和B+树
    remove(db_fname.c_str());
    TableID tid = 1;
    auto disk_manager = std::make_unique<DiskManager>(db_fname);
    auto *bpm = new BufferPoolInstance(0, FRAMES, disk_manager.get());
    page_id_t page_id = bpm->NewPage();
    PageKeyCompator comparator;
    size_t bucket_cnt = 10;
    HashGeneric tree(tid, page_id, bpm, comparator,bucket_cnt);

    // 1. 先串行插入一批数据
  const int num_threads = 20;         // 线程数
  const int keys_per_thread = 10;   // 每个线程插入的键数
  const int total_keys = (num_threads * keys_per_thread);
  // 用于收集所有插入的键
  // std::vector<page_key_t> all_keys;
  std::vector<page_key_t> inserted = {};
  // std::vector<page_key_t> deleted = {};
  std::mutex keys_mutex;  // 保护all_keys的互斥锁
  std::vector<page_key_t> all_keys = generateRandomUniqueKeys(total_keys);
  // std::vector<page_key_t> keys = {31, 16, 4, 35, 28, 6, 38, 34, 20, 30, 33, 8, 11, 10, 39, 40, 15, 5, 29, 14};
  std::string keys_str = "";
    for (size_t i = 0; i < all_keys.size(); i++)
    {
      keys_str += std::to_string(all_keys[i]);
      if (i != all_keys.size() - 1) {
        keys_str += ", ";
      }
    }
    LOG_DEBUG("Keys to insert: " + keys_str);
    // 线程函数 - 每个线程插入一组唯一的键
    auto insert_task = [&](int thread_id) {
      for (int i = 0; i < keys_per_thread; ++i) {
          page_key_t key = all_keys[thread_id * keys_per_thread + i];
          uint64_t value = key & 0xFFFFFFFF;
            // 执行插入
          bool res = tree.Insert(reinterpret_cast<byte*>(&key), DASET_PAGE_KEY_LEN, reinterpret_cast<byte*>(&value), sizeof(uint64_t));
          EXPECT_TRUE(res); 
          // 记录插入的键(需要加锁)
          {
              inserted.push_back(key);
              std::lock_guard<std::mutex> lock(keys_mutex);
              byte* value_tmp = new byte[sizeof(uint64_t)];
              size_t value_len;
              LOG_DEBUG("insert key : "+std::to_string(key));
              // tree.PrintfAllLeaf();
              bool is_pres = tree.Search(reinterpret_cast<byte*>(&key), DASET_PAGE_KEY_LEN, value_tmp, value_len);
              if(!is_pres){
                tree.Search(reinterpret_cast<byte*>(&key), DASET_PAGE_KEY_LEN, value_tmp, value_len);
              }
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
    EXPECT_EQ(inserted.size(), total_keys);

    // 检查每个键是否存在
    for (auto key : inserted) {
        is_present = tree.Search(reinterpret_cast<byte*>(&key), DASET_PAGE_KEY_LEN, 
                               value, value_len);

        EXPECT_TRUE(is_present) << "Key " << key << " not found after concurrent insertion";
        if(!is_present){
          tree.Search(reinterpret_cast<byte*>(&key), DASET_PAGE_KEY_LEN, value, value_len);
        }
    }

    // 清理资源
    remove(db_fname.c_str());
    delete value;
    delete bpm;
}

}  // namespace bustub
