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

bool TreeValuesMatch(HashGeneric &tree, std::vector<page_key_t> &inserted,
                     std::vector<page_key_t> &deleted) {
  // std::vector<KeyValue> rids;
  // KeyType index_key;
  for (auto &key : inserted) {
    // rids.clear();
    // index_key.SetFromInteger(key);
    byte* value = new byte[sizeof(uint64_t)];
    size_t value_len;
    bool in_present = false;
    in_present = tree.Search(reinterpret_cast<byte*>(&key),sizeof(uint64_t),value,value_len);
    if (!in_present) {
      return false;
    }
  }
  for (auto &key : deleted) {
    byte* value = new byte[sizeof(uint64_t)];
    size_t value_len;
    bool in_present = false;
    in_present = tree.Search(reinterpret_cast<byte*>(&key),sizeof(uint64_t),value,value_len);
    if (in_present) {
      return false;
    }
    delete value;
  }
  return true;
}


static std::string db_fname = "test.db";

const size_t FRAMES = 50;

static std::string test_log_file = "daset.log";

TEST(HashTreeTests, DeleteTestNoIterator) {
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
  size_t bucket_cnt = 10;
  HashGeneric tree(tid,page_id,bpm,comparator,bucket_cnt);

  std::vector<page_key_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    uint64_t value = key & 0xFFFFFFFF;
    tree.Insert(reinterpret_cast<byte*>(&key),DASET_PAGE_KEY_LEN,reinterpret_cast<byte*>(&value),sizeof(uint64_t));
  }

  bool is_present;
  byte* value = new byte[sizeof(uint64_t)];
  size_t value_len;
  for (auto key : keys) {
    is_present = tree.Search(reinterpret_cast<byte*>(&key),DASET_PAGE_KEY_LEN,value,value_len);
    EXPECT_EQ(is_present, true);
  }

  std::vector<page_key_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    tree.Remove(reinterpret_cast<byte*>(&key),DASET_PAGE_KEY_LEN);
  }

  int64_t size = 0;
//   bool is_present;

  for (auto key : keys) {
    is_present = tree.Search(reinterpret_cast<byte*>(&key),DASET_PAGE_KEY_LEN,value,value_len);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      ++size;
    }
  }
  EXPECT_EQ(size, 1);

  // Remove the remaining key
  page_key_t key = 2;
  tree.Remove(reinterpret_cast<byte*>(&key),DASET_PAGE_KEY_LEN);
  // auto root_page_id = tree.GetRootPageId();
  // ASSERT_EQ(root_page_id, DASET_INVALID_PAGE_ID);
  delete value;
  delete bpm;
}

TEST(HashTreeTests, SequentialEdgeMixTest) {  // NOLINT
  DebugLogger::getInstance().setLevel(DebugLogger::Level::DEBUG);
  DebugLogger::getInstance().setOutputFile(test_log_file);
  remove(db_fname.c_str());
  TableID tid = 0;  
  auto disk_manager = std::make_unique<DiskManager>(db_fname);
  auto *bpm = new BufferPoolInstance(0, FRAMES, disk_manager.get());

    page_id_t page_id = bpm->NewPage();
    PageKeyCompator comparator;
    size_t bucket_cnt_ = 40;
    HashGeneric tree(tid,page_id,bpm,comparator,bucket_cnt_);

    std::vector<page_key_t> keys = {1, 5, 15, 20, 25, 2, 11, 22, 6, 14, 4};
    std::vector<page_key_t> inserted = {};
    std::vector<page_key_t> deleted = {};
    for (auto key : keys) {
      uint64_t value = key & 0xFFFFFFFF;
      tree.Insert(reinterpret_cast<byte*>(&key),DASET_PAGE_KEY_LEN,reinterpret_cast<byte*>(&value),sizeof(uint64_t));
      inserted.push_back(key);
      auto res = TreeValuesMatch(tree, inserted, deleted);
      ASSERT_TRUE(res);
    }
    tree.PrintfAllLeaf();
    page_key_t tmp_key = 1;
    tree.Remove(reinterpret_cast<byte*>(&tmp_key),DASET_PAGE_KEY_LEN);
    deleted.push_back(1);
    inserted.erase(std::find(inserted.begin(), inserted.end(), 1));
    auto res = TreeValuesMatch(tree, inserted, deleted);
    ASSERT_TRUE(res);

    tmp_key = 3;
    uint64_t value = tmp_key & 0xFFFFFFFF;
    tree.Insert(reinterpret_cast<byte*>(&tmp_key),DASET_PAGE_KEY_LEN,reinterpret_cast<byte*>(&value),sizeof(uint64_t));
    inserted.push_back(3);
    res = TreeValuesMatch(tree, inserted, deleted);
    ASSERT_TRUE(res);

    keys = {4, 14, 6, 2, 15, 22, 11, 3, 5, 25, 20};
    for (auto key : keys) {
      // index_key.SetFromInteger(key);
      tree.Remove(reinterpret_cast<byte*>(&key),DASET_PAGE_KEY_LEN);
      deleted.push_back(key);
      inserted.erase(std::find(inserted.begin(), inserted.end(), key));
      res = TreeValuesMatch(tree, inserted, deleted);
      ASSERT_TRUE(res);
    }
  // }
  remove(db_fname.c_str());
  delete bpm;
}


std::vector<page_key_t> generateRandomUniqueKeys(size_t n) {
    // 1. 生成不重复的随机数（大于0）
    std::unordered_set<page_key_t> unique_keys;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<page_key_t> dist(1, n*2); // 确保足够大的范围避免重复

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

TEST(HashTreeTests, DeleteSingleTest) {  // NOLINT
  DebugLogger::getInstance().setLevel(DebugLogger::Level::DEBUG);
  DebugLogger::getInstance().setOutputFile(test_log_file);
  remove(db_fname.c_str());
  TableID tid = 0;  
  auto disk_manager = std::make_unique<DiskManager>(db_fname);
  auto *bpm = new BufferPoolInstance(0, FRAMES, disk_manager.get());

    page_id_t page_id = bpm->NewPage();
    PageKeyCompator comparator;
    size_t bucket_cnt = 40;
    HashGeneric tree(tid,page_id,bpm,comparator,bucket_cnt);

    // std::vector<page_key_t> keys = generateRandomUniqueKeys(20);
    std::vector<page_key_t> keys = {22, 6, 32, 19, 31, 24, 28, 4, 13, 1, 5, 14, 10, 3, 35, 8, 17, 36, 9, 33};
    std::vector<page_key_t> inserted = {};
    std::vector<page_key_t> deleted = {};
    std::string keys_str = "";
    for (size_t i = 0; i < keys.size(); i++)
    {
      keys_str += std::to_string(keys[i]);
      if (i != keys.size() - 1) {
        keys_str += ", ";
      }
    }
    LOG_DEBUG("Keys to insert: " + keys_str);
    for (auto key : keys) {
      uint64_t value = key & 0xFFFFFFFF;
      tree.Insert(reinterpret_cast<byte*>(&key),DASET_PAGE_KEY_LEN,reinterpret_cast<byte*>(&value),sizeof(uint64_t));
      inserted.push_back(key);
      auto res = TreeValuesMatch(tree, inserted, deleted);
      ASSERT_TRUE(res);
    }
    tree.PrintfAllLeaf();
    std::vector<page_key_t> to_del_keys;
    for (size_t i = 0; i < keys.size()/2; i++)
    {
      to_del_keys.push_back(keys[i]);
    }
    for (auto key : to_del_keys) {
      // index_key.SetFromInteger(key);
      tree.Remove(reinterpret_cast<byte*>(&key),DASET_PAGE_KEY_LEN);
      deleted.push_back(key);
      inserted.erase(std::find(inserted.begin(), inserted.end(), key));
      auto res = TreeValuesMatch(tree, inserted, deleted);
      tree.PrintfAllLeaf();
      // ASSERT_TRUE(res);
      // if(!res){
      //   TreeValuesMatch(tree, inserted, deleted);
      // }
    }
  // }
  tree.PrintfAllLeaf();
  remove(db_fname.c_str());
  delete bpm;
}


TEST(HashTreeTests, ConcurrentDeleteTest) {  // NOLINT
  DebugLogger::getInstance().setLevel(DebugLogger::Level::DEBUG);
  DebugLogger::getInstance().setOutputFile(test_log_file);
  remove(db_fname.c_str());
  TableID tid = 0;  
  auto disk_manager = std::make_unique<DiskManager>(db_fname);
  auto *bpm = new BufferPoolInstance(0, FRAMES, disk_manager.get());

  page_id_t page_id = bpm->NewPage();
  PageKeyCompator comparator;
  size_t bucket_cnt = 800;
  HashGeneric tree(tid,page_id,bpm,comparator,bucket_cnt);

  // 1. 先串行插入一批数据
  const int num_threads = 10;         // 线程数
  const int keys_per_thread = 20;   // 每个线程插入的键数
  const int total_keys = (num_threads * keys_per_thread)*2;
  // 用于收集所有插入的键
  // std::vector<page_key_t> all_keys;
  std::vector<page_key_t> inserted = {};
  std::vector<page_key_t> deleted = {};
  std::mutex keys_mutex;  // 保护all_keys的互斥锁
  std::vector<page_key_t> keys = generateRandomUniqueKeys(total_keys);
  // std::vector<page_key_t> keys = {31, 16, 4, 35, 28, 6, 38, 34, 20, 30, 33, 8, 11, 10, 39, 40, 15, 5, 29, 14};
  std::string keys_str = "";
    for (size_t i = 0; i < keys.size(); i++)
    {
      keys_str += std::to_string(keys[i]);
      if (i != keys.size() - 1) {
        keys_str += ", ";
      }
    }
    LOG_DEBUG("Keys to insert: " + keys_str);
  for (auto key : keys) {
    uint64_t value = key & 0xFFFFFFFF;
    tree.Insert(reinterpret_cast<byte*>(&key), DASET_PAGE_KEY_LEN,
                reinterpret_cast<byte*>(&value), sizeof(uint64_t));
    inserted.push_back(key);
  }
  tree.PrintfAllLeaf();   // 调试用
  // 线程函数 - 每个线程插入一组唯一的键
  auto del_task = [&](int thread_id) {
    for (int i = 0; i < keys_per_thread; ++i) {
        page_key_t key = keys[thread_id * keys_per_thread + i];
        bool res = tree.Remove(reinterpret_cast<byte*>(&key), DASET_PAGE_KEY_LEN);
        EXPECT_TRUE(res);
        {
            std::lock_guard<std::mutex> lock(keys_mutex);
            deleted.push_back(key);
            inserted.erase(std::find(inserted.begin(), inserted.end(), key));
            auto res = TreeValuesMatch(tree, inserted, deleted);
            // tree.PrintfAllLeaf();
        }
    }
  };
  // 创建并启动所有线程
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; ++i) {
      threads.emplace_back(del_task, i);
  }
  // 等待所有线程完成
  for (auto &t : threads) {
      t.join();
  }
  // 清理
  tree.PrintfAllLeaf();   // 调试用
  remove(db_fname.c_str());
  delete bpm;
}


}