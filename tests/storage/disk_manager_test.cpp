#include <cstring>

#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"

namespace daset {

static std::string db_fname = "test.db";


class DiskManagerTest : public ::testing::Test {
 protected:
  // This function is called before every test.
  void SetUp() override {
    remove(db_fname.c_str());
  }

  // This function is called after every test.
  void TearDown() override {
    remove(db_fname.c_str());
  };
};

// NOLINTNEXTLINE
TEST_F(DiskManagerTest, ReadWritePageTest) {
  char buf[DASET_PAGE_SIZE] = {0};
  char data[DASET_PAGE_SIZE] = {0};
  DiskManager dm(db_fname);
  std::strncpy(data, "A test string.", sizeof(data));

  dm.ReadPage(0, buf);  // tolerate empty read

  dm.WritePage(0, data);
  dm.ReadPage(0, buf);
  EXPECT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  std::memset(buf, 0, sizeof(buf));
  dm.WritePage(5, data);
  dm.ReadPage(5, buf);
  EXPECT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  dm.Shutdown();
}

TEST_F(DiskManagerTest, DeletePageTest) {
  char buf[DASET_PAGE_SIZE] = {0};
  char data[DASET_PAGE_SIZE] = {0};
  DiskManager dm(db_fname);

  dm.ReadPage(0, buf);  // tolerate empty read

  std::strncpy(data, "A test string.", sizeof(data));
  size_t pages_to_write = 100;
  for (page_id_t page_id = 0; page_id < static_cast<page_id_t>(pages_to_write); page_id++) {
    dm.WritePage(page_id, data);
    dm.ReadPage(page_id, buf);
    EXPECT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);
  }

  pages_to_write *= 2;
  std::strncpy(data, "test string version 2", sizeof(data));
  for (page_id_t page_id = 0; page_id < static_cast<page_id_t>(pages_to_write); page_id++) {
    dm.WritePage(page_id, data);
    dm.ReadPage(page_id, buf);
    EXPECT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

    dm.DeletePage(page_id);
  }

  // expect no change in file size after delete because we're reclaiming space

  dm.Shutdown();
}

}  // namespace bustub
