#include <cstring>

#include "gtest/gtest.h"
#include "storage/disk/disk_scheduler.h"

namespace daset {

static std::string db_fname = "test.db";


class DiskSchedulerTest : public ::testing::Test {
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
TEST_F(DiskSchedulerTest, ScheduleWriteReadPageTest) {
  char buf[DASET_PAGE_SIZE] = {0};
  char data[DASET_PAGE_SIZE] = {0};

  auto disk_manager = std::make_unique<DiskManager>(db_fname);
  auto disk_scheduler = std::make_unique<DiskScheduler>(disk_manager.get());

  std::strncpy(data, "A test string.", sizeof(data));

  auto promise_future1 = disk_scheduler->CreatePromise();
  auto promise_future2 = disk_scheduler->CreatePromise();

  auto promise1 = std::move(promise_future1.first);
  auto promise2 = std::move(promise_future2.first);
  auto future1 = std::move(promise_future1.second);
  auto future2 = std::move(promise_future2.second);

  disk_scheduler->Scheduler(DiskRequest(PAGE_WRITE, 0, std::move(promise1), data));
  disk_scheduler->Scheduler(DiskRequest(PAGE_READ, 0, std::move(promise2), buf));
  ASSERT_TRUE(future1.get());
  ASSERT_TRUE(future2.get());
  ASSERT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  disk_scheduler = nullptr;  // Call the DiskScheduler destructor to finish all scheduled jobs.
  disk_manager->Shutdown();
}

}