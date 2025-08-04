#include "buffer/lru_replacer.h"
#include "gtest/gtest.h"
#include <thread>
#include "util/daset_debug_logger.h"

namespace daset{

static std::string test_log_file = "daset.log";

TEST(LRUReplacerTest, SampleTest) {
    DebugLogger::getInstance().setLevel(DebugLogger::Level::DEBUG);
    DebugLogger::getInstance().setOutputFile(test_log_file);
    LRUReplacer lru_replacer(7);

    // Scenario: unpin six elements, i.e. add them to the replacer.
    lru_replacer.Unpin(1);
    lru_replacer.Unpin(2);
    lru_replacer.Unpin(3);
    lru_replacer.Unpin(4);
    lru_replacer.Unpin(5);
    lru_replacer.Unpin(6);
    lru_replacer.Unpin(1);
    EXPECT_EQ(6, lru_replacer.Size());

    // Scenario: get three victims from the lru.
    int value;
    lru_replacer.Evict(value);
    EXPECT_EQ(1, value);
    lru_replacer.Evict(value);
    EXPECT_EQ(2, value);
    lru_replacer.Evict(value);
    EXPECT_EQ(3, value);

    // Scenario: pin elements in the replacer.
    // Note that 3 has already been victimized, so pinning 3 should have no effect.
    lru_replacer.Pin(3);
    lru_replacer.Pin(4);
    EXPECT_EQ(2, lru_replacer.Size());

    // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
    lru_replacer.Unpin(4);

    // Scenario: continue looking for victims. We expect these victims.
    lru_replacer.Evict(value);
    EXPECT_EQ(5, value);
    lru_replacer.Evict(value);
    EXPECT_EQ(6, value);
    lru_replacer.Evict(value);
    EXPECT_EQ(4, value);
}

TEST(LRUReplacerTest, MultiThreadTest) {
    DebugLogger::getInstance().setLevel(DebugLogger::Level::DEBUG);
    DebugLogger::getInstance().setOutputFile(test_log_file);
    LRUReplacer lru_replacer(100);
    const int num_threads = 100;
    const int num_operations = 1000;

    std::vector<std::thread> threads;
    std::mutex print_mutex;

    auto worker = [&](int thread_id) {
        for (int i = 0; i < num_operations; ++i) {
            int value = thread_id * num_operations + i;
            lru_replacer.Unpin(value);

            int victim;
            if (lru_replacer.Evict(victim)) {
                std::unique_lock<std::mutex> lock(print_mutex);
                std::cout << "Thread " << thread_id << " got victim: " << victim << std::endl;
            }

            lru_replacer.Pin(value);
        }
    };

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i);
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(lru_replacer.Size(), 0);
}

}