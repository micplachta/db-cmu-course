#include <cstdio>
#include <filesystem>

#include "gtest/gtest.h"

#include <buffer/buffer_pool_manager.hpp>
#include <storage/page_guard.hpp>

#include <iostream>

static std::filesystem::path db_filename("test.db");

const size_t FRAMES = 10;

TEST(BufferPoolManagerTest, BasicTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_filename);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  const PageId_t pid = bpm->NewPage();
  const std::string str = "Hello, world!";

  {
    auto guard = bpm->WritePage(pid);
    snprintf(guard.GetDataMut(), DB_PAGE_SIZE, "%s", str.c_str());
    EXPECT_STREQ(guard.GetData(), str.c_str());
  }

  {
    const auto guard = bpm->ReadPage(pid);
    EXPECT_STREQ(guard.GetData(), str.c_str());
  }

  {
    const auto guard = bpm->ReadPage(pid);
    EXPECT_STREQ(guard.GetData(), str.c_str());
  }

  ASSERT_TRUE(bpm->DeletePage(pid));

  remove(db_filename);
  remove(disk_manager->GetLogFileName());
}

TEST(BufferPoolManagerTest, PagePinEasyTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_filename);
  auto bpm = std::make_shared<BufferPoolManager>(2, disk_manager.get());

  const PageId_t pageid0 = bpm->NewPage();
  const PageId_t pageid1 = bpm->NewPage();

  const std::string str0 = "page0";
  const std::string str1 = "page1";
  const std::string str0updated = "page0updated";
  const std::string str1updated = "page1updated";

  {
    auto page0_write_opt = bpm->CheckedWritePage(pageid0);
    ASSERT_TRUE(page0_write_opt.has_value());
    auto page0_write = std::move(page0_write_opt.value());
    snprintf(page0_write.GetDataMut(), DB_PAGE_SIZE, "%s", str0.c_str());

    auto page1_write_opt = bpm->CheckedWritePage(pageid1);
    ASSERT_TRUE(page1_write_opt.has_value());
    auto page1_write = std::move(page1_write_opt.value());
    snprintf(page1_write.GetDataMut(), DB_PAGE_SIZE, "%s", str1.c_str());

    ASSERT_EQ(1, bpm->GetPinCount(pageid0));
    ASSERT_EQ(1, bpm->GetPinCount(pageid1));

    const auto temp_page_id1 = bpm->NewPage();
    const auto temp_page1_opt = bpm->CheckedReadPage(temp_page_id1);
    ASSERT_FALSE(temp_page1_opt.has_value());

    const auto temp_page_id2 = bpm->NewPage();
    const auto temp_page2_opt = bpm->CheckedWritePage(temp_page_id2);
    ASSERT_FALSE(temp_page2_opt.has_value());

    ASSERT_EQ(1, bpm->GetPinCount(pageid0));
    page0_write.Drop();
    ASSERT_EQ(0, bpm->GetPinCount(pageid0));

    ASSERT_EQ(1, bpm->GetPinCount(pageid1));
    page1_write.Drop();
    ASSERT_EQ(0, bpm->GetPinCount(pageid1));
  }

  {
    const auto temp_page_id1 = bpm->NewPage();
    const auto temp_page1_opt = bpm->CheckedReadPage(temp_page_id1);
    ASSERT_TRUE(temp_page1_opt.has_value());

    const auto temp_page_id2 = bpm->NewPage();
    const auto temp_page2_opt = bpm->CheckedWritePage(temp_page_id2);
    ASSERT_TRUE(temp_page2_opt.has_value());

    ASSERT_FALSE(bpm->GetPinCount(pageid0).has_value());
    ASSERT_FALSE(bpm->GetPinCount(pageid1).has_value());
  }

  {
    auto page0_write_opt = bpm->CheckedWritePage(pageid0);
    ASSERT_TRUE(page0_write_opt.has_value());
    auto page0_write = std::move(page0_write_opt.value());
    EXPECT_STREQ(page0_write.GetData(), str0.c_str());
    snprintf(page0_write.GetDataMut(), DB_PAGE_SIZE, "%s", str0updated.c_str());

    auto page1_write_opt = bpm->CheckedWritePage(pageid1);
    ASSERT_TRUE(page1_write_opt.has_value());
    auto page1_write = std::move(page1_write_opt.value());
    EXPECT_STREQ(page1_write.GetData(), str1.c_str());
    snprintf(page1_write.GetDataMut(), DB_PAGE_SIZE, "%s", str1updated.c_str());

    ASSERT_EQ(1, bpm->GetPinCount(pageid0));
    ASSERT_EQ(1, bpm->GetPinCount(pageid1));
  }

  ASSERT_EQ(0, bpm->GetPinCount(pageid0));
  ASSERT_EQ(0, bpm->GetPinCount(pageid1));

  {
    auto page0_read_opt = bpm->CheckedReadPage(pageid0);
    ASSERT_TRUE(page0_read_opt.has_value());
    const auto page0_read = std::move(page0_read_opt.value());
    EXPECT_STREQ(page0_read.GetData(), str0updated.c_str());

    auto page1_read_opt = bpm->CheckedReadPage(pageid1);
    ASSERT_TRUE(page1_read_opt.has_value());
    const auto page1_read = std::move(page1_read_opt.value());
    EXPECT_STREQ(page1_read.GetData(), str1updated.c_str());

    ASSERT_EQ(1, bpm->GetPinCount(pageid0));
    ASSERT_EQ(1, bpm->GetPinCount(pageid1));
  }

  ASSERT_EQ(0, bpm->GetPinCount(pageid0));
  ASSERT_EQ(0, bpm->GetPinCount(pageid1));

  remove(db_filename);
  remove(disk_manager->GetLogFileName());
}

TEST(BufferPoolManagerTest, PagePinMediumTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_filename);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  const auto pid0 = bpm->NewPage();
  auto page0 = bpm->WritePage(pid0);

  const std::string hello = "Hello";
  snprintf(page0.GetDataMut(), DB_PAGE_SIZE, "%s", hello.c_str());
  EXPECT_STREQ(page0.GetData(), hello.c_str());

  page0.Drop();

  std::vector<WritePageGuard> pages;
  pages.reserve(FRAMES);

  for (size_t i = 0; i < FRAMES; i++) {
    const auto pid = bpm->NewPage();
    auto page = bpm->WritePage(pid);
    pages.push_back(std::move(page));
  }

  for (const auto &page : pages) {
    const auto pid = page.GetPageId();
    EXPECT_EQ(1, bpm->GetPinCount(pid));
  }

  for (size_t i = 0; i < FRAMES; i++) {
    const auto pid = bpm->NewPage();
    const auto fail = bpm->CheckedWritePage(pid);
    ASSERT_FALSE(fail.has_value());
  }

  for (size_t i = 0; i < FRAMES / 2; i++) {
    const auto pid = pages[0].GetPageId();
    EXPECT_EQ(1, bpm->GetPinCount(pid));
    pages.erase(pages.begin());
    EXPECT_EQ(0, bpm->GetPinCount(pid));
  }

  for (const auto &page : pages) {
    const auto pid = page.GetPageId();
    EXPECT_EQ(1, bpm->GetPinCount(pid));
  }

  for (size_t i = 0; i < ((FRAMES / 2) - 1); i++) {
    const auto pid = bpm->NewPage();
    auto page = bpm->WritePage(pid);
    pages.push_back(std::move(page));
  }

  {
    const auto original_page = bpm->ReadPage(pid0);
    EXPECT_STREQ(original_page.GetData(), hello.c_str());
  }

  const auto last_pid = bpm->NewPage();
  const auto last_page = bpm->ReadPage(last_pid);

  const auto fail = bpm->CheckedReadPage(pid0);
  ASSERT_FALSE(fail.has_value());

  disk_manager->ShutDown();
  remove(db_filename);
  remove(disk_manager->GetLogFileName());
}

TEST(BufferPoolManagerTest, PageAccessTest) {
  const size_t rounds = 50;

  auto disk_manager = std::make_shared<DiskManager>(db_filename);
  auto bpm = std::make_shared<BufferPoolManager>(1, disk_manager.get());

  const auto pid = bpm->NewPage();
  char buf[DB_PAGE_SIZE];

  auto thread = std::thread([&]() {
    for (size_t i = 0; i < rounds; i++) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      auto guard = bpm->WritePage(pid);
      snprintf(guard.GetDataMut(), DB_PAGE_SIZE, "%s", std::to_string(i).c_str());
    }
  });

  for (size_t i = 0; i < rounds; i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    const auto guard = bpm->ReadPage(pid);
    memcpy(buf, guard.GetData(), DB_PAGE_SIZE);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_STREQ(guard.GetData(), buf);
  }

  thread.join();
  remove(db_filename);
  remove(disk_manager->GetLogFileName());
}

TEST(BufferPoolManagerTest, ContentionTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_filename);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  const size_t rounds = 100000;

  const auto pid = bpm->NewPage();

  auto thread1 = std::thread([&]() {
    for (size_t i = 0; i < rounds; i++) {
      auto guard = bpm->WritePage(pid);
      snprintf(guard.GetDataMut(), DB_PAGE_SIZE, "%s", std::to_string(i).c_str());
    }
  });

  auto thread2 = std::thread([&]() {
    for (size_t i = 0; i < rounds; i++) {
      auto guard = bpm->WritePage(pid);
      snprintf(guard.GetDataMut(), DB_PAGE_SIZE, "%s", std::to_string(i).c_str());
    }
  });

  auto thread3 = std::thread([&]() {
    for (size_t i = 0; i < rounds; i++) {
      auto guard = bpm->WritePage(pid);
      snprintf(guard.GetDataMut(), DB_PAGE_SIZE, "%s", std::to_string(i).c_str());
    }
  });

  auto thread4 = std::thread([&]() {
    for (size_t i = 0; i < rounds; i++) {
      auto guard = bpm->WritePage(pid);
      snprintf(guard.GetDataMut(), DB_PAGE_SIZE, "%s", std::to_string(i).c_str());
    }
  });

  thread3.join();
  thread2.join();
  thread4.join();
  thread1.join();
  remove(db_filename);
  remove(disk_manager->GetLogFileName());
}

TEST(BufferPoolManagerTest, DeadlockTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_filename);
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  const auto pid0 = bpm->NewPage();
  const auto pid1 = bpm->NewPage();

  auto guard0 = bpm->WritePage(pid0);

  std::atomic<bool> start = false;

  auto child = std::thread([&]() {
    start.store(true);
    const auto guard0 = bpm->WritePage(pid0);
  });

  while (!start.load()) {}

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  const auto guard1 = bpm->WritePage(pid1);

  guard0.Drop();
  child.join();
  remove(db_filename);
  remove(disk_manager->GetLogFileName());
}

TEST(BufferPoolManagerTest, EvictableTest) {
  const size_t rounds = 1000;
  const size_t num_readers = 8;

  auto disk_manager = std::make_shared<DiskManager>(db_filename);
  auto bpm = std::make_shared<BufferPoolManager>(1, disk_manager.get());

  for (size_t i = 0; i < rounds; i++) {
    std::mutex mutex;
    std::condition_variable cv;

    bool signal = false;

    const auto winner_pid = bpm->NewPage();
    const auto loser_pid = bpm->NewPage();

    std::vector<std::thread> readers;
    for (size_t j = 0; j < num_readers; j++) {
      readers.emplace_back([&]() {
        std::unique_lock<std::mutex> lock(mutex);

        while (!signal) {
          cv.wait(lock);
        }

        const auto read_guard = bpm->ReadPage(winner_pid);
        ASSERT_FALSE(bpm->CheckedReadPage(loser_pid).has_value());
      });
    }

    std::unique_lock<std::mutex> lock(mutex);

    if (i % 2 == 0) {
      auto read_guard = bpm->ReadPage(winner_pid);

      signal = true;
      cv.notify_all();
      lock.unlock();

      read_guard.Drop();
    } else {
      auto write_guard = bpm->WritePage(winner_pid);

      signal = true;
      cv.notify_all();
      lock.unlock();

      write_guard.Drop();
    }

    for (size_t i = 0; i < num_readers; i++) {
      readers[i].join();
    }
  }
  remove(db_filename);
  remove(disk_manager->GetLogFileName());
}
