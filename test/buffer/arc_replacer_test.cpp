#include "gtest/gtest.h"

#include <buffer/arc_replacer.hpp>

TEST(ArcReplacerTest, BasicTest1) {
  ArcReplacer arc(5);

  arc.RecordAccess(1, 1);
  arc.RecordAccess(2, 2);
  arc.RecordAccess(3, 3);
  arc.RecordAccess(4, 4);
  arc.SetEvictable(1, true);
  arc.SetEvictable(2, true);
  arc.SetEvictable(3, true);
  arc.SetEvictable(4, false);

  ASSERT_EQ(arc.Size(), 3);

  arc.RecordAccess(1, 1);

  ASSERT_EQ(arc.Evict(), 2);
  ASSERT_EQ(arc.Evict(), 3);
  ASSERT_EQ(arc.Evict(), 1);
  ASSERT_EQ(arc.Evict(), std::nullopt);
  ASSERT_EQ(arc.Size(), 0);

  arc.RecordAccess(2, 5);
  arc.SetEvictable(2, true);

  arc.RecordAccess(3, 2);
  arc.SetEvictable(3, true);

  ASSERT_EQ(arc.Size(), 2);
}

TEST(ArcReplacerTest, BasicTest2) {
  ArcReplacer arc(3);
  arc.RecordAccess(1, 1);
  arc.SetEvictable(1, true);
  arc.RecordAccess(2, 2);
  arc.SetEvictable(2, true);
  arc.RecordAccess(3, 3);
  arc.SetEvictable(3, true);
  ASSERT_EQ(3, arc.Size());

  ASSERT_EQ(1, arc.Evict());
  ASSERT_EQ(2, arc.Evict());
  ASSERT_EQ(3, arc.Evict());
  ASSERT_EQ(0, arc.Size());

  arc.RecordAccess(3, 4);
  arc.SetEvictable(3, true);

  arc.RecordAccess(2, 1);
  arc.SetEvictable(2, true);
  ASSERT_EQ(2, arc.Size());

  arc.RecordAccess(1, 3);
  arc.SetEvictable(1, true);

  ASSERT_EQ(3, arc.Evict());
  ASSERT_EQ(2, arc.Evict());
  ASSERT_EQ(1, arc.Evict());

  arc.RecordAccess(1, 1);
  arc.SetEvictable(1, true);

  arc.RecordAccess(2, 4);
  arc.SetEvictable(2, true);

  arc.RecordAccess(3, 5);
  arc.SetEvictable(3, true);
  ASSERT_EQ(1, arc.Evict());

  arc.RecordAccess(1, 6);
  arc.SetEvictable(1, true);
  ASSERT_EQ(2, arc.Evict());

  arc.RecordAccess(2, 7);
  arc.SetEvictable(2, true);
  ASSERT_EQ(3, arc.Evict());

  arc.RecordAccess(3, 5);
  arc.SetEvictable(3, true);

  ASSERT_EQ(3, arc.Evict());

  arc.RecordAccess(3, 2);
  arc.SetEvictable(3, true);

  ASSERT_EQ(1, arc.Evict());

  arc.RecordAccess(1, 3);
  arc.SetEvictable(1, true);

  ASSERT_EQ(2, arc.Evict());
  ASSERT_EQ(3, arc.Evict());
  ASSERT_EQ(1, arc.Evict());
}

TEST(ArcReplacerTest, RecordAccessPerformanceTest) {
  const size_t bpm_size = 256 << 10;
  ArcReplacer arc_replacer(bpm_size);
  for (size_t i = 0; i < bpm_size; i++) {
    arc_replacer.RecordAccess(i, i);
    arc_replacer.SetEvictable(i, true);
  }
  std::vector<std::thread> threads;
  const size_t rounds = 10;
  size_t access_frame_id = 256 << 9;
  std::vector<size_t> access_times;
  for (size_t round = 0; round < rounds; round++) {
    auto start_time = std::chrono::system_clock::now();
    for (size_t i = 0; i < bpm_size; i++) {
      arc_replacer.RecordAccess(access_frame_id, access_frame_id);
      access_frame_id = (access_frame_id + 1) % bpm_size;
    }
    auto end_time = std::chrono::system_clock::now();
    size_t time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    access_times.push_back(time);
  }
  double total = 0;
  for (const auto &x : access_times) {
    total += x;
  }
  total /= 1000;
  double avg = total / access_times.size();
  ASSERT_LT(avg, 3);
}
