#ifndef _BUFFER_POOL_MANAGER_HPP_
#define _BUFFER_POOL_MANAGER_HPP_

#include <buffer/arc_replacer.hpp>
#include <config.hpp>
#include <storage/disk_manager.hpp>
#include <storage/disk_scheduler.hpp>
#include <storage/page_guard.hpp>

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;

class FrameHeader {
  friend class BufferPoolManager;
  friend class ReadPageGuard;
  friend class WritePageGuard;

 public:
  FrameHeader(FrameId_t);

 private:
  const char* GetData() const;
  char* GetDataMut();
  void Reset();

  const FrameId_t frame_id_;
  std::shared_mutex rw_mutex_;
  std::atomic<size_t> pin_count_;
  bool is_dirty_{false};
  std::vector<char> data_;
};

class BufferPoolManager {
 public:
  BufferPoolManager(size_t, DiskManager*);
  ~BufferPoolManager();

  size_t Size() const;
  PageId_t NewPage();
  bool DeletePage(PageId_t);
  std::optional<WritePageGuard> CheckedWritePage(
      PageId_t, AccessType access_type = AccessType::Unknown);
  std::optional<ReadPageGuard> CheckedReadPage(
      PageId_t, AccessType access_type = AccessType::Unknown);
  WritePageGuard WritePage(PageId_t,
                           AccessType access_type = AccessType::Unknown);
  ReadPageGuard ReadPage(PageId_t,
                         AccessType access_type = AccessType::Unknown);
  bool FlushPageUnsafe(PageId_t);
  bool FlushPage(PageId_t);
  void FlushAllPagesUnsafe();
  void FlushAllPages();
  std::optional<size_t> GetPinCount(PageId_t);

 private:
  const size_t num_frames_;
  std::atomic<PageId_t> next_page_id_;
  std::shared_ptr<std::mutex> mutex_;
  std::vector<std::shared_ptr<FrameHeader>> frames_;
  std::unordered_map<PageId_t, FrameId_t> page_table_;
  std::unordered_map<FrameId_t, PageId_t> rev_page_table_;
  std::list<FrameId_t> free_frames_;
  std::shared_ptr<ArcReplacer> replacer_;
  std::shared_ptr<DiskScheduler> disk_scheduler_;
};

#endif
