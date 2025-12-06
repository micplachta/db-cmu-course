#ifndef _PAGE_GUARD_HPP_
#define _PAGE_GUARD_HPP_

#include <storage/disk_scheduler.hpp>
#include <buffer/arc_replacer.hpp>
// #include <buffer/buffer_pool_manager.hpp>

#include <shared_mutex>
#include <mutex>

class FrameHeader;
class BufferPoolManager;

class ReadPageGuard {
  friend class BufferPoolManager;

public:
  ReadPageGuard() = default;
  ReadPageGuard(const ReadPageGuard&) = delete;
  ReadPageGuard& operator=(const ReadPageGuard&) = delete;
  ReadPageGuard(ReadPageGuard&&) noexcept;
  ReadPageGuard& operator=(ReadPageGuard&&) noexcept;

  ~ReadPageGuard();

  PageId_t GetPageId() const;
  const char* GetData() const;
  template <class T>
  const T* As() const {
    return reinterpret_cast<const T*>(GetData());
  }

  bool IsDirty() const;
  void Flush();
  void Drop();

private:
  ReadPageGuard(PageId_t, std::shared_ptr<FrameHeader>, std::shared_ptr<ArcReplacer>, std::shared_ptr<std::mutex>, std::shared_ptr<DiskScheduler>);

  PageId_t page_id_;
  std::shared_ptr<FrameHeader> frame_;
  std::shared_ptr<ArcReplacer> replacer_;
  std::shared_ptr<std::mutex> bpm_mutex_;
  std::shared_ptr<DiskScheduler> disk_scheduler_;
  std::shared_lock<std::shared_mutex> read_lock_;
  bool is_valid_{false};
};

class WritePageGuard {
  friend class BufferPoolManager;

public:
  WritePageGuard() = default;
  WritePageGuard(const WritePageGuard&) = delete;
  WritePageGuard& operator=(const WritePageGuard&) = delete;
  WritePageGuard(WritePageGuard&&) noexcept;
  WritePageGuard& operator=(WritePageGuard&&) noexcept;

  ~WritePageGuard();

  PageId_t GetPageId() const;
  const char* GetData() const;
  template <class T>
  const T* As() const {
    return reinterpret_cast<const T*>(GetData());
  }
  char* GetDataMut();
  template <class T>
  T* AsMut() {
    return reinterpret_cast<T *>(GetDataMut());
  }

  bool IsDirty() const;
  void Flush();
  void Drop();

private:
  WritePageGuard(PageId_t, std::shared_ptr<FrameHeader>, std::shared_ptr<ArcReplacer>, std::shared_ptr<std::mutex>, std::shared_ptr<DiskScheduler>);

  PageId_t page_id_;
  std::shared_ptr<FrameHeader> frame_;
  std::shared_ptr<ArcReplacer> replacer_;
  std::shared_ptr<std::mutex> bpm_mutex_;
  std::shared_ptr<DiskScheduler> disk_scheduler_;
  std::unique_lock<std::shared_mutex> rw_lock_;
  bool is_valid_{false};
};

#endif
