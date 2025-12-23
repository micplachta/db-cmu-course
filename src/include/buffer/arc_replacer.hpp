#ifndef _ARC_REPLACER_HPP_
#define _ARC_REPLACER_HPP_

#include <config.hpp>

#include <list>
#include <mutex>
#include <optional>
#include <unordered_map>

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

enum class ArcStatus { MRU = 0, MFU, MRU_GHOST, MFU_GHOST };

struct FrameStatus {
  FrameStatus() = default;
  FrameStatus(PageId_t, FrameId_t, bool, ArcStatus);
  FrameStatus& operator=(const FrameStatus& other) = default;

  PageId_t page_id;
  FrameId_t frame_id;
  bool evictable;
  ArcStatus arc_status;
};

class ArcReplacer {
 public:
  ArcReplacer(size_t);
  ArcReplacer(ArcReplacer&) = delete;
  ArcReplacer(ArcReplacer&&) = delete;

  std::optional<FrameId_t> Evict();
  void RecordAccess(FrameId_t, PageId_t,
                    AccessType access_type = AccessType::Unknown);
  void SetEvictable(FrameId_t, bool);
  void Remove(FrameId_t);
  size_t Size() const noexcept;

 private:
  std::optional<FrameId_t> EvictOneList_(bool);
  bool RecordAccessExists_(FrameId_t, AccessType);
  bool RecordAccessGhostHit_(FrameId_t, PageId_t, AccessType);
  void RecordAccessNoHit_(FrameId_t, PageId_t, AccessType);

  std::list<FrameId_t> mru_;
  std::list<FrameId_t> mfu_;
  std::list<PageId_t> mru_ghost_;
  std::list<PageId_t> mfu_ghost_;

  std::unordered_map<FrameId_t, std::list<FrameId_t>::iterator> mru_map_;
  std::unordered_map<FrameId_t, std::list<FrameId_t>::iterator> mfu_map_;
  std::unordered_map<PageId_t, std::list<PageId_t>::iterator> mru_ghost_map_;
  std::unordered_map<PageId_t, std::list<PageId_t>::iterator> mfu_ghost_map_;

  std::unordered_map<FrameId_t, FrameStatus> alive_map_;
  std::unordered_map<PageId_t, FrameStatus> ghost_map_;

  size_t curr_size_ = 0;
  size_t mru_target_size_ = 0;
  size_t replacer_size_;

  mutable std::mutex mutex_;
};

#endif
