#include <buffer/arc_replacer.hpp>

#include <stdexcept>

FrameStatus::FrameStatus(PageId_t page_id, FrameId_t frame_id, bool ev,
                         ArcStatus status)
    : page_id(page_id), frame_id(frame_id), evictable(ev), arc_status(status) {}

ArcReplacer::ArcReplacer(size_t size) : replacer_size_(size) {}

std::optional<FrameId_t> ArcReplacer::EvictOneList_(bool is_mru) {
  std::list<FrameId_t>::reverse_iterator it, end;
  if (is_mru) {
    it = mru_.rbegin();
    end = mru_.rend();
  } else {
    it = mfu_.rbegin();
    end = mfu_.rend();
  }

  for (; it != end; it++) {
    FrameId_t frame_id = *it;
    auto status_it = alive_map_.find(frame_id);
    if (status_it == alive_map_.end())
      throw std::runtime_error("frame_id without frame status");
    FrameStatus status = status_it->second;
    if (!status.evictable)
      continue;

    if (is_mru)
      status.arc_status = ArcStatus::MRU_GHOST;
    else
      status.arc_status = ArcStatus::MFU_GHOST;

    curr_size_--;
    ghost_map_.insert({status.page_id, status});
    alive_map_.erase(frame_id);

    auto erase_it = std::prev(it.base());

    if (is_mru) {
      mru_.erase(erase_it);
      mru_map_.erase(frame_id);
      mru_ghost_.push_front(status.page_id);
      mru_ghost_map_[status.page_id] = mru_ghost_.begin();
      return frame_id;
    } else {
      mfu_.erase(erase_it);
      mfu_map_.erase(frame_id);
      mfu_ghost_.push_front(status.page_id);
      mfu_ghost_map_[status.page_id] = mfu_ghost_.begin();
      return frame_id;
    }
  }

  return std::nullopt;
}

std::optional<FrameId_t> ArcReplacer::Evict() {
  std::lock_guard<std::mutex> l(mutex_);
  std::optional<FrameId_t> ret;

  if (mru_.size() >= mru_target_size_) {
    if ((ret = EvictOneList_(true)) != std::nullopt)
      return ret;
    return EvictOneList_(false);
  } else {
    if ((ret = EvictOneList_(false)) != std::nullopt)
      return ret;
    return EvictOneList_(true);
  }
}

bool ArcReplacer::RecordAccessExists_(FrameId_t frame_id,
                                      AccessType access_type) {
  auto status_it = alive_map_.find(frame_id);

  if (status_it == alive_map_.end())
    return false;

  FrameStatus& status = status_it->second;

  if (status.arc_status == ArcStatus::MRU) {
    auto position_it = mru_map_.find(frame_id);
    if (position_it == mru_map_.end())
      throw std::runtime_error("ARC Replacer inconsistent state");
    mru_.erase(position_it->second);
    mru_map_.erase(frame_id);
    mfu_.push_front(frame_id);
    mfu_map_[frame_id] = mfu_.begin();
    status.arc_status = ArcStatus::MFU;
    return true;
  }

  if (status.arc_status == ArcStatus::MFU) {
    auto position_it = mfu_map_.find(frame_id);
    if (position_it == mfu_map_.end())
      throw std::runtime_error("ARC Replacer inconsistent state");
    mfu_.erase(position_it->second);
    mfu_map_.erase(frame_id);
    mfu_.push_front(frame_id);
    mfu_map_[frame_id] = mfu_.begin();
    return true;
  }

  throw std::runtime_error("ARC Replacer inconsistent state");
}

bool ArcReplacer::RecordAccessGhostHit_(FrameId_t frame_id, PageId_t page_id,
                                        AccessType access_type) {
  auto status_it = ghost_map_.find(page_id);
  if (status_it == ghost_map_.end())
    return false;

  FrameStatus status = status_it->second;

  if (status.arc_status == ArcStatus::MRU_GHOST) {
    auto position_it = mru_ghost_map_.find(page_id);
    if (position_it == mru_ghost_map_.end())
      throw std::runtime_error("ARC Replacer inconsistent state");
    
    auto& it = position_it->second;

    if (mru_ghost_.size() >= mfu_ghost_.size()) {
      mru_target_size_++;
    } else {
      mru_target_size_ += mfu_ghost_.size() / mru_ghost_.size();
    }
    mru_target_size_ = std::min(mru_target_size_, replacer_size_);

    ghost_map_.erase(page_id);
    mfu_.push_front(frame_id);
    mfu_map_[frame_id] = mfu_.begin();

    status.frame_id = frame_id;
    status.page_id = page_id;
    status.arc_status = ArcStatus::MFU;
    status.evictable = true;

    alive_map_[frame_id] = status;
    curr_size_++;
    mru_ghost_.erase(it);
    mru_ghost_map_.erase(page_id);
    return true;
  }

  if (status.arc_status == ArcStatus::MFU_GHOST) {
    auto position_it = mfu_ghost_map_.find(page_id);
    if (position_it == mfu_ghost_map_.end())
      throw std::runtime_error("ARC Replacer inconsistent state");
    
    auto& it = position_it->second;
    if (mfu_ghost_.size() >= mru_ghost_.size()) {
      mru_target_size_--;
    } else {
      mru_target_size_ -= mru_ghost_.size() / mfu_ghost_.size();
    }
    mru_target_size_ = std::max(mru_target_size_, static_cast<size_t>(0));

    ghost_map_.erase(page_id);
    mfu_.push_front(frame_id);
    mfu_map_[frame_id] = mfu_.begin();

    status.frame_id = frame_id;
    status.page_id = page_id;
    status.evictable = true;
    status.arc_status = ArcStatus::MFU;

    alive_map_[frame_id] = status;
    curr_size_++;
    mfu_ghost_.erase(it);
    mfu_ghost_map_.erase(page_id);
    return true;
  }

  throw std::runtime_error("ARC Replacer inconsistent state");
}

void ArcReplacer::RecordAccessNoHit_(FrameId_t frame_id, PageId_t page_id,
                                     AccessType access_type) {
  size_t mru_size = mru_.size() + mru_ghost_.size();
  size_t all_size = mru_size + mfu_.size() + mfu_ghost_.size();
  if (mru_size == replacer_size_) {
    PageId_t deleted = mru_ghost_.back();
    mru_ghost_.pop_back();
    mru_ghost_map_.erase(deleted);
    ghost_map_.erase(deleted);
  } else if (all_size == 2 * replacer_size_) {
    PageId_t deleted = mfu_ghost_.back();
    mfu_ghost_.pop_back();
    mfu_ghost_map_.erase(deleted);
    ghost_map_.erase(deleted);
  }

  FrameStatus status(page_id, frame_id, false, ArcStatus::MRU);
  mru_.push_front(frame_id);
  mru_map_[frame_id] = mru_.begin();
  alive_map_[frame_id] = status;
}

void ArcReplacer::RecordAccess(FrameId_t frame_id, PageId_t page_id,
                               AccessType access_type) {
  std::lock_guard<std::mutex> l(mutex_);
  if (RecordAccessExists_(frame_id, access_type)) {
    return;
  } else if (RecordAccessGhostHit_(frame_id, page_id, access_type)) {
    return;
  } else {
    RecordAccessNoHit_(frame_id, page_id, access_type);
  }
}

void ArcReplacer::SetEvictable(FrameId_t frame_id, bool value) {
  std::lock_guard<std::mutex> l(mutex_);

  auto status_it = alive_map_.find(frame_id);
  if (status_it == alive_map_.end())
    throw std::runtime_error("frame_id without frame status");

  FrameStatus& status = status_it->second;

  if (!status.evictable && value)
    curr_size_++;
  if (status.evictable && !value)
    curr_size_--;

  status.evictable = value;
}

void ArcReplacer::Remove(FrameId_t frame_id) {
  std::lock_guard<std::mutex> l(mutex_);
  auto status_it = alive_map_.find(frame_id);

  if (status_it == alive_map_.end())
    return;

  FrameStatus status = status_it->second;

  if (!status.evictable)
    return;

  alive_map_.erase(frame_id);

  if (status.arc_status == ArcStatus::MRU) {
    auto position_it = mru_map_.find(frame_id);
    if (position_it == mru_map_.end())
      throw std::runtime_error("ARC Replacer inconsistent state");

    auto& it = position_it->second;
    mru_.erase(it);
    mru_map_.erase(frame_id);
    curr_size_--;
    return;
  }

  if (status.arc_status == ArcStatus::MFU) {
    auto position_it = mfu_map_.find(frame_id);
    if (position_it == mfu_map_.end())
      throw std::runtime_error("ARC Replacer inconsistent state");

    auto& it = position_it->second;
    mfu_.erase(it);
    mfu_map_.erase(frame_id);
    curr_size_--;
    return;
  }

  throw std::runtime_error("ARC Replacer inconsistent state");
}

size_t ArcReplacer::Size() const noexcept {
  std::lock_guard<std::mutex> l(mutex_);
  return curr_size_;
}
