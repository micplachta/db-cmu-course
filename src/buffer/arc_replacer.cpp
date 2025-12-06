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
    if (alive_map_.count(frame_id) == 0) {
      throw std::runtime_error("frame_id without frame status");
    }
    FrameStatus status = alive_map_[frame_id];

    if (!status.evictable)
      continue;

    curr_size_--;
    ghost_map_.insert({status.page_id, status});
    alive_map_.erase(frame_id);

    auto erase_it = std::prev(it.base());

    if (is_mru) {
      mru_.erase(erase_it);
      mru_ghost_.push_front(frame_id);
      return frame_id;
    } else {
      mfu_.erase(erase_it);
      mfu_ghost_.push_front(frame_id);
      return frame_id;
    }
  }

  return std::nullopt;
}

std::optional<FrameId_t> ArcReplacer::Evict() {
  std::optional<FrameId_t> ret;

  if ((ret = EvictOneList_(true)) != std::nullopt)
    return ret;
  return EvictOneList_(false);
}

bool ArcReplacer::RecordAccessExists_(FrameId_t frame_id,
                                      AccessType access_type) {
  if (alive_map_.count(frame_id) == 0) {
    return false;
  }

  for (auto it = mru_.begin(); it != mru_.end(); it++) {
    if (*it != frame_id)
      continue;

    mru_.erase(it);
    mfu_.push_front(frame_id);
    alive_map_[frame_id].arc_status = ArcStatus::MFU;
    return true;
  }

  for (auto it = mfu_.begin(); it != mfu_.end(); it++) {
    if (*it != frame_id)
      continue;

    mfu_.erase(it);
    mfu_.push_front(frame_id);
    return true;
  }

  throw std::runtime_error("Didn't find frame_id entry in MRU");
}

bool ArcReplacer::RecordAccessGhostHit_(FrameId_t frame_id, PageId_t page_id,
                                        AccessType access_type) {
  if (ghost_map_.count(page_id) == 0)
    return false;

  FrameStatus status = ghost_map_[page_id];

  if (status.arc_status == ArcStatus::MRU_GHOST) {
    for (auto it = mru_ghost_.begin(); it != mru_ghost_.end(); it++) {
      if (*it != page_id)
        continue;

      if (mru_ghost_.size() >= mfu_ghost_.size()) {
        mru_target_size_++;
      } else {
        mru_target_size_ += mfu_ghost_.size() / mru_ghost_.size();
      }
      mru_target_size_ = std::min(mru_target_size_, replacer_size_);

      ghost_map_.erase(page_id);
      mfu_.push_front(frame_id);
      status.arc_status = ArcStatus::MFU;
      status.evictable = true;
      alive_map_.insert({frame_id, status});
      mru_ghost_.erase(it);
      return true;
    }
  }

  if (status.arc_status == ArcStatus::MFU_GHOST) {
    for (auto it = mfu_ghost_.begin(); it != mfu_ghost_.end(); it++) {
      if (*it != page_id)
        continue;

      if (mfu_ghost_.size() >= mru_ghost_.size()) {
        mru_target_size_--;
      } else {
        mru_target_size_ += mru_ghost_.size() / mfu_ghost_.size();
      }
      mru_target_size_ = std::max(mru_target_size_, static_cast<size_t>(0));

      ghost_map_.erase(page_id);
      mfu_.push_front(frame_id);
      status.arc_status = ArcStatus::MFU;
      status.evictable = true;
      alive_map_.insert({frame_id, status});
      mfu_ghost_.erase(it);
    }
  }

  throw std::runtime_error("ARC status inconsisent");
}

void ArcReplacer::RecordAccessNoHit_(FrameId_t frame_id, PageId_t page_id,
                                     AccessType access_type) {
  size_t mru_size = mru_.size() + mru_ghost_.size();
  size_t all_size = mru_size + mfu_.size() + mfu_ghost_.size();
  if (mru_size == replacer_size_) {
    PageId_t deleted = mru_ghost_.back();
    mru_ghost_.pop_back();
    ghost_map_.erase(deleted);

    FrameStatus status(page_id, frame_id, true, ArcStatus::MRU);
    mru_.push_front(frame_id);
    alive_map_.insert({frame_id, status});
  } else if (all_size == 2 * replacer_size_) {
    PageId_t deleted = mfu_ghost_.back();
    mfu_ghost_.pop_back();
    ghost_map_.erase(deleted);

    FrameStatus status(page_id, frame_id, true, ArcStatus::MRU);
    mru_.push_front(frame_id);
    alive_map_.insert({frame_id, status});
  } else {
    FrameStatus status(page_id, frame_id, true, ArcStatus::MRU);
    mru_.push_front(frame_id);
    alive_map_.insert({frame_id, status});
  }
}

void ArcReplacer::RecordAccess(FrameId_t frame_id, PageId_t page_id,
                               AccessType access_type) {
  if (RecordAccessExists_(frame_id, access_type)) {
    return;
  } else if (RecordAccessGhostHit_(frame_id, page_id, access_type)) {
    return;
  } else {
    RecordAccessNoHit_(frame_id, page_id, access_type);
  }
}

void ArcReplacer::SetEvictable(FrameId_t frame_id, bool value) {
  if (alive_map_.count(frame_id) == 0) {
    throw std::runtime_error("frame_id without frame status");
  }

  if (!alive_map_[frame_id].evictable && value)
    curr_size_++;
  if (alive_map_[frame_id].evictable && !value)
    curr_size_--;

  alive_map_[frame_id].evictable = value;
}

void ArcReplacer::Remove(FrameId_t frame_id) {
  if (alive_map_.count(frame_id) == 0)
    return;

  if (!alive_map_[frame_id].evictable)
    return;

  alive_map_.erase(frame_id);

  for (auto it = mru_.begin(); it != mru_.end(); it++) {
    if (*it == frame_id) {
      mru_.erase(it);
      curr_size_--;
      break;
    }
  }

  for (auto it = mfu_.begin(); it != mfu_.end(); it++) {
    if (*it == frame_id) {
      mfu_.erase(it);
      curr_size_--;
      break;
    }
  }
}

size_t ArcReplacer::Size() const noexcept {
  return curr_size_;
}
