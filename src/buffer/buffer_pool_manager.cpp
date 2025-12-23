#include <buffer/buffer_pool_manager.hpp>

#include <iostream>

FrameHeader::FrameHeader(FrameId_t frame_id)
    : frame_id_(frame_id), data_(DB_PAGE_SIZE, 0) {
  Reset();
}

const char* FrameHeader::GetData() const {
  return data_.data();
}

char* FrameHeader::GetDataMut() {
  return data_.data();
}

void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

BufferPoolManager::BufferPoolManager(size_t num_frames,
                                     DiskManager* disk_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      mutex_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<ArcReplacer>(num_frames)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)) {
  std::unique_lock<std::mutex> l(*mutex_);
  next_page_id_.store(0);
  frames_.reserve(num_frames_);
  page_table_.reserve(num_frames_);
  rev_page_table_.reserve(num_frames_);
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() = default;

size_t BufferPoolManager::Size() const {
  return num_frames_;
}

PageId_t BufferPoolManager::NewPage() {
  return next_page_id_.fetch_add(1);
}

bool BufferPoolManager::DeletePage(PageId_t page_id) {
  std::unique_lock<std::mutex> l(*mutex_);

  auto page_it = page_table_.find(page_id);
  if (page_it != page_table_.end()) {
    // Page is in memory
    FrameId_t frame_id = page_it->second;
    auto frame = frames_[frame_id];
    if (frame->pin_count_.load() > 0)
      return false;

    page_table_.erase(page_id);
    rev_page_table_.erase(frame_id);
    replacer_->Remove(frame_id);

    if (frame->is_dirty_) {
      DiskRequest req{.is_write = true,
                      .data = frame->data_.data(),
                      .page_id = page_id,
                      .cb = disk_scheduler_->CreatePromise()};
      auto fut = req.cb.get_future();
      std::vector<DiskRequest> v;
      v.push_back(std::move(req));
      disk_scheduler_->Schedule(v);
      fut.get();
      frame->is_dirty_ = false;
    }

    frame->Reset();
    free_frames_.push_back(frame_id);
  }
  
  disk_scheduler_->DeallocatePage(page_id);
  
  return true;
}

std::optional<WritePageGuard> BufferPoolManager::CheckedWritePage(
    PageId_t page_id, AccessType access_type) {

  std::unique_lock<std::mutex> l(*mutex_);

  auto page_it = page_table_.find(page_id);
  if (page_it != page_table_.end()) {
    // Page is in memory
    FrameId_t frame_id = page_it->second;
    auto frame = frames_[frame_id];
    frame->pin_count_.fetch_add(1);

    replacer_->RecordAccess(frame_id, page_id, access_type);
    replacer_->SetEvictable(frame_id, false);

    l.unlock();
    WritePageGuard guard(page_id, frame, replacer_, mutex_, disk_scheduler_);
    return std::optional<WritePageGuard>(std::move(guard));
  }

  // We need to load the page into memory
  FrameId_t frame_id = -1;
  if (free_frames_.empty()) {
    // No free frames, try to evict
    auto frame_id_opt = replacer_->Evict();
    if (!frame_id_opt.has_value()) {
      return std::nullopt;
    }
    frame_id = frame_id_opt.value();
    auto frame = frames_[frame_id];

    PageId_t evicted_page = rev_page_table_[frame_id];
    page_table_.erase(evicted_page);
    rev_page_table_.erase(frame_id);

    if (frame->is_dirty_) {
      DiskRequest req{.is_write = true,
                      .data = frame->data_.data(),
                      .page_id = evicted_page,
                      .cb = disk_scheduler_->CreatePromise()};
      auto fut = req.cb.get_future();
      std::vector<DiskRequest> v;
      v.push_back(std::move(req));
      disk_scheduler_->Schedule(v);
      fut.get();
      frame->is_dirty_ = false;
    }
  } else {
    frame_id = free_frames_.front();
    free_frames_.pop_front();
  }

  auto frame = frames_[frame_id];
  DiskRequest req{.is_write = false,
                  .data = frame->data_.data(),
                  .page_id = page_id,
                  .cb = disk_scheduler_->CreatePromise()};
  auto fut = req.cb.get_future();
  std::vector<DiskRequest> v;
  v.push_back(std::move(req));
  disk_scheduler_->Schedule(v);
  fut.get();

  frame->pin_count_.fetch_add(1);

  page_table_[page_id] = frame_id;
  rev_page_table_[frame_id] = page_id;
  replacer_->RecordAccess(frame_id, page_id, access_type);
  replacer_->SetEvictable(frame_id, false);

  WritePageGuard guard(page_id, frame, replacer_, mutex_, disk_scheduler_);

  return std::optional<WritePageGuard>(std::move(guard));
}

std::optional<ReadPageGuard> BufferPoolManager::CheckedReadPage(
    PageId_t page_id, AccessType access_type) {
  std::unique_lock<std::mutex> l(*mutex_);

  auto page_it = page_table_.find(page_id);
  if (page_it != page_table_.end()) {
    // Page is in memory
    FrameId_t frame_id = page_it->second;
    auto frame = frames_[frame_id];
    frame->pin_count_.fetch_add(1);

    replacer_->RecordAccess(frame_id, page_id, access_type);
    replacer_->SetEvictable(frame_id, false);

    l.unlock();
    ReadPageGuard guard(page_id, frame, replacer_, mutex_, disk_scheduler_);
    return std::optional<ReadPageGuard>(std::move(guard));
  }

  // We need to load the page into memory
  FrameId_t frame_id = -1;
  if (free_frames_.empty()) {
    // No free frames, try to evict
    auto frame_id_opt = replacer_->Evict();
    if (!frame_id_opt.has_value()) {
      return std::nullopt;
    }
    frame_id = frame_id_opt.value();
    auto frame = frames_[frame_id];

    PageId_t evicted_page = rev_page_table_[frame_id];
    page_table_.erase(evicted_page);
    rev_page_table_.erase(frame_id);

    if (frame->is_dirty_) {
      DiskRequest req{.is_write = true,
                      .data = frame->data_.data(),
                      .page_id = evicted_page,
                      .cb = disk_scheduler_->CreatePromise()};
      auto fut = req.cb.get_future();
      std::vector<DiskRequest> v;
      v.push_back(std::move(req));
      disk_scheduler_->Schedule(v);
      fut.get();
      frame->is_dirty_ = false;
    }
  } else {
    frame_id = free_frames_.front();
    free_frames_.pop_front();
  }

  auto frame = frames_[frame_id];
  DiskRequest req{.is_write = false,
                  .data = frame->data_.data(),
                  .page_id = page_id,
                  .cb = disk_scheduler_->CreatePromise()};
  auto fut = req.cb.get_future();
  std::vector<DiskRequest> v;
  v.push_back(std::move(req));
  disk_scheduler_->Schedule(v);
  fut.get();

  frame->pin_count_.fetch_add(1);

  page_table_[page_id] = frame_id;
  rev_page_table_[frame_id] = page_id;
  replacer_->RecordAccess(frame_id, page_id, access_type);
  replacer_->SetEvictable(frame_id, false);

  ReadPageGuard guard(page_id, frame, replacer_, mutex_, disk_scheduler_);
  return std::optional<ReadPageGuard>(std::move(guard));
}

WritePageGuard BufferPoolManager::WritePage(PageId_t page_id,
                                            AccessType access_type) {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    throw std::runtime_error("CheckedWritePage failed to bring in page");
  }

  return std::move(guard_opt).value();
}

ReadPageGuard BufferPoolManager::ReadPage(PageId_t page_id,
                                          AccessType access_type) {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    throw std::runtime_error("CheckedReadPage failed to bring in page");
  }

  return std::move(guard_opt).value();
}

bool BufferPoolManager::FlushPageUnsafe(PageId_t page_id) {
  std::unique_lock<std::mutex> l(*mutex_);
  auto page_it = page_table_.find(page_id);
  if (page_it == page_table_.end())
    return false;

  FrameId_t frame_id = page_it->second;
  auto frame = frames_[frame_id];

  if (frame->is_dirty_) {
    frame->pin_count_.fetch_add(1);
    l.unlock();
    DiskRequest req{.is_write = true,
                    .data = frame->data_.data(),
                    .page_id = page_id,
                    .cb = disk_scheduler_->CreatePromise()};
    auto fut = req.cb.get_future();
    std::vector<DiskRequest> v;
    v.push_back(std::move(req));
    disk_scheduler_->Schedule(v);
    fut.get();
    frame->is_dirty_ = false;
    frame->pin_count_.fetch_sub(1);
  }

  return true;
}

bool BufferPoolManager::FlushPage(PageId_t page_id) {
  std::unique_lock<std::mutex> l(*mutex_);
  auto page_it = page_table_.find(page_id);
  if (page_it == page_table_.end())
    return false;

  FrameId_t frame_id = page_it->second;
  auto frame = frames_[frame_id];

  if (frame->is_dirty_) {
    frame->pin_count_.fetch_add(1);
    l.unlock();
    std::shared_lock<std::shared_mutex>(frame->rw_mutex_);
    DiskRequest req{.is_write = true,
                    .data = frame->data_.data(),
                    .page_id = page_id,
                    .cb = disk_scheduler_->CreatePromise()};
    auto fut = req.cb.get_future();
    std::vector<DiskRequest> v;
    v.push_back(std::move(req));
    disk_scheduler_->Schedule(v);
    fut.get();
    frame->is_dirty_ = false;
    frame->pin_count_.fetch_sub(1);
  }

  return true;
}

void BufferPoolManager::FlushAllPagesUnsafe() {
  std::unique_lock<std::mutex> l(*mutex_);
  std::vector<std::pair<PageId_t, std::shared_ptr<FrameHeader>>> headers;
  for (const auto& kv : page_table_) {
    PageId_t page_id = kv.first;
    FrameId_t frame_id = kv.second;
    auto frame = frames_[frame_id];

    if (frame->is_dirty_) {
      frame->pin_count_.fetch_add(1);
      headers.push_back({page_id, frame});
    }
  }
  l.unlock();

  for (auto& page_frame_pair : headers) {
    PageId_t page_id = page_frame_pair.first;
    auto frame = page_frame_pair.second;
    DiskRequest req{.is_write = true,
                    .data = frame->data_.data(),
                    .page_id = page_id,
                    .cb = disk_scheduler_->CreatePromise()};
    auto fut = req.cb.get_future();
    std::vector<DiskRequest> v;
    v.push_back(std::move(req));
    disk_scheduler_->Schedule(v);
    fut.get();
    frame->is_dirty_ = false;
    frame->pin_count_.fetch_sub(1);
  }
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::mutex> l(*mutex_);
  std::vector<std::pair<PageId_t, std::shared_ptr<FrameHeader>>> headers;
  for (const auto& kv : page_table_) {
    PageId_t page_id = kv.first;
    FrameId_t frame_id = kv.second;
    auto frame = frames_[frame_id];

    if (frame->is_dirty_) {
      frame->pin_count_.fetch_add(1);
      headers.push_back({page_id, frame});
    }
  }
  l.unlock();

  for (auto& page_frame_pair : headers) {
    PageId_t page_id = page_frame_pair.first;
    auto frame = page_frame_pair.second;
    std::shared_lock<std::shared_mutex>(frame->rw_mutex_);
    DiskRequest req{.is_write = true,
                    .data = frame->data_.data(),
                    .page_id = page_id,
                    .cb = disk_scheduler_->CreatePromise()};
    auto fut = req.cb.get_future();
    std::vector<DiskRequest> v;
    v.push_back(std::move(req));
    disk_scheduler_->Schedule(v);
    fut.get();
    frame->is_dirty_ = false;
    frame->pin_count_.fetch_sub(1);
  }
}

std::optional<size_t> BufferPoolManager::GetPinCount(PageId_t page_id) {
  std::unique_lock<std::mutex> l(*mutex_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }
  FrameId_t frame_id = it->second;
  return std::optional<size_t>(frames_[frame_id]->pin_count_.load());
}
