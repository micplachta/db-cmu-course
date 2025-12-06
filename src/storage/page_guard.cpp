#include <buffer/buffer_pool_manager.hpp>
#include <storage/page_guard.hpp>

ReadPageGuard::ReadPageGuard(ReadPageGuard&& other) noexcept
    : page_id_(other.page_id_),
      frame_(std::move(other.frame_)),
      replacer_(std::move(other.replacer_)),
      bpm_mutex_(std::move(other.bpm_mutex_)),
      disk_scheduler_(std::move(other.disk_scheduler_)),
      read_lock_(std::move(other.read_lock_)),
      is_valid_(other.is_valid_) {
  other.is_valid_ = false;
}

ReadPageGuard& ReadPageGuard::operator=(ReadPageGuard&& other) noexcept {
  page_id_ = other.page_id_;
  frame_ = std::move(other.frame_);
  replacer_ = std::move(other.replacer_);
  bpm_mutex_ = std::move(other.bpm_mutex_);
  disk_scheduler_ = std::move(other.disk_scheduler_);
  read_lock_ = std::move(other.read_lock_);
  is_valid_ = other.is_valid_;
  other.is_valid_ = false;
  return *this;
}

ReadPageGuard::~ReadPageGuard() {
  Drop();
}

PageId_t ReadPageGuard::GetPageId() const {
  if (!is_valid_)
    throw std::runtime_error("Error, tried to use an invalid read guard");
  return page_id_;
}

const char* ReadPageGuard::GetData() const {
  if (!is_valid_)
    throw std::runtime_error("Error, tried to use an invalid read guard");
  return frame_->GetData();
}

bool ReadPageGuard::IsDirty() const {
  if (!is_valid_)
    throw std::runtime_error("Error, tried to use an invalid read guard");
  return frame_->is_dirty_;
}

void ReadPageGuard::Flush() {
  if (!is_valid_) {
    throw std::runtime_error("Error, tried to flush using invalid read guard");
  }

  std::unique_lock<std::mutex> l(*bpm_mutex_);
  if (frame_->is_dirty_) {
    // disk_scheduler_->WritePage(page_id_, frame_->GetData());
    DiskRequest req{.is_write = true,
                    .data = frame_->GetDataMut(),
                    .page_id = page_id_,
                    .cb = disk_scheduler_->CreatePromise()};
    auto fut = req.cb.get_future();
    std::vector<DiskRequest> v;
    v.push_back(std::move(req));
    disk_scheduler_->Schedule(v);
    fut.get();
    frame_->is_dirty_ = false;
  }
}

void ReadPageGuard::Drop() {
  if (!is_valid_)
    return;

  std::unique_lock<std::mutex> l(*bpm_mutex_);
  size_t old_pin = frame_->pin_count_.fetch_sub(1);
  if (old_pin <= 0) {
    throw std::runtime_error("Pin count underflow in ReadPageGuard::Drop()");
  }

  if (frame_->pin_count_.load() == 0) {
    replacer_->SetEvictable(frame_->frame_id_, true);
  }

  is_valid_ = false;
}

ReadPageGuard::ReadPageGuard(PageId_t page_id,
                             std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<ArcReplacer> replacer,
                             std::shared_ptr<std::mutex> mutex,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(frame),
      replacer_(replacer),
      bpm_mutex_(mutex),
      disk_scheduler_(disk_scheduler),
      read_lock_(frame_->rw_mutex_) {}

WritePageGuard::WritePageGuard(WritePageGuard&& other) noexcept
    : page_id_(other.page_id_),
      frame_(std::move(other.frame_)),
      replacer_(std::move(other.replacer_)),
      bpm_mutex_(std::move(other.bpm_mutex_)),
      disk_scheduler_(std::move(other.disk_scheduler_)),
      rw_lock_(std::move(other.rw_lock_)),
      is_valid_(other.is_valid_) {
  other.is_valid_ = false;
}

WritePageGuard& WritePageGuard::operator=(WritePageGuard&& other) noexcept {
  page_id_ = other.page_id_;
  frame_ = std::move(other.frame_);
  replacer_ = std::move(other.replacer_);
  bpm_mutex_ = std::move(other.bpm_mutex_);
  disk_scheduler_ = std::move(other.disk_scheduler_);
  rw_lock_ = std::move(other.rw_lock_);
  is_valid_ = other.is_valid_;
  other.is_valid_ = false;
  return *this;
}

WritePageGuard::~WritePageGuard() {
  Drop();
}

PageId_t WritePageGuard::GetPageId() const {
  if (!is_valid_)
    throw std::runtime_error("Error, tried to use an invalid write guard");
  return page_id_;
}

const char* WritePageGuard::GetData() const {
  if (!is_valid_)
    throw std::runtime_error("Error, tried to use an invalid write guard");
  return frame_->GetData();
}

char* WritePageGuard::GetDataMut() {
  if (!is_valid_) {
    throw std::runtime_error("Error, tried to use an invalid write guard");
  }
  frame_->is_dirty_ = true;
  return frame_->GetDataMut();
}

bool WritePageGuard::IsDirty() const {
  if (!is_valid_)
    throw std::runtime_error("Error, tried to use an invalid write guard");
  return frame_->is_dirty_;
}

void WritePageGuard::Flush() {
  if (!is_valid_) {
    throw std::runtime_error("Error, tried to flush using invalid write guard");
  }

  std::unique_lock<std::mutex> l(*bpm_mutex_);
  if (frame_->is_dirty_) {
    // disk_scheduler_->WritePage(page_id_, frame_->GetData());

    DiskRequest req{.is_write = true,
                    .data = frame_->GetDataMut(),
                    .page_id = page_id_,
                    .cb = disk_scheduler_->CreatePromise()};
    auto fut = req.cb.get_future();
    std::vector<DiskRequest> v;
    v.push_back(std::move(req));
    disk_scheduler_->Schedule(v);
    fut.get();
    frame_->is_dirty_ = false;
  }
}

void WritePageGuard::Drop() {
  if (!is_valid_)
    return;

  std::unique_lock<std::mutex> lk(*bpm_mutex_);

  size_t old_pin = frame_->pin_count_.fetch_sub(1);
  if (old_pin <= 0) {
    throw std::runtime_error("Pin count underflow in WritePageGuard::Drop()");
  }

  if (frame_->pin_count_.load() == 0) {
    replacer_->SetEvictable(frame_->frame_id_, true);
  }

  is_valid_ = false;
}

WritePageGuard::WritePageGuard(PageId_t page_id,
                               std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<ArcReplacer> replacer,
                               std::shared_ptr<std::mutex> mutex,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(frame),
      replacer_(replacer),
      bpm_mutex_(mutex),
      disk_scheduler_(disk_scheduler),
      rw_lock_(frame->rw_mutex_) {}
