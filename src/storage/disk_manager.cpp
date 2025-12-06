#include <storage/disk_manager.hpp>

#include <thread>
#include <sys/stat.h>
#include <stdexcept>
#include <cassert>
#include <cstring>

DiskManager::DiskManager(const std::filesystem::path& p) : db_file_name_(p) {
  log_file_name_ = p.filename().stem().string() + ".log";
  log_io_.open(log_file_name_, std::ios::binary | std::ios::in | std::ios::app | std::ios::out);

  if (!log_io_.is_open()) {
    log_io_.clear();
    log_io_.open(log_file_name_, std::ios::binary | std::ios::in | std::ios::trunc | std::ios::out);

    if (!log_io_.is_open()) {
      throw std::runtime_error("Can't open db log file");
    }
  }

  db_io_.open(p, std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
  if (!db_io_.is_open()) {
    db_io_.clear();
    db_io_.open(p, std::ios::binary | std::ios::in | std::ios::trunc | std::ios::out);

    if (!db_io_.is_open()) {
      throw std::runtime_error("Can't open db log file");
    }
  }

  std::filesystem::resize_file(p, (page_capacity_ + 1) * DB_PAGE_SIZE);
  if (static_cast<size_t>(GetFileSize(db_file_name_)) < page_capacity_ * DB_PAGE_SIZE) {
    throw std::runtime_error("File size lower than expected");
  }
}

void DiskManager::ShutDown() {
  {
    std::unique_lock<std::mutex> l(db_io_mutex_);
    db_io_.close();
  }
  log_io_.close();
}

void DiskManager::WritePage(PageId_t page_id, const char* data) {
  std::unique_lock<std::mutex> l(db_io_mutex_);
  size_t offset;

  if (pages_.find(page_id) != pages_.end()) {
    offset = pages_[page_id];
  } else {
    offset = AllocatePage();
  }

  db_io_.seekp(offset);
  db_io_.write(data, DB_PAGE_SIZE);

  if (db_io_.bad()) {
    throw std::runtime_error("Error writing data to file");
  }

  num_writes_ += 1;
  pages_[page_id] = offset;
  db_io_.flush();
}

void DiskManager::ReadPage(PageId_t page_id, char* buffer) {
  std::unique_lock<std::mutex> l(db_io_mutex_);
  size_t offset;
  if (pages_.find(page_id) != pages_.end()) {
    offset = pages_[page_id];
  } else {
    offset = AllocatePage();
  }

  int file_size = GetFileSize(db_file_name_);
  if (file_size < 0) {
    throw std::runtime_error("Error while getting file size");
  }

  if (offset + DB_PAGE_SIZE > static_cast<size_t>(file_size)) {
    throw std::runtime_error("Offset outside file size");
  }

  pages_[page_id] = offset;
  db_io_.seekp(offset);
  db_io_.read(buffer, DB_PAGE_SIZE);

  if (db_io_.bad()) {
    throw std::runtime_error("Error reading data from file");
  }

  int read_count = db_io_.gcount();
  if (read_count < DB_PAGE_SIZE) {
    db_io_.clear();
    throw std::runtime_error("Error reading data from file");
  }
}

void DiskManager::DeletePage(PageId_t page_id) {
  std::unique_lock<std::mutex> l(db_io_mutex_);
  if (pages_.find(page_id) == pages_.end()) {
    return;
  }

  size_t offset = pages_[page_id];
  free_slots_.push_back(offset);
  pages_.erase(page_id);
  num_deletes_ += 1;
}

void DiskManager::WriteLog(char* data, int size) {
  if (size == 0) {
    return;
  }

  flush_log_ = true;

  if (flush_log_f_ != nullptr) {
    assert(flush_log_f_->wait_for(std::chrono::seconds(10)) == std::future_status::ready);
  }

  num_flushes_ += 1;
  log_io_.write(data, size);

  if (log_io_.bad()) {
    throw std::runtime_error("Error writing log");
  }
  log_io_.flush();
  flush_log_ = false;
}

void DiskManager::ReadLog(char* buffer, int size , int offset) {
  if (offset + size > GetFileSize(log_file_name_)) {
    throw std::runtime_error("Error, tried to read log outside of file");
  }
  log_io_.seekp(offset);
  log_io_.read(buffer, size);

  if (log_io_.bad()) {
    throw std::runtime_error("Error reading log");
  }

  int read_count = log_io_.gcount();
  if (read_count < size) {
    log_io_.clear();
    memset(buffer + read_count, 0, size - read_count);
  }
}

int DiskManager::GetNumFlushes() const {
  return num_flushes_;
}

bool DiskManager::GetFlushState() const {
  return flush_log_;
}

int DiskManager::GetNumWrites() const {
  return num_writes_;
}

int DiskManager::GetNumDeletes() const {
  return num_deletes_;
}

void DiskManager::SetFlushLogFuture(std::future<void> *f) {
  flush_log_f_ = f;
}

bool DiskManager::HasFlushLogFuture() {
  return flush_log_f_ != nullptr;
}

std::filesystem::path DiskManager::GetLogFileName() const {
  return log_file_name_;
}

size_t DiskManager::GetDbFileSize() {
  int file_size = GetFileSize(db_file_name_);
  if (file_size < 0) {
    return -1;
  }

  return static_cast<size_t>(file_size);
}

int DiskManager::GetFileSize(const std::string& name) {
  struct stat stat_buf;
  int rc = stat(db_file_name_.c_str(), &stat_buf);
  return rc == 0 ? static_cast<int>(stat_buf.st_size) : -1;
}

size_t DiskManager::AllocatePage() {
  if (!free_slots_.empty()) {
    auto offset = free_slots_.back();
    free_slots_.pop_back();
    return offset;
  }

  if (pages_.size() + 1 >= page_capacity_) {
    page_capacity_ *= 2;
    std::filesystem::resize_file(db_file_name_, (page_capacity_ + 1) * DB_PAGE_SIZE);
  }

  return pages_.size() * DB_PAGE_SIZE;
}
