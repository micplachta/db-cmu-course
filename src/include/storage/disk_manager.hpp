#ifndef _DISK_MANAGER_HPP_
#define _DISK_MANAGER_HPP_

#include <config.hpp>
#include <filesystem>
#include <fstream>
#include <future>
#include <mutex>
#include <unordered_map>
#include <vector>

class DiskManager {
 public:
  DiskManager(const std::filesystem::path&);
  DiskManager() = default;

  void ShutDown();

  virtual void WritePage(PageId_t, const char*);
  virtual void ReadPage(PageId_t, char*);
  virtual void DeletePage(PageId_t);

  void WriteLog(char*, int);
  void ReadLog(char*, int, int);
  int GetNumFlushes() const;
  bool GetFlushState() const;
  int GetNumWrites() const;
  int GetNumDeletes() const;

  void SetFlushLogFuture(std::future<void>*);
  bool HasFlushLogFuture();
  std::filesystem::path GetLogFileName() const;

  size_t GetDbFileSize();

 protected:
  int num_flushes_{0};
  int num_writes_{0};
  int num_deletes_{0};

  size_t page_capacity_{DEFAULT_DB_IO_SIZE};

 private:
  int GetFileSize(const std::string&);
  size_t AllocatePage();

  std::fstream log_io_;
  std::filesystem::path log_file_name_;

  std::fstream db_io_;
  std::filesystem::path db_file_name_;

  std::unordered_map<PageId_t, size_t> pages_;
  std::vector<size_t> free_slots_;

  bool flush_log_{false};
  std::future<void>* flush_log_f_{nullptr};
  std::mutex db_io_mutex_;
};

#endif
