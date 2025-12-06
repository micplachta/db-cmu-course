#ifndef _DISK_SCHEDULER_HPP_
#define _DISK_SCHEDULER_HPP_

#include <config.hpp>
#include <storage/disk_manager.hpp>
#include <utility/channel.hpp>

#include <memory>
#include <future>
#include <optional>
#include <thread>
#include <vector>

using DiskSchedulerPromise = std::promise<bool>;

struct DiskRequest {
  bool is_write;
  char* data;
  PageId_t page_id;
  DiskSchedulerPromise cb;
};

class DiskScheduler {
public:
  DiskScheduler(DiskManager*);
  ~DiskScheduler();

  void Schedule(std::vector<DiskRequest>&);
  void StartWorkerThread();
  DiskSchedulerPromise CreatePromise();
  void DeallocatePage(PageId_t);
private:
  DiskManager* disk_manager_;
  Channel<std::optional<DiskRequest>> request_q_;
  std::optional<std::thread> worker_thread_;
  bool end_thread_{true};
};

#endif
