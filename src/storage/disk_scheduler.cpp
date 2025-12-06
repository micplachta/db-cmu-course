#include <storage/disk_scheduler.hpp>

DiskScheduler::DiskScheduler(DiskManager* m) : disk_manager_(m) {
  worker_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  end_thread_ = true;
  request_q_.Put(std::nullopt);
  if (worker_thread_.has_value()) {
    worker_thread_->join();
  }
}

void DiskScheduler::Schedule(std::vector<DiskRequest>& requests) {
  for (auto& r : requests) {
    request_q_.Put(std::move(r));
  }
}

void DiskScheduler::StartWorkerThread() {
  while (true) {
    auto r(request_q_.Get());
    if (!r.has_value() && !end_thread_) {
      break;
    }

    if (r->is_write) {
      disk_manager_->WritePage(r->page_id, r->data);
    } else {
      disk_manager_->ReadPage(r->page_id, r->data);
    }
    r->cb.set_value(true);
  }
}

DiskSchedulerPromise DiskScheduler::CreatePromise() {
  return {};
}

void DiskScheduler::DeallocatePage(PageId_t page_id) {
  disk_manager_->DeletePage(page_id);
}
