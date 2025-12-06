#ifndef _CHANNEL_HPP_
#define _CHANNEL_HPP_

#include <condition_variable>
#include <mutex>
#include <queue>

template <typename T>
class Channel {
 public:
  Channel() = default;

  void Put(T element) {
    std::unique_lock<std::mutex> l(mutex_);
    q_.push(std::move(element));
    l.unlock();
    cv_.notify_all();
  }

  T Get() {
    std::unique_lock<std::mutex> l(mutex_);
    cv_.wait(l, [&]() { return !q_.empty(); });
    T e(std::move(q_.front()));
    q_.pop();
    return e;
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::queue<T> q_;
};

#endif
