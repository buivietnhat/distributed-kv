#pragma once

#include <condition_variable>
#include <mutex>

#include "common/container/concurrent_blocking_queue.h"
#include "common/macros.h"

namespace kv::common {

template <typename T>
class Channel {
 public:
  Channel() = default;

  DISALLOW_COPY_AND_MOVE(Channel);

  T Receive() { return DoReceive(); }

  void Send(T val) { return DoSend(val); }

  void Close() { return DoClose(); }

 private:
  T DoReceive() {
    if (closed_) {
      return {};
    }

    std::unique_lock l(mu_);
    has_receiver_ = true;
    cond_.notify_all();
    cond_.wait(l, [&] { return (has_receiver_ && has_value_) || closed_; });

    if (closed_) {
      return {};
    }

    has_value_ = false;
    has_receiver_ = false;
    return val_;
  }

  void DoSend(T val) {
    if (closed_) {
      return;
    }

    std::unique_lock l(mu_);
    cond_.wait(l, [&] { return (has_receiver_ && !has_value_) || closed_; });

    if (closed_) {
      return;
    }

    val_ = std::move(val);
    has_value_ = true;
    cond_.notify_all();
  }

  void DoClose() {
    closed_ = true;
    cond_.notify_all();
  }

  T val_;
  bool has_value_{false};
  bool has_receiver_{false};
  std::mutex mu_;
  std::condition_variable cond_;
  bool closed_{false};

  //  ConcurrentBlockingQueue<T> queue_;
};

}  // namespace kv::common