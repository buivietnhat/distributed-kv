#pragma once

#include <condition_variable>
#include <mutex>
#include <optional>

#include "common/container/concurrent_blocking_queue.h"
#include "common/macros.h"
#include "common/util.h"

namespace kv::common {

template <typename T>
class Channel {
 public:
  Channel() = default;

  DISALLOW_COPY_AND_MOVE(Channel);

  T Receive(std::optional<int> timeout = {}) { return DoReceive(timeout); }

  void Send(T val, std::optional<int> timeout = {}) { return DoSend(val, timeout); }

  void Close() { return DoClose(); }

  bool IsClose() const { return closed_; }

 private:
  T DoReceive(std::optional<int> timeout) {
    if (closed_) {
      return {};
    }

    std::unique_lock l(mu_);
    has_receiver_ = true;
    bool ok = true;
    cond_.notify_all();
    if (!timeout) {
      cond_.wait(l, [&] { return (has_receiver_ && has_value_) || closed_; });
    } else {
      ok = cond_.wait_for(l, MS(*timeout), [&] { return (has_receiver_ && has_value_) || closed_; });
    }

    if (closed_) {
      return {};
    }

    if (!ok) {
      return {};
    }

    has_value_ = false;
    has_receiver_ = false;
    return val_;
  }

  void DoSend(T val, std::optional<int> timeout) {
    if (closed_) {
      return;
    }

    bool ok = true;
    std::unique_lock l(mu_);
    if (!timeout) {
      cond_.wait(l, [&] { return (has_receiver_ && !has_value_) || closed_; });
    } else {
      ok = cond_.wait_for(l, MS(*timeout), [&] { return (has_receiver_ && !has_value_) || closed_; });
    }

    if (closed_) {
      return;
    }

    if (!ok) {
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
  std::atomic<bool> closed_{false};
};

}  // namespace kv::common