//#pragma once
//
//#include <deque>
//#include <functional>
//#include <mutex>
//#include <thread>
//
//#include "common/macros.h"
//
//namespace kv::common {
//
//class ThreadRegistry {
// public:
//  ThreadRegistry() {
//    bk_thread_ = std::thread([&] { BookKeeper(); });
//  };
//
//  DISALLOW_COPY_AND_MOVE(ThreadRegistry);
//
//  void RegisterNewThread(std::function<void(void)> task) {
//    std::lock_guard lock(mu_);
//    threads_.emplace_back(std::move(task));
//  }
//
//  ~ThreadRegistry() {
//    finished_ = true;
//    bk_thread_.join();
//
//    for (auto &thread : threads_) {
//      if (thread.joinable()) {
//        thread.join();
//      }
//    }
//  }
//
//  void BookKeeper() {
//    while (!finished_) {
//      std::thread t;
//      {
//        std::lock_guard lock(mu_);
//        if (!threads_.empty()) {
//          t = std::move(threads_.front());
//          threads_.pop_front();
//        }
//      }
//      if (t.joinable()) {
//        t.join();
//      }
//    }
//  }
//
// private:
//  std::mutex mu_;
//  std::deque<std::thread> threads_;
//  std::thread bk_thread_;
//  bool finished_{false};
//};
//
//}  // namespace kv::common
