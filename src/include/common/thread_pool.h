#pragma once

#include <condition_variable>
#include <deque>
#include <future>
#include <iostream>
#include <thread>
#include <vector>

namespace kv::common {

class ThreadPool {
 public:
  explicit ThreadPool(int num_worker) : num_worker_(num_worker) { SpawnThreads(); }
  explicit ThreadPool() : num_worker_(DEFAULT_NUM_WORKER) { SpawnThreads(); }

  void AddTask(std::function<void(void)> func) {
    auto task = std::packaged_task<void(void)>(func);
    std::lock_guard lock(m_);
    task_queue_.push_back(std::move(task));
    cond_.notify_one();
  }

  // wait for all tasks to be finished
  void Wait() {
    while (!task_queue_.empty()) {
    }
  }

  void Drain() {
    std::lock_guard l(m_);
    while (!task_queue_.empty()) {
      task_queue_.pop_front();
    }
  }

  uint32_t UnsafeSize() const { return task_queue_.size(); }

  ~ThreadPool() {
    finished_ = true;
    cond_.notify_all();
    for (auto &thread : threads_) {
      thread.join();
    }
  }

 private:
  void Dispatch() {
    while (!finished_) {
      std::unique_lock lock(m_);
      cond_.wait(lock, [this]() { return !task_queue_.empty() || finished_; });

      if (finished_) {
        return;
      }

      auto task = std::move(task_queue_.front());
      task_queue_.pop_front();
      lock.unlock();

      task();
    }
  }

  void SpawnThreads() {
    for (int i = 0; i < num_worker_; i++) {
      threads_.emplace_back(&ThreadPool::Dispatch, this);
    }
  }

  static constexpr int DEFAULT_NUM_WORKER = 5;
  std::vector<std::thread> threads_;
  const int num_worker_;
  std::deque<std::packaged_task<void(void)>> task_queue_;
  mutable std::mutex m_;
  std::condition_variable cond_;
  bool finished_{false};
};

}  // namespace kv::common
