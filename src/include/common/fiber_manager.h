#pragma once
#include <boost/fiber/all.hpp>
#include <boost/fiber/detail/thread_barrier.hpp>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

namespace kv::common {

class FiberThreadManager {
 public:
  explicit FiberThreadManager(const int num_worker) : num_worker_(num_worker) {
    finish_ = std::make_shared<bool>(false);
    mu_ = std::make_shared<boost::fibers::mutex>();
    cv_ = std::make_shared<boost::fibers::condition_variable>();

    boost::fibers::use_scheduling_algorithm<boost::fibers::algo::shared_work>();
    auto b = std::make_shared<boost::fibers::detail::thread_barrier>(num_worker);


    workers_.reserve(num_worker - 1);
    for (int i = 0; i < num_worker_ - 1; i++) {
      workers_.push_back(std::thread([b, mu = mu_, cv = cv_, finish = finish_] {
        // join the shared work scheduling

        boost::fibers::use_scheduling_algorithm<boost::fibers::algo::shared_work>();
        b->wait();  // sync with other threads, allow them to start processing

        std::unique_lock l(*mu);
        cv->wait(l, [&] { return *finish; });
      }));
    }

    b->wait();
  }

  ~FiberThreadManager() {
    {
      std::unique_lock l(*mu_);
      *finish_ = true;
    }
    cv_->notify_all();
    for (auto &&worker : workers_) {
      worker.detach();
    }
  }

 private:
  const int num_worker_;
  std::vector<std::thread> workers_;
  std::shared_ptr<boost::fibers::mutex> mu_;
  std::shared_ptr<boost::fibers::condition_variable> cv_;
  std::shared_ptr<bool> finish_;
};

}  // namespace kv::common