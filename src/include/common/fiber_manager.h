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
    boost::fibers::use_scheduling_algorithm<boost::fibers::algo::shared_work>();
    boost::fibers::detail::thread_barrier b(num_worker);

//    std::cout << "main thread started " << std::this_thread::get_id() << std::endl;

    workers_.reserve(num_worker - 1);
    for (int i = 0; i < num_worker_ - 1; i++) {
      workers_.push_back(std::thread([&] {
        // join the shared work scheduling
//        std::cout << "thread started " << std::this_thread::get_id() << std::endl;

        boost::fibers::use_scheduling_algorithm<boost::fibers::algo::shared_work>();
        b.wait();  // sync with other threads, allow them to start processing

        std::unique_lock l(mu_);
        cv_.wait(l, [&] { return finish_; });
//        std::cout << "thread finished " << std::this_thread::get_id() << std::endl;
      }));
    }

    b.wait();
  }

  ~FiberThreadManager() {
    {
      std::unique_lock l(mu_);
      finish_ = true;
    }
    cv_.notify_all();
    for (auto &&worker : workers_) {
      worker.join();
    }
  }

 private:

  const int num_worker_ ;
  std::vector<std::thread> workers_;
  boost::fibers::mutex mu_;
  boost::fibers::condition_variable cv_;
  bool finish_ = false;
};

}  // namespace kv::common