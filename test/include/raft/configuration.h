#pragma once

#include <any>
#include <format>
#include <functional>
#include <iostream>
#include <mutex>
#include <string_view>

#include "common/container/concurrent_blocking_queue.h"
#include "common/logger.h"
#include "common/util.h"
#include "network/network.h"
#include "raft/raft.h"
#include "storage/mocking_persister.h"

using namespace std::chrono_literals;

namespace kv::raft {

using common::Logger;

class Configuration {
  using applier_t = std::function<void(int, std::shared_ptr<common::ConcurrentBlockingQueue<ApplyMsg>>)>;

 public:
  Configuration(int num_servers, bool unreliable, bool snapshot) {
    net_ = std::make_unique<network::Network>();
    num_servers_ = num_servers;

    rafts_.resize(num_servers);
    connected_.resize(num_servers);
    endnames_.resize(num_servers);
    last_applied_.resize(num_servers);
    logs_.resize(num_servers);
    saved_.resize(num_servers);

    net_->SetLongDelay(true);

    applier_t applier;
    if (snapshot) {
      applier = [&](int server_num, std::shared_ptr<common::ConcurrentBlockingQueue<ApplyMsg>> apply_channel) {
        ApplierSnap(server_num, apply_channel);
      };
    } else {
      applier = [&](int server_num, std::shared_ptr<common::ConcurrentBlockingQueue<ApplyMsg>> apply_channel) {
        Applier(server_num, apply_channel);
      };
    }

    for (int i = 0; i < num_servers_; i++) {
      Start(i, applier);
    }

    // connect everyone
    for (int i = 0; i < num_servers_; i++) {
      Connect(i);
    }

    start_ = common::Now();
  }

  ~Configuration() { Cleanup(); }

  void Disconnect(int server_num) {
    connected_[server_num] = false;

    // outgoing Clientends
    for (int i = 0; i < num_servers_; i++) {
      if (!endnames_[server_num].empty()) {
        auto endname = endnames_[server_num][i];
        net_->Enable(endname, false);
      }
    }

    // incoming ClientEnds
    for (int i = 0; i < num_servers_; i++) {
      if (!endnames_[i].empty()) {
        auto endname = endnames_[i][server_num];
        net_->Enable(endname, false);
      }
    }
  }

  void Connect(int server_num) {
    connected_[server_num] = true;

    // outgoing ClientEnds
    for (int j = 0; j < num_servers_; j++) {
      if (connected_[j]) {
        auto endname = endnames_[server_num][j];
        net_->Enable(endname, true);
      }
    }

    // incoming ClientEnds
    for (int j = 0; j < num_servers_; j++) {
      if (connected_[j]) {
        auto endname = endnames_[j][server_num];
        net_->Enable(endname, true);
      }
    }
  }

  void Crash(int server_num) {
    Disconnect(server_num);
    net_->DeleteServer(std::to_string(server_num));

    std::lock_guard lock(mu_);

    auto *rf = rafts_[server_num].get();
    if (rf != nullptr) {
      mu_.lock();
      rf->Kill();
      mu_.unlock();
      rafts_[server_num] = nullptr;
    }

    if (saved_[server_num] != nullptr) {
      raft::RaftPersistState state;
      raft::Snapshot snapshot;
      saved_[server_num]->ReadRaftState(&state);
      saved_[server_num]->ReadRaftSnapshot(&snapshot);
      saved_[server_num] = std::make_unique<storage::MockingPersister>(std::move(state), std::move(snapshot));
    }
  }

  void Start(int server_num, applier_t applier) {
    Crash(server_num);

    // a fresh set of outgoing ClientEnd names.
    // so that old crashed instance's ClientEnds can't send
    endnames_[server_num] = std::vector<std::string>(num_servers_);
    for (int j = 0; j < num_servers_; j++) {
      endnames_[server_num][j] = common::RandString(20);
    }

    // a fresh set of ClientEnds
    auto ends = std::vector<network::ClientEnd *>(num_servers_);
    for (int j = 0; j < num_servers_; j++) {
      ends[j] = net_->MakeEnd(endnames_[server_num][j]);
      net_->Connect(endnames_[server_num][j], std::to_string(j));
    }

    mu_.lock();
    last_applied_[server_num] = 0;

    // a fresh persister, so old instance doesn't overwrite
    // new instance's persisted state.
    // but copy old persister's content so that we always
    // pass Make() the last persisted state.
    if (saved_[server_num] != nullptr) {
      raft::RaftPersistState state;
      raft::Snapshot snapshot;
      saved_[server_num]->ReadRaftState(&state);
      saved_[server_num]->ReadRaftSnapshot(&snapshot);
      saved_[server_num] = std::make_unique<storage::MockingPersister>(std::move(state), std::move(snapshot));

      if (!snapshot.Empty()) {
        auto err = IngestSnap(server_num, snapshot, -1);
        if (err != "") {
          throw std::runtime_error(fmt::format("{}", err));
        }
      }
    } else {
      saved_[server_num] = std::make_unique<storage::MockingPersister>();
    }

    mu_.unlock();

    auto appply_channel = std::make_shared<common::ConcurrentBlockingQueue<raft::ApplyMsg>>();
    auto rf = std::make_unique<Raft>(std::move(ends), server_num, saved_[server_num].get(), appply_channel);

    mu_.lock();
    rafts_[server_num] = std::move(rf);
    mu_.unlock();

    std::thread apply_thread(applier, server_num, appply_channel);
    apply_thread.detach();

    auto server = std::make_unique<network::Server>();
    server->AddRaft(rafts_[server_num].get());
    net_->AddServer(std::to_string(server_num), std::move(server));
  }

  void SetUnreliable(bool unrel) { net_->SetReliable(!unrel); }

  void Begin(std::string_view description) {
    Logger::Debug(kDTest, -1, fmt::format("{} ...", description));
    t0_ = common::Now();
  }

  // return false if timeouted
  bool Cleanup() {
    if (!finished_) {
      finished_ = true;

      for (int i = 0; i < num_servers_; i++) {
        if (rafts_[i] != nullptr) {
          rafts_[i]->Kill();
        }
      }

      net_->Cleanup();
      return !CheckTimeout();
    }
    return false;
  }

  // ok if the time it takes <= 120s
  bool CheckTimeout() const {
    auto now = common::Now();
    auto execution_time = common::ElapsedTimeS(start_, now);
    return execution_time > 120;
  }

  int CheckOneLeader() const {
    for (int iters = 0; iters < 10; iters++) {
      auto ms = 450 + common::RandInt() % 100;
      std::this_thread::sleep_for(std::chrono::milliseconds(ms));

      std::unordered_map<int, std::vector<int>> leaders_map;
      for (int i = 0; i < num_servers_; i++) {
        if (connected_[i]) {
          auto [term, is_leader] = rafts_[i]->GetState();
          if (is_leader) {
            leaders_map[term].push_back(i);
          }
        }
      }

      auto last_term_with_leader = -1;
      for (const auto &[term, leaders] : leaders_map) {
        if (leaders.size() > 1) {
          std::cout << fmt::format("term {} has {}(>1) leaders\n", term, leaders.size());
          return -1;
        }

        if (term > last_term_with_leader) {
          last_term_with_leader = term;
        }
      }

      if (!leaders_map.empty()) {
        return leaders_map[last_term_with_leader][0];
      }
    }

    std::cout << fmt::format("expected one leader, got none\n");
    return -1;
  }

  std::optional<int> CheckTerm() {
    int term = -1;
    for (int i = 0; i < num_servers_; i++) {
      if (connected_[i]) {
        auto [xterm, _] = rafts_[i]->GetState();
        if (term == -1) {
          term = xterm;
        } else if (term != xterm) {
          Logger::Debug(kDTest, -1, "servers disagree on term");
          return {};
        }
      }
    }
    return term;
  }

  // check that one of the connected servers think it is the leader
  bool CheckNoLeader() {
    for (int i = 0; i < num_servers_; i++) {
      if (connected_[i]) {
        auto [_, is_leader] = rafts_[i]->GetState();
        if (is_leader) {
          Logger::Debug(kDTest, -1,
                        fmt::format("expected no leader among connected servers, but {} claims to be leader", i));
          return false;
        }
      }
    }
    return true;
  }

 private:
  std::string IngestSnap(int server_num, const raft::Snapshot &snapshot, int index) {
    if (snapshot.Empty()) {
      throw std::runtime_error("empty snapshot");
    }

    // TODO(nhat): find a way to to work with serialized data

    return "";
  }

  void ApplierSnap(int server_num, std::shared_ptr<common::ConcurrentBlockingQueue<raft::ApplyMsg>>) {
    while (!finished_) {
    }
  }

  void Applier(int server_num, std::shared_ptr<common::ConcurrentBlockingQueue<raft::ApplyMsg>>) {
    while (!finished_) {
      //      std::cout << "applier running for server " << server_num << std::endl;
      std::this_thread::sleep_for(100ms);
    }
  }

  std::mutex mu_;
  bool finished_{false};
  int num_servers_;
  std::unique_ptr<network::Network> net_;
  std::vector<std::unique_ptr<Raft>> rafts_;
  std::vector<bool> connected_;
  std::vector<std::vector<std::string>> endnames_;
  std::vector<std::unordered_map<int, std::any>> logs_;
  std::vector<std::unique_ptr<storage::PersistentInterface>> saved_;
  std::vector<int> last_applied_;
  common::time_t t0_;     // time at which tester called Begin()
  common::time_t start_;  // time at which the Configuration constructor was called
};

}  // namespace kv::raft