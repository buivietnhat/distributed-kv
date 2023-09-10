#pragma once

#include <filesystem>
#include <mutex>
#include <vector>

#include "common/util.h"
#include "network/network.h"
#include "shardctrler/client.h"
#include "shardctrler/server.h"
#include "storage/mocking_persister.h"

namespace kv::shardctrler {

inline std::vector<network::ClientEnd *> RandomHandlers(std::vector<network::ClientEnd *> kvh) {
  auto sa = kvh;
  for (uint32_t i = 0; i < sa.size(); i++) {
    auto j = common::RandNInt(i + 1);
    std::swap(sa[i], sa[j]);
  }
  return sa;
}

class Config {
 public:
  Config(int n, bool unreliable) {
    net_ = std::make_unique<network::Network>();
    n_ = n;
    servers_.resize(n_);
    saved_.resize(n_);
    endnames_.resize(n_);
    next_client_id_ = n_ + 1000;  // client ids start 1000 above the highest serverid
    start_ = common::Now();

    // create a full set of servers
    for (int i = 0; i < n_; i++) {
      StartServer(i);
    }

    ConnectAll();

    net_->SetReliable(!unreliable);
  }

  ~Config() { Cleanup(); }

  inline bool Cleanup() {
    if (!finished_) {
      finished_ = true;
      std::lock_guard l(mu_);
      for (uint32_t i = 0; i < servers_.size(); i++) {
        if (servers_[i] != nullptr) {
          servers_[i]->Kill();
        }
      }
      net_->Cleanup();
      auto timeout = CheckTimeout();
      if (!timeout) {
//        Logger::Debug(kDTest, -1, "  ... Passed --");
      }
      return !timeout;
    }
    return false;
  }

  inline int LogSize() const {
    int logsize = 0;
    for (int i = 0; i < n_; i++) {
      auto n = saved_[i]->RaftStateSize();
      if (n > logsize) {
        logsize = n;
      }
    }
    return logsize;
  }

  inline void Connect(int i, const std::vector<int> &to) {
    std::lock_guard l(mu_);
    ConnectUnlocked(i, to);
  }

  inline void Disconnect(int i, const std::vector<int> &from) {
    std::lock_guard l(mu_);
    DisconnectUnlocked(i, from);
  }

  inline void ConnectAll() {
    std::lock_guard l(mu_);
    for (int i = 0; i < n_; i++) {
      ConnectUnlocked(i, All());
    }
  }

  // set up 2 partitions with connectivity between servers in each partition
  inline void Partition(const std::vector<int> &p1, const std::vector<int> &p2) {
    std::lock_guard l(mu_);
    for (uint32_t i = 0; i < p1.size(); i++) {
      DisconnectUnlocked(p1[i], p2);
      ConnectUnlocked(p1[i], p1);
    }

    for (uint32_t i = 0; i < p2.size(); i++) {
      DisconnectUnlocked(p2[i], p1);
      ConnectUnlocked(p2[i], p2);
    }
  }

  // create a cleark with clerk specific server names
  // give it connections to all of the servers, but for
  // now enable only connections to servers in to
  inline Clerk *MakeClient(const std::vector<int> &to) {
    std::lock_guard l(mu_);

    // a fresh set of ClientEnds
    auto ends = std::vector<network::ClientEnd *>(n_);
    auto endnames = std::vector<std::string>(n_);
    for (int j = 0; j < n_; j++) {
      endnames[j] = common::RandString(20);
      ends[j] = net_->MakeEnd(endnames[j]);
      net_->Connect(endnames[j], std::to_string(j));
    }

    auto ck = std::make_shared<Clerk>(RandomHandlers(ends));
    clerks_[ck] = endnames;
    next_client_id_++;
    ConnectClientUnlocked(ck, to);

    return ck.get();
  }

  inline void DeleteClient(std::shared_ptr<Clerk> ck) {
    std::lock_guard l(mu_);

    auto v = clerks_[ck];
    for (uint32_t i = 0; i < v.size(); i++) {
      std::filesystem::remove(v[i]);
    }
    clerks_.erase(ck);
  }

  inline void ConnectClientUnlocked(std::shared_ptr<Clerk> ck, const std::vector<int> &to) {
    auto endnames = clerks_[ck];
    for (uint32_t j = 0; j < to.size(); j++) {
      auto s = endnames[to[j]];
      net_->Enable(s, true);
    }
  }

  inline void ConnectClient(std::shared_ptr<Clerk> ck, const std::vector<int> &to) {
    std::lock_guard l(mu_);
    ConnectClientUnlocked(ck, to);
  }

  inline void DisconnectClientUnlocked(std::shared_ptr<Clerk> ck, const std::vector<int> &from) {
    auto endnames = clerks_[ck];
    for (uint32_t j = 0; j < from.size(); j++) {
      auto s = endnames[from[j]];
      net_->Enable(s, false);
    }
  }

  inline void DisconnectClient(std::shared_ptr<Clerk> ck, const std::vector<int> &from) {
    std::lock_guard l(mu_);
    DisconnectClientUnlocked(ck, from);
  }

  // shutdown a server by isolating it
  inline void ShutdownServer(int i) {
    std::unique_lock l(mu_);

    DisconnectUnlocked(i, All());

    net_->DeleteServer(std::to_string(i));

    if (saved_[i] != nullptr) {
      std::optional<raft::RaftPersistState> state;
      std::optional<raft::Snapshot> snap;
      saved_[i]->ReadStateAndSnap(state, snap);
      saved_[i] = std::make_unique<storage::MockingPersister>(std::move(state), std::move(snap));
    }

    auto kv = servers_[i].get();
    if (kv != nullptr) {
      l.unlock();
      kv->Kill();
      l.lock();
      servers_[i] = nullptr;
    }
  }

  inline void StartServer(int i) {
    std::unique_lock l(mu_);

    // a fresh set of outgoing ClientEnd names.
    endnames_[i] = std::vector<std::string>(n_);
    for (int j = 0; j < n_; j++) {
      endnames_[i][j] = common::RandString(20);
    }

    // a fresh set of ClientEnds
    auto ends = std::vector<network::ClientEnd *>(n_);
    for (int j = 0; j < n_; j++) {
      ends[j] = net_->MakeEnd(endnames_[i][j]);
      net_->Connect(endnames_[i][j], std::to_string(j));
    }

    if (saved_[i] != nullptr) {
      std::optional<raft::RaftPersistState> state;
      std::optional<raft::Snapshot> snap;
      saved_[i]->ReadStateAndSnap(state, snap);
      saved_[i] = std::make_shared<storage::MockingPersister>(std::move(state), std::move(snap));
    } else {
      saved_[i] = std::make_shared<storage::MockingPersister>();
    }

    l.unlock();

    servers_[i] = std::make_unique<ShardCtrler>(std::move(ends), i, saved_[i]);

    auto server = std::make_unique<network::Server>();
    server->AddShardCtrler(servers_[i].get());
    server->AddRaft(servers_[i]->Raft());
    net_->AddServer(std::to_string(i), std::move(server));
  }

  std::pair<bool, int> Leader() const {
    std::lock_guard l(mu_);

    for (int i = 0; i < n_; i++) {
      if (servers_[i] != nullptr) {
        auto [_, is_leader] = servers_[i]->Raft()->GetState();
        if (is_leader) {
          return {true, i};
        }
      }
    }
    return {false, 0};
  }

  // partition servers into 2 groups and put current leader in minority
  inline std::pair<std::vector<int>, std::vector<int>> MakePartition() const {
    auto [_, l] = Leader();
    auto p1 = std::vector<int>(n_ / 2 + 1);
    auto p2 = std::vector<int>(n_ / 2);
    int j = 0;
    for (int i = 0; i < n_; i++) {
      if (i != l) {
        if (j < static_cast<int>(p1.size())) {
          p1[j] = i;
        } else {
          p2[j - p1.size()] = i;
        }
        j++;
      }
    }
    p2[p2.size() - 1] = l;
    return {std::move(p1), std::move(p2)};
  }

  std::vector<int> All() const {
    std::vector<int> all(n_);
    for (int i = 0; i < n_; i++) {
      all[i] = i;
    }
    return all;
  }

 private:
  // attach server i to servers listed in to
  inline void ConnectUnlocked(int i, const std::vector<int> &to) {
    // outgoing socket files
    for (uint32_t j = 0; j < to.size(); j++) {
      auto endname = endnames_[i][to[j]];
      net_->Enable(endname, true);
    }

    // incoming socket files
    for (uint32_t j = 0; j < to.size(); j++) {
      auto endname = endnames_[to[j]][i];
      net_->Enable(endname, true);
    }
  }

  // detach server i from the servers listed in from
  inline void DisconnectUnlocked(int i, const std::vector<int> &from) {
    // outgoing socket files
    for (uint32_t j = 0; j < from.size(); j++) {
      if (!endnames_[i].empty()) {
        auto endname = endnames_[i][from[j]];
        net_->Enable(endname, false);
      }
    }

    // incoming socket files
    for (uint32_t j = 0; j < from.size(); j++) {
      if (!endnames_[j].empty()) {
        auto endname = endnames_[from[j]][i];
        net_->Enable(endname, false);
      }
    }
  }

  inline bool CheckTimeout() {
    auto now = common::Now();
    auto execution_time = common::ElapsedTimeS(start_, now);
    return execution_time > 180;
  }

  mutable std::mutex mu_;
  std::unique_ptr<network::Network> net_;
  bool finished_{false};
  int n_;
  std::vector<std::unique_ptr<ShardCtrler>> servers_;
  std::vector<std::shared_ptr<storage::PersistentInterface>> saved_;
  std::vector<std::vector<std::string>> endnames_;  // names of each server's sending ClientEnds
  std::unordered_map<std::shared_ptr<Clerk>, std::vector<std::string>> clerks_;
  int next_client_id_;
  common::time_t start_;
};

}  // namespace kv::shardctrler
