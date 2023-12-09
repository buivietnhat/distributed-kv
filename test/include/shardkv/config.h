#pragma once

#include <memory>
#include <mutex>

#include "network/network.h"
#include "shardctrler/client.h"
#include "shardctrler/server.h"
#include "shardkv/server.h"
#include "storage/mocking_persister.h"

namespace kv::shardkv {

struct Group {
  int gid_;
  std::vector<std::shared_ptr<ShardKV>> servers_;
  std::vector<std::shared_ptr<storage::PersistentInterface>> saved_;
  std::vector<std::vector<std::string>> endnames_;
  std::vector<std::vector<std::string>> mendnames_;
};

class Config {
 public:
  Config(int n, bool unreliable, int maxraftstate) : n_(n), maxraftstate_(maxraftstate) {
    net_ = std::make_unique<network::Network>();
    start_ = common::Now();

    // controller
    nctrlers_ = 3;
    ctrlerservers_.resize(nctrlers_);
    for (int i = 0; i < nctrlers_; i++) {
      StartCtrlerServer(i);
    }
    mck_ = ShardClerk();

    ngroups_ = 3;
    groups_.resize(ngroups_);
    for (int gi = 0; gi < ngroups_; gi++) {
      auto gg = std::make_unique<Group>();
      groups_[gi] = std::move(gg);
      groups_[gi]->gid_ = 100 + gi;
      groups_[gi]->servers_.resize(n_);
      groups_[gi]->saved_.resize(n_);
      groups_[gi]->endnames_.resize(n_);
      groups_[gi]->mendnames_.resize(nctrlers_);
      for (int i = 0; i < n_; i++) {
        StartServer(gi, i);
      }
    }

    next_client_id_ = n_ + 1000;  // client ids start 1000 above the highest serverid

    net_->SetReliable(!unreliable);
  }

  std::unique_ptr<Clerk> MakeClient() {
    std::lock_guard lock(mu_);

    // ClientEnds to talk to controler service
    auto ends = std::vector<network::ClientEnd *>(nctrlers_);
    auto endnames = std::vector<std::string>(n_);
    for (int j = 0; j < nctrlers_; j++) {
      endnames[j] = common::RandString(20);
      ends[j] = net_->MakeEnd(endnames[j]);
      net_->Connect(endnames[j], CtrlerName(j));
      net_->Enable(endnames[j], true);
    }

    auto ck = std::make_unique<Clerk>(ends, [&](std::string servername) {
      auto name = common::RandString(20);
      auto end = net_->MakeEnd(name);
      net_->Connect(name, servername);
      net_->Enable(name, true);
      return end;
    });

    clerks_[ck.get()] = endnames;
    next_client_id_++;

    return ck;
  }

  void DeleteClient(Clerk *ck) {
    std::lock_guard lock(mu_);

    clerks_.erase(ck);
  }

  // check that no server's log is too big
  void CheckLogs() {
    for (int gi = 0; gi < ngroups_; gi++) {
      for (int i = 0; i < n_; i++) {
        auto raft_size = groups_[gi]->saved_[i]->RaftStateSize();
        auto snap = groups_[gi]->saved_[i]->ReadRaftSnapshot();
        if (maxraftstate_ >= 0 && raft_size > 8 * maxraftstate_) {
          Logger::Debug(kDTest, -1,
                        fmt::format("persister.RaftStateSize() {}, but maxraftstate {}", raft_size, maxraftstate_));
//          throw SHARDKV_EXCEPTION(
//              fmt::format("persister.RaftStateSize() {}, but maxraftstate {}", raft_size, maxraftstate_));
        }
        if (maxraftstate_ < 0 && snap && snap->Size() > 0) {
          Logger::Debug(kDTest, -1, "maxraftstate is -1, but snapshot is non-empty!");
//          throw SHARDKV_EXCEPTION("maxraftstate is -1, but snapshot is non-empty!");
        }
      }
    }
  }

  inline bool CheckTimeout() {
    auto now = common::Now();
    auto execution_time = common::ElapsedTimeS(start_, now);
    return execution_time > 180;
  }

  void ShutdownServer(int gi, int i) {
//    Logger::Debug(kDTest, -1, fmt::format("Shut down server {} of Group {}", i, gi));
    std::unique_lock lock(mu_);

    auto gg = groups_[gi].get();

    // prevent this server from sending
    for (uint32_t j = 0; j < gg->servers_.size(); j++) {
      auto name = gg->endnames_[i][j];
      net_->Enable(name, false);
    }
    for (uint32_t j = 0; j < gg->mendnames_.size(); j++) {
      auto name = gg->mendnames_[i][j];
      net_->Enable(name, false);
    }

    // disable client connections to the server.
    // it's important to do this before creating
    // the new Persister in saved[i], to avoid
    // the possibility of the server returning a
    // positive reply to an Append but persisting
    // the result in the superseded Persister.
    net_->DeleteServer(ServerName(gg->gid_, i));

    // a fresh persister, in case old instance
    // continues to update the Persister.
    // but copy old persister's content so that we always
    // pass Make() the last persisted state.
    if (gg->saved_[i] != nullptr) {
      std::optional<raft::RaftPersistState> state;
      std::optional<raft::Snapshot> snap;
      gg->saved_[i]->ReadStateAndSnap(state, snap);
      gg->saved_[i] = std::make_shared<storage::MockingPersister>(std::move(state), std::move(snap));
    }

    auto kv = gg->servers_[i];
    if (kv != nullptr) {
      lock.unlock();
      kv->Kill();
      lock.lock();
      gg->servers_[i] = nullptr;
    }
  }

  void ShutdownGroup(int gi) {
//    Logger::Debug(kDTest, -1, fmt::format("Shutdown group {}", gi));
    for (int i = 0; i < n_; i++) {
      ShutdownServer(gi, i);
    }
  }

  inline bool CleanUp() {
    if (!finished_) {
      finished_ = true;
      for (int gi = 0; gi < ngroups_; gi++) {
        ShutdownGroup(gi);
      }

      for (int i = 0; i < nctrlers_; i++) {
        ctrlerservers_[i]->Kill();
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

  // start i'th server in gi'th group
  void StartServer(int gi, int i) {
    std::unique_lock lock(mu_);

    auto gg = groups_[gi].get();

    // a fresh set of outgoing ClientEnd names
    // to talk to other servers in this group.
    gg->endnames_[i].resize(n_);
    for (int j = 0; j < n_; j++) {
      gg->endnames_[i][j] = common::RandString(20);
    }

    // and the connections to other servers in this group.
    auto ends = std::vector<network::ClientEnd *>(n_);
    for (int j = 0; j < n_; j++) {
      ends[j] = net_->MakeEnd(gg->endnames_[i][j]);
      net_->Connect(gg->endnames_[i][j], ServerName(gg->gid_, j));
      net_->Enable(gg->endnames_[i][j], true);
    }

    // ends to talk to shardctrler service
    auto mends = std::vector<network::ClientEnd *>(nctrlers_);
    gg->mendnames_[i].resize(nctrlers_);
    for (int j = 0; j < nctrlers_; j++) {
      gg->mendnames_[i][j] = common::RandString(20);
      mends[j] = net_->MakeEnd(gg->mendnames_[i][j]);
      net_->Connect(gg->mendnames_[i][j], CtrlerName(j));
      net_->Enable(gg->mendnames_[i][j], true);
    }

    // a fresh persister, so old instance doesn't overwrite
    // new instance's persisted state.
    // give the fresh persister a copy of the old persister's
    // state, so that the spec is that we pass StartKVServer()
    // the last persisted state.
    if (gg->saved_[i] != nullptr) {
//      Logger::Debug(kDTest, -1, fmt::format("Copy old persister for group {} server {}", gi, i));
      std::optional<raft::RaftPersistState> state;
      std::optional<raft::Snapshot> snap;
      gg->saved_[i]->ReadStateAndSnap(state, snap);
      gg->saved_[i] = std::make_shared<storage::MockingPersister>(std::move(state), std::move(snap));
    } else {
//      Logger::Debug(kDTest, -1, fmt::format("Start with fresh persister for group {} server {}", gi, i));
      gg->saved_[i] = std::make_shared<storage::MockingPersister>();
    }
    lock.unlock();

    gg->servers_[i] =
        std::make_shared<ShardKV>(ends, i, gg->saved_[i], maxraftstate_, gg->gid_, mends, [&](std::string servername) {
          auto name = common::RandString(20);
          auto end = net_->MakeEnd(name);
          net_->Connect(name, servername);
          net_->Enable(name, true);
          return end;
        });

    auto server = std::make_unique<network::Server>();
    server->AddRaft(gg->servers_[i]->GetRaft());
    server->AddShardKV(gg->servers_[i].get());
    net_->AddServer(ServerName(gg->gid_, i), std::move(server));
  }

  void StartGroup(int gi) {
    for (int i = 0; i < n_; i++) {
      StartServer(gi, i);
    }
  }

  void StartCtrlerServer(int i) {
    // ClientEnds to talk to other controler replicas.
    auto ends = std::vector<network::ClientEnd *>(nctrlers_);
    for (int j = 0; j < nctrlers_; j++) {
      auto endname = common::RandString(20);
      ends[j] = net_->MakeEnd(endname);
      net_->Connect(endname, CtrlerName(j));
      net_->Enable(endname, true);
    }

    ctrlerservers_[i] =
        std::make_shared<shardctrler::ShardCtrler>(ends, i, std::make_shared<storage::MockingPersister>());

    auto server = std::make_unique<network::Server>();
    server->AddRaft(ctrlerservers_[i]->Raft());
    server->AddShardCtrler(ctrlerservers_[i].get());
    net_->AddServer(CtrlerName(i), std::move(server));
  }

  std::unique_ptr<shardctrler::Clerk> ShardClerk() {
    auto ends = std::vector<network::ClientEnd *>(nctrlers_);
    for (int j = 0; j < nctrlers_; j++) {
      auto name = common::RandString(20);
      ends[j] = net_->MakeEnd(name);
      net_->Connect(name, CtrlerName(j));
      net_->Enable(name, true);
    }

    return std::make_unique<shardctrler::Clerk>(ends);
  }

  // tell the shardctrler that a group is joining
  void Join(int gi) { Joinm({gi}); }

  void Joinm(std::vector<int> gis) {
    std::unordered_map<int, std::vector<std::string>> m;
    for (auto g : gis) {
      auto gid = groups_[g]->gid_;
      auto servername = std::vector<std::string>(n_);
      for (int i = 0; i < n_; i++) {
        servername[i] = ServerName(gid, i);
      }
      m[gid] = std::move(servername);
    }
    mck_->Join(m);
  }

  void Leave(int gi) { Leavem({gi}); }

  void Leavem(std::vector<int> gis) {
    auto gids = std::vector<int>();
    for (auto g : gis) {
      gids.push_back(groups_[g]->gid_);
    }
    mck_->Leave(gids);
  }

 private:
  std::string ServerName(int gid, int i) { return "server-" + std::to_string(gid) + "-" + std::to_string(i); }

  std::string CtrlerName(int i) { return "ctrler" + std::to_string(i); }

  std::mutex mu_;
  std::unique_ptr<network::Network> net_;
  common::time_t start_;

  int nctrlers_;
  std::vector<std::shared_ptr<shardctrler::ShardCtrler>> ctrlerservers_;
  std::unique_ptr<shardctrler::Clerk> mck_;

  int ngroups_;
  int n_;  // servers per k/v group
  std::vector<std::unique_ptr<Group>> groups_;

  std::unordered_map<Clerk *, std::vector<std::string>> clerks_;
  int next_client_id_;
  int maxraftstate_;
  bool finished_{false};
};

}  // namespace kv::shardkv
