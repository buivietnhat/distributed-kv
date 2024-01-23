#pragma once

#include <any>
#include <functional>
#include <iostream>
#include <mutex>
#include <string_view>

#include "common/container/concurrent_blocking_queue.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/util.h"
#include "common/fiber_manager.h"
#include "network/network.h"
#include "nlohmann/json.hpp"
#include "raft/raft.h"
#include "storage/mocking_persister.h"

using namespace std::chrono_literals;

namespace kv::raft {

static constexpr int RAFT_ELECTION_TIMEOUT = 1000;
static constexpr int SNAPSHOT_INTERVAL = 10;
static constexpr int MAXLOGSIZE = 2000;

using common::Logger;

template <typename CommandType>
class Config {
 public:
  Config(int num_servers, bool unreliable, bool snapshot, int num_worker = DEFAULT_NUM_WORKER) : ftm_(num_worker) {
    net_ = std::make_shared<network::Network>();
    num_servers_ = num_servers;

    rafts_.resize(num_servers);
    connected_.resize(num_servers);
    endnames_.resize(num_servers);
    last_applied_.resize(num_servers);
    logs_.resize(num_servers);
    saved_.resize(num_servers);
    apply_err_.resize(num_servers);
    apply_chs_.resize(num_servers);
    apply_threads_.resize(num_servers);

    apply_finished_ = std::vector<bool>(num_servers, false);

    net_->SetLongDelay(true);
    net_->SetReliable(!unreliable);

    applier_t applier;
    if (snapshot) {
      applier = [&](int server_num, apply_channel_ptr apply_channel) { ApplierSnap(server_num, apply_channel); };
    } else {
      applier = [&](int server_num, apply_channel_ptr apply_channel) { Applier(server_num, apply_channel); };
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

  ~Config() { Cleanup(); }

  // how many servers think a log entry is commited?
  std::pair<int, std::any> NCommited(int index) {
    int count = 0;
    std::any cmd;

    for (uint32_t i = 0; i < rafts_.size(); i++) {
      if (apply_err_[i] != "") {
        throw CONFIG_EXCEPTION(apply_err_[i]);
      }

      std::unique_lock l(mu_);
      auto ok = logs_[i].contains(index);
      std::any cmd1;
      if (ok) {
        cmd1 = logs_[i][index];
      }
      l.unlock();

      if (ok) {
        if (count > 0 && std::any_cast<CommandType>(cmd) != std::any_cast<CommandType>(cmd1)) {
          throw CONFIG_EXCEPTION(fmt::format("committed values do not match: index {}, {}, {}", index,
                                             std::any_cast<CommandType>(cmd), std::any_cast<CommandType>(cmd1)));
        }
        count += 1;
        cmd = cmd1;
      }
    }
    return {count, cmd};
  }

  Raft *GetRaft(int server) { return rafts_[server].get(); }

  // wait for at least n servers to commit.
  // but don't wait forever.
  std::any Wait(int index, int n, int start_term) {
    auto to = 10;
    for (int iters = 0; iters < 30; iters++) {
      auto [nd, _] = NCommited(index);
      if (nd >= n) {
        break;
      }
      common::SleepMs(to);
      if (to < 1000) {  // 1 second
        to *= 2;
      }
      if (start_term > -1) {
        for (const auto &r : rafts_) {
          auto [t, _] = r->GetState();
          if (t > start_term) {
            // someone has moved on
            // can no longer guarantee that we'll "win"
            return -1;
          }
        }
      }
    }
    auto [nd, cmd] = NCommited(index);
    if (nd < n) {
      throw CONFIG_EXCEPTION(fmt::format("only {} decided for index {}; wanted {}", nd, index, n));
    }
    return cmd;
  }

  // do a complete agreement.
  int One(std::any cmd, int expected_server, bool retry) {
    auto t0 = common::Now();
    auto starts = 0;
    while (common::ElapsedTimeS(t0, common::Now()) < 10 && !finished_) {
      // try all the servers, maybe one is the leader
      int index = -1;
      for (int si = 0; si < num_servers_; si++) {
        starts = (starts + 1) % num_servers_;
        raft::Raft *rf{nullptr};
        std::unique_lock l(mu_);
        if (connected_[starts]) {
          rf = rafts_[starts].get();
        }
        l.unlock();
        if (rf != nullptr) {
          auto [index1, _, ok] = rf->Start(cmd);
          if (ok) {
            index = index1;
            break;
          }
        }
      }

      if (index != -1) {
        // somebody claimed to be the leader and to have
        // submitted our command; wait a while for agreement.
        auto t1 = common::Now();
        while (common::ElapsedTimeS(t1, common::Now()) < 2) {
          auto [nd, cmd1] = NCommited(index);
          //          Logger::Debug(kDTest, -1, fmt::format("index = {}, nd = {}", index, nd));
          if (nd > 0 && nd >= expected_server) {
            // commited
            if (std::any_cast<CommandType>(cmd1) == std::any_cast<CommandType>(cmd)) {
              // and it was the command we submitted
              return index;
            }
          }
          common::SleepMs(20);
        }
        if (retry == false) {
          Logger::Debug(kDTest, -1, fmt::format("One({}) failed to reach agreement", std::any_cast<CommandType>(cmd)));
          throw CONFIG_EXCEPTION(fmt::format("One({}) failed to reach agreement", std::any_cast<CommandType>(cmd)));
        }
      } else {
        common::SleepMs(50);
      }
    }
    if (finished_ == false) {
      Logger::Debug(kDTest, -1, fmt::format("One({}) failed to reach agreement", std::any_cast<CommandType>(cmd)));
      throw CONFIG_EXCEPTION(fmt::format("One({}) failed to reach agreement", std::any_cast<CommandType>(cmd)));
    }
    return -1;
  }

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

    std::unique_lock lock(mu_);

    auto *rf = rafts_[server_num].get();
    if (rf != nullptr) {
      lock.unlock();
      rf->Kill();
      lock.lock();
      rafts_[server_num] = nullptr;
    }

    if (saved_[server_num] != nullptr) {
      auto state = saved_[server_num]->ReadRaftState();
      auto snap = saved_[server_num]->ReadRaftSnapshot();
      saved_[server_num] = std::make_shared<storage::MockingPersister>(std::move(state), std::move(snap));
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
      //      auto state = saved_[server_num]->ReadRaftState();
      //      auto snap = saved_[server_num]->ReadRaftSnapshot();
      std::optional<raft::RaftPersistState> state;
      std::optional<raft::Snapshot> snap;
      saved_[server_num]->ReadStateAndSnap(state, snap);
      saved_[server_num] = std::make_shared<storage::MockingPersister>(std::move(state), std::move(snap));

      if (snap && !snap->Empty()) {
        auto err = IngestSnap(server_num, *snap, -1);
        if (err != "") {
          throw std::runtime_error(fmt::format("{}", err));
        }
      }
    } else {
      saved_[server_num] = std::make_shared<storage::MockingPersister>();
    }

    mu_.unlock();

    if (apply_threads_[server_num].joinable()) {
      apply_finished_[server_num] = true;
      apply_chs_[server_num]->close();
      apply_threads_[server_num].join();
      apply_finished_[server_num] = false;
    }

    apply_chs_[server_num] = std::make_shared<raft::apply_channel_t>();
    auto rf = std::make_unique<Raft>(std::move(ends), server_num, saved_[server_num], apply_chs_[server_num]);

    mu_.lock();
    rafts_[server_num] = std::move(rf);
    mu_.unlock();

    apply_threads_[server_num] = boost::fibers::fiber(applier, server_num, apply_chs_[server_num]);

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
      apply_finished_ = std::vector<bool>(num_servers_, true);

      for (int i = 0; i < num_servers_; i++) {
        if (rafts_[i] != nullptr) {
          rafts_[i]->Kill();
        }
      }

      net_->Cleanup();

      // wake up all the apply channels to finish the thread
      for (auto &apply_ch : apply_chs_) {
//        apply_ch->Enqueue({});
        apply_ch->close();
      }
      for (auto &thread : apply_threads_) {
        thread.join();
      }

      auto timeout = CheckTimeout();
      if (!timeout) {
        Logger::Debug(kDTest, -1, "  ... Passed --");
      }
      return !timeout;
    }
    return false;
  }

  // ok if the time it takes <= 180s
  bool CheckTimeout() const {
    auto now = common::Now();
    auto execution_time = common::ElapsedTimeS(start_, now);
    return execution_time > 180;
  }

  int CheckOneLeader() const {
    for (int iters = 0; iters < 10; iters++) {
      auto ms = 450 + common::RandInt() % 100;
      boost::this_fiber::sleep_for(std::chrono::milliseconds(ms));

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
          throw CONFIG_EXCEPTION(fmt::format("term {} has {}(>1) leaders\n", term, leaders.size()));
        }

        if (term > last_term_with_leader) {
          last_term_with_leader = term;
        }
      }

      if (!leaders_map.empty()) {
        return leaders_map[last_term_with_leader][0];
      }
    }

    throw CONFIG_EXCEPTION("expected one leader, got none");
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

  inline std::function<void(int, apply_channel_t)> GetApplier() {
    return [&](int server_num, apply_channel_t apply_channel) { Applier(server_num, apply_channel); };
  }

  inline std::function<void(int, apply_channel_t)> GetApplierSnap() {
    return [&](int server_num, apply_channel_t apply_channel) { ApplierSnap(server_num, apply_channel); };
  }

  // Maximum log size across all servers
  inline int LogSize() const {
    int logsize = 0;
    for (int i = 0; i < num_servers_; i++) {
      auto n = saved_[i]->RaftStateSize();
      if (n > logsize) {
        logsize = n;
      }
    }
    return logsize;
  }

 private:
  std::string IngestSnap(int server_num, const raft::Snapshot &snapshot, int index) {
    if (snapshot.Empty()) {
      throw std::runtime_error("empty snapshot");
    }

    int last_included_index{-1};
    std::vector<CommandType> xlog;

    try {
      auto json_snap = nlohmann::json::parse(snapshot.data_);
      last_included_index = json_snap["last_included_index"].get<int>();
      xlog = std::move(json_snap["xlog"]).get<std::vector<CommandType>>();
    } catch (...) {
      return "snapshot Decode() error";
    }

    if (index != -1 && index != last_included_index) {
      return fmt::format("server {} snapshot doesn't match m.SnapshotIndex", server_num);
    }

    logs_[server_num] = std::unordered_map<int, std::any>();
    for (uint32_t j = 0; j < xlog.size(); j++) {
      logs_[server_num][j] = xlog[j];
    }
    last_applied_[server_num] = last_included_index;
    return "";
  }

  std::pair<std::string, bool> CheckLogs(int i, const raft::ApplyMsg &m) {
    std::string err_msg = "";
    auto v = std::any_cast<CommandType>(m.command_);

    for (uint32_t j = 0; j < logs_.size(); j++) {
      if (logs_[j].contains(m.command_index_)) {
        auto old = std::any_cast<CommandType>(logs_[j][m.command_index_]);
        if (old != v) {
          err_msg = fmt::format("commit index={} server={} {} != server={} {}", m.command_index_, i,
                                std::any_cast<CommandType>(m.command_), j, old);
        }
      }
    }

    bool prevok = logs_[i].contains(m.command_index_ - 1);
    logs_[i][m.command_index_] = v;
    if (m.command_index_ > max_index_) {
      max_index_ = m.command_index_;
    }

    return {err_msg, prevok};
  }

  void ApplierSnap(int server_num, apply_channel_ptr apply_ch) {
    std::unique_lock l(mu_);
    auto rf = rafts_[server_num].get();
    l.unlock();

    if (rf == nullptr) {
      throw CONFIG_EXCEPTION(fmt::format("Raft {} is nullptr", server_num));
    }

    while (!apply_finished_[server_num]) {
      raft::ApplyMsg m;
      apply_ch->pop(m);
      std::string err_msg = "";
      if (m.snapshot_valid_) {
        l.lock();
        err_msg = IngestSnap(server_num, m.snapshot_, m.snapshot_index_);
        l.unlock();
      } else if (m.command_valid_) {
        if (m.command_index_ != last_applied_[server_num] + 1) {
          err_msg = fmt::format("server {} apply out of order, expected index {}, got {}", server_num,
                                last_applied_[server_num] + 1, m.command_index_);
        }

        if (err_msg == "") {
          l.lock();
          auto [err, prevok] = CheckLogs(server_num, m);
          err_msg = std::move(err);
          l.unlock();

          if (m.command_index_ > 1 && prevok == false) {
            err_msg = fmt::format("server {} apply out of order {}", server_num, m.command_index_);
          }
        }

        l.lock();
        last_applied_[server_num] = m.command_index_;
        l.unlock();

        if (m.command_index_ % SNAPSHOT_INTERVAL == 0) {
          nlohmann::json json_snap;
          json_snap["last_included_index"] = m.command_index_;
          std::vector<CommandType> xlog;
          for (int j = 0; j <= m.command_index_; j++) {
            if (logs_[server_num].contains(j)) {
              xlog.push_back(std::any_cast<CommandType>(logs_[server_num][j]));
            } else {
              // little hack for the very first index
              xlog.push_back({});
            }
          }
          json_snap["xlog"] = xlog;
          Snapshot snap{json_snap.dump()};
          rf->DoSnapshot(m.command_index_, snap);
        }
      } else {
        // Ignore other types of ApplyMsg.
      }
      if (err_msg != "") {
        Logger::Debug(kDTest, -1, fmt::format("apply error: {}", err_msg));
        apply_err_[server_num] = err_msg;
        // keep reading after error so that Raft doesn't block holding locks...
      }
    }
  }

  void Applier(int server_num, apply_channel_ptr apply_ch) {
    while (!apply_finished_[server_num]) {
      raft::ApplyMsg m;
      apply_ch->pop(m);
      if (m.command_valid_ == false) {
        // ignore
      } else {
        std::unique_lock l(mu_);
        auto [err_msg, prevok] = CheckLogs(server_num, m);
        l.unlock();

        if (m.command_index_ > 1 && prevok == false) {
          err_msg = fmt::format("server {} apply out of order {}\n", server_num, m.command_index_);
        }
        if (err_msg != "") {
          Logger::Debug(kDTest, -1, fmt::format("apply error: {}", err_msg));
          apply_err_[server_num] = err_msg;
        }
      }
    }
  }

  common::FiberThreadManager ftm_;
  boost::fibers::mutex mu_;
  bool finished_{false};
  std::vector<bool> apply_finished_;
  int num_servers_;
  std::shared_ptr<network::Network> net_;
  std::vector<std::shared_ptr<Raft>> rafts_;
  std::vector<bool> connected_;
  std::vector<std::vector<std::string>> endnames_;
  std::vector<std::unordered_map<int, std::any>> logs_;
  std::vector<std::shared_ptr<storage::PersistentInterface>> saved_;
  std::vector<int> last_applied_;
  common::time_t t0_;     // time at which tester called Begin()
  common::time_t start_;  // time at which the Config constructor was called
  int max_index_{0};
  std::vector<std::string> apply_err_;
  std::vector<apply_channel_ptr> apply_chs_;
  std::vector<boost::fibers::fiber> apply_threads_;

  static constexpr int DEFAULT_NUM_WORKER = 4;
};

}  // namespace kv::raft