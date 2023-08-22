#pragma once

#include <tbb/task_group.h>

#include <iostream>
#include <mutex>
#include <vector>

#include "common/container/concurrent_blocking_queue.h"
#include "common/thread_pool.h"
#include "common/thread_registry.h"
#include "raft/common.h"
#include "raft/log_manager.h"
#include "raft/voter.h"
#include "network/rpc_interface.h"
#include "storage/persistent_interface.h"

namespace kv::raft {

class Raft {
 public:
  using enum Role;

  Raft() = default;

  Raft(std::vector<network::ClientEnd *> peers, uint32_t me, storage::PersistentInterface *persister,
       std::shared_ptr<common::ConcurrentBlockingQueue<ApplyMsg>> apply_channel);

  RequestVoteReply RequestVote(const RequestVoteArgs &args);

  AppendEntryReply AppendEntries(const AppendEntryArgs &args);

  RaftState GetState() const;

  std::tuple<int, int, bool> Start(std::any command);

  void DoSnapshot(int index, const Snapshot &snap);

  std::optional<InstallSnapshotReply> RequestInstallSnapshot(int server, const InstallSnapshotArgs &args) const;

  InstallSnapshotReply InstallSnapshot(const InstallSnapshotArgs &args);

  int Test(int input) { return input + 100; }

  inline void Kill() {
    lm_->Kill();
    dead_ = true;
  }

  ~Raft();

 private:
  void RequestCommit(int server, int index, int prev_log_term);

  void RequestCommits(const std::vector<int> &server_list, int index, int start_idx);

  void RequestAppendEntries(const std::vector<int> &replica_list, int start_idx);

  std::pair<std::vector<int>, int> AnalyseToCommit(const std::unordered_map<int, int> &matched_result, int server);

  int ComputePreviousIndexes(int old_pre_index, const AppendEntryReply &reply) const;

  AppendEntriesResult RequestAppendEntry(int server, int prev_log_idx, int prev_log_term, bool commit, int commit_idx);

  std::pair<std::vector<int>, int> NeedToRequestAppend() const;

  void Persist(const Snapshot &snapshot = {}) const;

  void SendLatestSnapshot(int server);

  void ReadPersistState(const RaftPersistState &state);

  void ReadPersistSnap(const Snapshot &snap);

  void SendSnapshot(int server, int last_included_index, int last_included_term, int leader_term,
                    std::shared_ptr<Snapshot> snapshot);

  void SendSnapshots(const std::vector<int> &replica_list, int last_included_index, const Snapshot &snapshot);

  void CheckAndSendInstallSnapshot(int last_included_index, const Snapshot &snapshot);

  inline void InitMetaDataForLeader() {
    auto last_log_idx = lm_->GetLastLogIdx();
    for (uint32_t s = 0; s < peers_.size(); s++) {
      if (s != me_) {
        next_index_[s] = last_log_idx + 1;
        tentative_next_index_[s] = last_log_idx + 1;
        match_index_[s] = 0;
      }
    }
  }

  inline bool Killed() const { return dead_; }

  inline bool IsLeaderOutdate(int my_time, int leader_term) const { return my_time > leader_term; }

  inline bool IsLeader() const {
    std::lock_guard lock(mu_);
    return role_ == LEADER;
  }

  void TransitionToFollower(int new_term);
  void TransitionToCandidate();
  void TransitionToLeader();

  void AttemptElection();

  bool CheckOutdateAndTransitionToFollower(int current_term, int new_term);

  void SendHeartBeat(int server, int term);

  void BroadcastHeartBeats();

  void LeaderWorkLoop();

  void Ticker();

  std::optional<AppendEntryReply> DoRequestAppendEntry(int server, const AppendEntryArgs &args) const;

  std::shared_ptr<InternalState> CaptureCurrentState() const;

  mutable std::mutex mu_;
  std::vector<network::ClientEnd *> peers_;
  [[maybe_unused]] storage::PersistentInterface *persister_;
  uint32_t me_;
  bool dead_{false};

  Role role_{FOLLOWER};
  int term_{0};

  std::unordered_map<int, int> next_index_;
  std::unordered_map<int, int> tentative_next_index_;
  std::unordered_map<int, int> tentative_cmit_index_;
  std::unordered_map<int, int> match_index_;

  std::unique_ptr<Voter> voter_;
  std::unique_ptr<LogManager> lm_;

  std::thread tickert_;
  std::thread hbt_;
  std::thread ldwlt_;

  common::ThreadRegistry thread_registry_;

  static constexpr int NUM_THREAD = 5;
};

}  // namespace kv::raft
