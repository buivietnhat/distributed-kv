#pragma once

#include <tbb/task_group.h>

#include <iostream>
#include <mutex>
#include <vector>

#include "common/container/concurrent_blocking_queue.h"
#include "raft/log_manager.h"
#include "raft/voter.h"

namespace kv::network {
class ClientEnd;
}

namespace kv::storage {
class PersistentInterface;
}

namespace kv::raft {

struct RaftPersistState {};

struct RaftState {
  int term_;
  bool is_leader_;
};

struct ApplyMsg {};

struct Snapshot {
  std::vector<std::byte> data;
  bool Empty() const { return data.empty(); }
};

struct AppendEntryArgs {
  bool hearbeat_;
  bool commit_;
  int leader_id_;
  int leader_term_;
  int prev_log_idx_;
  int prev_log_term_;
  int leader_commit_idx_;
  std::vector<LogEntry> entries_;
};

struct AppendEntryReply {
  int term_;  // the conflicting term
  bool success_;
  int xindex_;  // index of the first entry of the conflicting term
  int xlen_;    // length of the follower's log
};

enum class Role : uint8_t { RESERVED, LEADER, FOLLOWER, CANDIDATE };

inline std::string ToString(Role role) {
  switch (role) {
    case Role::LEADER:
      return "Leader";
    case Role::FOLLOWER:
      return "Follower";
    case Role::CANDIDATE:
      return "Candidate";
    default:
      return "";
  }
}

struct InternalState {
  int last_log_index_;
  int last_log_term_;
  int term_;
  Role role;
};

class Raft {
 public:
  using enum Role;

  Raft() = default;

  Raft(std::vector<network::ClientEnd *> peers, uint32_t me, storage::PersistentInterface *persister,
       std::shared_ptr<common::ConcurrentBlockingQueue<ApplyMsg>> apply_channel);

  RequestVoteReply RequestVote(const RequestVoteArgs &args);

  AppendEntryReply AppendEntries(const AppendEntryArgs &args);

  RaftState GetState() const;

  int Test(int input) {
    return input + 100;
  }

  inline void Kill() {
    dead_ = true;
  }

  ~Raft();

 private:
  void Persist(std::vector<std::byte> snapshot = {}) const;

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

  std::optional<AppendEntryReply> RequestAppendEntries(int server, const AppendEntryArgs &args) const;

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

  tbb::task_group group_;
  std::thread tickert_;
  std::thread hbt_;
  std::thread ldwlt_;

  static constexpr int NUM_THREAD = 5;
};

}  // namespace kv::raft
