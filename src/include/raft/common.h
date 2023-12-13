#pragma once

#include <any>
#include <string>
#include <vector>

#include "common/container/concurrent_blocking_queue.h"
#include "common/logger.h"

namespace kv::raft {

using common::Logger;

struct LogEntry {
  int term_;
  std::any command_;
};

struct RaftPersistState {
  int term_{0};
  int voted_for_{0};
  int log_start_idx_{0};
  int last_included_idx_{0};
  int last_included_term_{0};
  std::vector<LogEntry> logs_;

  size_t Size() const {
    // the first entry is just a fake one
    if (logs_.size() <= 1) {
      return 0;
    }

    return logs_.size() - 1;
  }
};

struct RaftState {
  int term_;
  bool is_leader_;
};

struct Snapshot {
  std::string data_;
  bool Empty() const { return data_.empty(); }
  size_t Size() const { return data_.size(); }
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

struct AppendEntriesResult {
  int last_log_idx_;  // index that the server is replicated upto
  int server_;
};

struct ApplyMsg {
  bool command_valid_{false};
  std::any command_;
  int command_index_{0};

  bool snapshot_valid_{false};
  Snapshot snapshot_;
  int snapshot_term_{0};
  int snapshot_index_{0};
};

struct RequestVoteArgs {
  int term_;
  int candidate_;
  int last_log_index_;
  int last_log_term_;
};

struct RequestVoteReply {
  int term_;
  bool vote_granted_;
};

struct VoteResult {
  int server_;
  int term_;
  bool vote_granted_;
};

enum class Role : uint8_t { RESERVED, LEADER, FOLLOWER, CANDIDATE };

struct InternalState {
  int last_log_index_;
  int last_log_term_;
  int term_;
  Role role;
};

struct InstallSnapshotArgs {
  int leader_term_{0};
  int leader_id_{0};
  int last_included_index_{0};
  int last_included_term_{0};
  int offset_{0};
  Snapshot data_;
  bool done_{true};
};

struct InstallSnapshotReply {
  int term_{0};
};

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

using apply_ch_t = std::shared_ptr<common::ConcurrentBlockingQueue<ApplyMsg>>;

}  // namespace kv::raft
