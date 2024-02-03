#pragma once

#include <any>
#include <string>
#include <vector>
#include <boost/fiber/all.hpp>

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
  bool hearbeat_{false};
  bool commit_{false};
  int leader_id_{-1};
  int leader_term_{-1};
  int prev_log_idx_{-1};
  int prev_log_term_{-1};
  int leader_commit_idx_{-1};
  std::vector<LogEntry> entries_;
};

struct AppendEntryReply {
  int term_{0};  // the conflicting term
  bool success_{false};
  int xindex_{0};  // index of the first entry of the conflicting term
  int xlen_{0};    // length of the follower's log
};

struct AppendEntriesResult {
  int last_log_idx_{0};  // index that the server is replicated upto
  int server_{-1};
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
  int term_{0};
  int candidate_{-1};
  int last_log_index_{0};
  int last_log_term_{0};
};

struct RequestVoteReply {
  int term_{0};
  bool vote_granted_{false};
};

struct VoteResult {
  int server_{-1};
  int term_{0};
  bool vote_granted_{false};
};

enum class Role : uint8_t { RESERVED, LEADER, FOLLOWER, CANDIDATE };

struct InternalState {
  int last_log_index_{0};
  int last_log_term_{0};
  int term_{0};
  Role role{Role::FOLLOWER};
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


using apply_channel_t = boost::fibers::unbuffered_channel<ApplyMsg>;
using apply_channel_ptr = std::shared_ptr<apply_channel_t>;
using applier_t = std::function<void(int, apply_channel_ptr)>;

}  // namespace kv::raft
