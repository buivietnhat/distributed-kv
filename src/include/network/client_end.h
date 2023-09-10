#pragma once

#include <optional>

#include "raft/common.h"
#include "shardctrler/common.h"

namespace kv::network {

class ClientEnd {
 public:
  virtual std::optional<raft::RequestVoteReply> RequestVote(const raft::RequestVoteArgs &args) const = 0;

  virtual std::optional<raft::AppendEntryReply> AppendEntries(const raft::AppendEntryArgs &args) const = 0;

  virtual std::optional<raft::InstallSnapshotReply> InstallSnapshot(const raft::InstallSnapshotArgs &args) const = 0;

  virtual std::optional<shardctrler::QueryReply> Query(const shardctrler::QueryArgs &args) const = 0;

  virtual std::optional<shardctrler::JoinReply> Join(const shardctrler::JoinArgs &args) const = 0;

  virtual std::optional<shardctrler::LeaveReply> Leave(const shardctrler::LeaveArgs &args) const = 0;

  virtual std::optional<shardctrler::MoveReply> Move(const shardctrler::MoveArgs &args) const = 0;

  virtual std::optional<int> Test(int input) const = 0;

  virtual void Terminate() = 0;

  virtual ~ClientEnd() = default;
};

}  // namespace kv::network