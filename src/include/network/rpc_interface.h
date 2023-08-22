#pragma once

#include <optional>
#include "raft/common.h"

namespace kv::network {

class ClientEnd {
 public:
  virtual std::optional<raft::RequestVoteReply> RequestVote(const raft::RequestVoteArgs &args) const = 0;

  virtual std::optional<raft::AppendEntryReply> AppendEntries(const raft::AppendEntryArgs &args) const = 0;

  virtual std::optional<raft::InstallSnapshotReply> InstallSnapshot(const raft::InstallSnapshotArgs &args) const = 0;

  virtual std::optional<int> Test(int input) const = 0;

  virtual void Terminate() = 0;

  virtual ~ClientEnd() = default;
};

class RPCInterface {
 public:
  virtual ~RPCInterface() = default;

  raft::RequestVoteReply RequestVote(const raft::RequestVoteArgs &args) const { return DoRequestVote(args); }

  raft::AppendEntryReply AppendEntries(const raft::AppendEntryArgs &args) const { return DoAppendEntries(args); }

 private:
  virtual raft::RequestVoteReply DoRequestVote(const raft::RequestVoteArgs &args) const = 0;
  virtual raft::AppendEntryReply DoAppendEntries(const raft::AppendEntryArgs &args) const = 0;
};

RPCInterface *GetRPCInterface();
void SetRPCInterface(RPCInterface *rpc);

extern RPCInterface *instance;

}  // namespace kv::network