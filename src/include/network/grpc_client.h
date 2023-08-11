#pragma once

#include "common/macros.h"
#include "network/rpc_interface.h"

namespace kv::network {

class GrpcClient : public RPCInterface {
 public:
  GrpcClient() = default;

  DISALLOW_COPY_AND_MOVE(GrpcClient);

 private:
  raft::RequestVoteReply DoRequestVote(const raft::RequestVoteArgs &args) const override;
  raft::AppendEntryReply DoAppendEntries(const raft::AppendEntryArgs &args) const override;
};

}  // namespace kv::network
