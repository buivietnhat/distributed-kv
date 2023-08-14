#include "network/grpc_client.h"

#include "common/exception.h"

namespace kv::network {

raft::RequestVoteReply GrpcClient::DoRequestVote(const kv::raft::RequestVoteArgs &args) const {
  throw NOT_IMPLEMENTED_EXCEPTION("not implemented yet!!");
}
raft::AppendEntryReply GrpcClient::DoAppendEntries(const kv::raft::AppendEntryArgs &args) const {
  throw NOT_IMPLEMENTED_EXCEPTION("not implemented yet!!");
}

}  // namespace kv::network
