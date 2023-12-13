#include "shardkv/common.h"
#include "shardkv/server.h"

namespace kv::shardkv {

std::optional<Reply> ShardKV::HasRequestBeenServed(int shard, uint64_t clientid, uint64_t seq) {
  std::lock_guard l(lot_.mu_);

  if (lot_.table_.contains(shard) && lot_.table_[shard].contains(clientid) &&
      lot_.table_[shard][clientid].seq_ == seq) {
    return lot_.table_[shard][clientid].rep_;
  }

  return {};
}

void ShardKV::PopulateLastOpInfo(int shard, uint64_t clientid, uint64_t seq, Reply r) {
  LastOp op;
  op.seq_ = seq;
  op.rep_ = r;

  std::lock_guard l(lot_.mu_);

  lot_.table_[shard][clientid] = op;
}

void ShardKV::RemoveShard(int shard, int config_num) {
  if (IsServingThisShard(shard) && config_num != CurrentConfig() + 1) {
    Logger::Debug1(
        kDError, me_, gid_,
        fmt::format("Error Remove shard {} info, configNum {}, currentConfig {}", shard, config_num, CurrentConfig()));
    throw SHARDKV_EXCEPTION("invalid remove shard request");
  }

  std::lock_guard l(mu_);
  shards_removed_.insert(shard);
  UnserveOne(shard);
}

bool ShardKV::CheckForAlreadyInstalledShards() {
  if (shards_installed_.empty()) {
    return false;
  }

  std::vector<int> shard_to_erase;
  for (const auto &shard : pending_shards_) {
    if (shards_installed_.contains(shard)) {
      Logger::Debug1(kDTrace, me_, gid_,
                     fmt::format("Remove shard {} out of pending list since I already installed it", shard));
      shard_to_erase.push_back(shard);
    }
  }

  for (auto shard : shard_to_erase) {
    pending_shards_.erase(shard);
  }

  return pending_shards_.empty();
}

Op ShardKV::GenerateGetOp(const GetArgs &args, int config_num) {
  Op op;
  op.op_type_ = OpType::GET;
  op.client_id_ = args.uuid_;
  op.seq_number_ = args.seq_number_;
  op.key_ = args.key_;
  op.config_num_ = config_num;
  op.sender_ = me_;

  return op;
}

Op ShardKV::GeneratePutAppendOp(const PutAppendArgs &args, OpType op_type, int config_num) {
  Op op;
  op.op_type_ = op_type;
  op.client_id_ = args.uuid_;
  op.seq_number_ = args.seq_number_;
  op.key_ = args.key_;
  op.value_ = args.value_;
  op.config_num_ = config_num;
  op.sender_ = me_;

  return op;
}

Op ShardKV::GenerateInstallConfigOp(const shardctrler::ShardConfig &cfg) {
  Op op;
  op.op_type_ = OpType::INSTALL_CONFIG;
  op.config_ = cfg;
  op.sender_ = me_;

  return op;
}

Op ShardKV::GenerateInstallShardOp(const InstallShardArgs &args) {
  Op op;
  op.op_type_ = OpType::INSTALL_SHARD;
  op.shard_ = args.shard_;
  op.data_ = args.data_;
  op.client_id_ = args.uuid_;
  op.seq_number_ = args.seq_number_;
  op.sender_ = me_;

  return op;
}

Op ShardKV::GenerateRemoveShardOp(int shard, int config_num) {
  Op op;
  op.op_type_ = OpType::REMOVE_SHARD;
  op.shard_ = shard;
  op.config_num_ = config_num;
  op.sender_ = me_;

  return op;
}

GetReply ShardKV::Get(const GetArgs &args) {
  GetReply reply;
  if (!rf_->IsLeader()) {
    reply.err_ = Err::ERR_WRONG_LEADER;
    return reply;
  }

  Logger::Debug1(kDServ, me_, gid_,
                 fmt::format("Receive Get request from client {} seq {} for key {}", args.uuid_ % 10000,
                             args.seq_number_, args.key_));

  // check if the request've already been served
  // to deal with network unreliable
  auto rep = HasRequestBeenServed(KeyToShard(args.key_), args.uuid_, args.seq_number_);
  if (rep) {
    Logger::Debug1(kDDrop, me_, gid_,
                   fmt::format("Get request from client {} with seq {} already been served, return ...",
                               args.uuid_ % 10000, args.seq_number_));
    reply.err_ = Err::OK;
    reply.value_ = rep->val_;
    return reply;
  }

  auto op = GenerateGetOp(args, CurrentConfig());

  std::unique_lock lock(mu_);
  // check if I am in charge of this shard
  if (!IsServingThisKeyUnlocked(args.key_)) {
    auto config_num = 0;
    if (config_ != nullptr) {
      config_num = config_->num_;
    }
    Logger::Debug1(kDDrop, me_, gid_,
                   fmt::format("key {} is belong to shard {}, not my responsibility {} with config num {}", args.key_,
                               KeyToShard(args.key_), common::ToString(serving_shards_), config_num));
    reply.err_ = Err::ERR_WRONG_GROUP;
    return reply;
  }

  // start a new operation to Raft
  auto [index, _, is_leader] = rf_->Start(op);
  Logger::Debug1(kDLeader1, me_, gid_,
                 fmt::format("Start new cmd index {} for Get client {} seq {} for key {} val ", index,
                             args.uuid_ % 10000, args.seq_number_, args.key_));
  if (!is_leader) {
    Logger::Debug1(kDDrop, gid_, me_, "I am not a leader, return ...");
    reply.err_ = Err::ERR_WRONG_LEADER;
    return reply;
  }

  SetOutstandingOpIdx(index);
  lock.unlock();

  // wait for the reply, max 2 seconds
  auto fut = op.promise_.p_->get_future();
  auto status = fut.wait_for(2s);

  if (Killed()) {
    Logger::Debug1(kDTrace, me_, gid_, "Get: return while waiting for the result");
    reply.err_ = Err::ERR_WRONG_LEADER;
    return reply;
  }

  if (status == std::future_status::timeout) {
    Logger::Debug1(kDTrace, me_, gid_, "Get: Timeout (2s) waiting for the result");
    reply.err_ = Err::ERR_WRONG_LEADER;
    return reply;
  }

  if (status == std::future_status::ready) {
    auto r = fut.get();
    if (r.wrong_group_) {
      reply.err_ = Err::ERR_WRONG_GROUP;
    } else {
      reply.err_ = Err::OK;
      reply.value_ = r.val_;
    }
    return reply;
  }

  throw SHARDKV_EXCEPTION("unreachable: should not be here");
}

PutAppendReply ShardKV::PutAppend(const PutAppendArgs &args) {
  PutAppendReply reply;
  if (!rf_->IsLeader()) {
    reply.err_ = Err::ERR_WRONG_LEADER;
    return reply;
  }

  Logger::Debug1(kDServ, me_, gid_,
                 fmt::format("Receive {} request from client {} seq {} for key {}", ToString(args.op_),
                             args.uuid_ % 10000, args.seq_number_, args.key_));

  // check if the request've already been served
  // to deal with network unreliable
  auto rep = HasRequestBeenServed(KeyToShard(args.key_), args.uuid_, args.seq_number_);
  if (rep) {
    Logger::Debug1(kDDrop, me_, gid_,
                   fmt::format("{} request from client {} with seq {} already been served, return ...",
                               ToString(args.op_), args.uuid_ % 10000, args.seq_number_));
    reply.err_ = Err::OK;
    return reply;
  }

  auto op = GeneratePutAppendOp(args, args.op_, CurrentConfig());

  std::unique_lock lock(mu_);
  // check if I am in charge of this shard
  if (!IsServingThisKeyUnlocked(args.key_)) {
    auto config_num = 0;
    if (config_ != nullptr) {
      config_num = config_->num_;
    }
    Logger::Debug1(kDDrop, me_, gid_,
                   fmt::format("key {} is belong to shard {}, not my responsibility {} with config num {}", args.key_,
                               KeyToShard(args.key_), common::ToString(serving_shards_), config_num));
    reply.err_ = Err::ERR_WRONG_GROUP;
    return reply;
  }

  // start a new operation to Raft
  auto [index, _, is_leader] = rf_->Start(op);
  Logger::Debug1(kDLeader1, me_, gid_,
                 fmt::format("Start new cmd index {} for {} client {} seq {} for key {} val {}", index,
                             ToString(args.op_), args.uuid_ % 10000, args.seq_number_, args.key_, args.value_));
  if (!is_leader) {
    Logger::Debug1(kDDrop, gid_, me_, "I am not a leader, return ...");
    reply.err_ = Err::ERR_WRONG_LEADER;
    return reply;
  }

  SetOutstandingOpIdx(index);
  lock.unlock();

  // wait for the reply, max 2 seconds
  auto fut = op.promise_.p_->get_future();
  auto status = fut.wait_for(2s);

  if (Killed()) {
    Logger::Debug1(kDTrace, me_, gid_, "Put/Append: return while waiting for the result");
    reply.err_ = Err::ERR_WRONG_LEADER;
    return reply;
  }

  if (status == std::future_status::timeout) {
    Logger::Debug1(kDTrace, me_, gid_, "Put/Append: Timeout (2s) waiting for the result");
    reply.err_ = Err::ERR_WRONG_LEADER;
    return reply;
  }

  if (status == std::future_status::ready) {
    auto r = fut.get();
    if (r.wrong_group_) {
      reply.err_ = Err::ERR_WRONG_GROUP;
    } else {
      reply.err_ = Err::OK;
    }
    return reply;
  }

  throw SHARDKV_EXCEPTION("unreachable: should not be here");
}

InstallShardReply ShardKV::InstallShard(const InstallShardArgs &args) {
  InstallShardReply reply;
  if (!rf_->IsLeader()) {
    reply.err_ = Err::ERR_WRONG_LEADER;
    return reply;
  }

  Logger::Debug1(kDServ, me_, gid_,
                 fmt::format("Receive request to InstallShard {} from Group {} for config Num {}", args.shard_,
                             args.gid_, args.cfg_num_));

  // check if the request've already been served
  // to deal with network unreliable
  auto rep = HasRequestBeenServed(args.shard_, args.uuid_, args.seq_number_);
  if (rep) {
    Logger::Debug1(kDDrop, me_, gid_,
                   fmt::format("InstallShard request from client {} with seq {} already been served, return ...",
                               args.uuid_ % 10000, args.seq_number_));
    reply.err_ = Err::OK;
    return reply;
  }

  // if it's a request for already-installed config, just return true
  if (args.cfg_num_ <= CurrentConfig()) {
    Logger::Debug1(kDTrace, me_, gid_,
                   fmt::format("I am now operating with config Num {} >= request Num ({}), just return true",
                               CurrentConfig(), args.cfg_num_));
    reply.err_ = Err::OK;
    return reply;
  }

  // check if I am in charge of this shard
  if (!IsPendingThisShard(args.shard_)) {
    Logger::Debug1(
        kDDrop, me_, gid_,
        fmt::format("I am not pending for this shard {}, my pending list {} for config Num {}, return", args.shard_,
                    common::ToString(pending_shards_), CurrentConfig()));  // maybe race, but it's ok
    reply.err_ = Err::ERR_WRONG_GROUP;
    return reply;
  }

  // start a new operation to Raft
  auto op = GenerateInstallShardOp(args);
  auto [index, _, is_leader] = rf_->Start(op);
  if (!is_leader) {
    Logger::Debug1(kDDrop, gid_, me_, "I am not a leader, return ...");
    reply.err_ = Err::ERR_WRONG_LEADER;
    return reply;
  }

  // wait for the reply, max 2 seconds
  auto fut = op.promise_.p_->get_future();
  auto status = fut.wait_for(2s);

  if (Killed()) {
    Logger::Debug1(kDTrace, me_, gid_, "InstallShard: return while waiting for the result");
    reply.err_ = Err::ERR_WRONG_LEADER;
    return reply;
  }

  if (status == std::future_status::timeout) {
    Logger::Debug1(kDTrace, me_, gid_, "InstallShard: Timeout (2s) waiting for the result");
    reply.err_ = Err::ERR_WRONG_LEADER;
    return reply;
  }

  if (status == std::future_status::ready) {
    reply.err_ = Err::OK;
    return reply;
  }

  throw SHARDKV_EXCEPTION("unreachable: should not be here");
}

}  // namespace kv::shardkv