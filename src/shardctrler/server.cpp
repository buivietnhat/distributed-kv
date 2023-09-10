#include "shardctrler/server.h"

#include <chrono>
#include <unordered_set>

#include "common/exception.h"

using namespace std::chrono_literals;

namespace kv::shardctrler {

ShardCtrler::ShardCtrler(std::vector<network::ClientEnd *> servers, int me,
                         std::shared_ptr<storage::PersistentInterface> persister)
    : me_(me) {
  configs_.push_back({});

  apply_ch_ = std::make_shared<common::ConcurrentBlockingQueue<raft::ApplyMsg>>();
  rf_ = std::make_unique<raft::Raft>(std::move(servers), me, persister, apply_ch_);

  lot_ = std::make_unique<LastOpTable>();

  raft_applied_thread_ = std::thread([&] { ListenFromRaft(); });
}

ShardCtrler::~ShardCtrler() {
  if (raft_applied_thread_.joinable()) {
    raft_applied_thread_.join();
  }
}

std::pair<bool, ShardConfig> LastOpTable::HasRequestBeenServed(uint64_t clientid, uint64_t seq) const {
  std::lock_guard l(mu_);

  if (table_.contains(clientid)) {
    auto last_op = table_.at(clientid);
    if (last_op.seq_ == seq) {
      return {true, last_op.config_};
    }
  }

  return {false, {}};
}

void LastOpTable::PopulateLastOpInfo(uint64_t clientid, uint64_t seq, ShardConfig config) {
  auto lo = LastOp{seq, std::move(config)};
  std::lock_guard l(mu_);
  table_[clientid] = std::move(lo);
}

JoinReply ShardCtrler::Join(const JoinArgs &args) {
  JoinReply reply;
  if (!rf_->IsLeader()) {
    reply.wrong_leader_ = true;
    return reply;
  }

  Logger::Debug(kDShardCtr, me_,
                fmt::format("Receive Join request from client {} with joined Server {}", args.uuid_ % kRound,
                            common::KeysToString(args.servers_)));

  // check if the request've already been served
  // to deal with network unreliable
  auto [serverd, config] = lot_->HasRequestBeenServed(args.uuid_, args.seq_number_);
  if (serverd) {
    Logger::Debug(kDDrop, me_,
                  fmt::format("Join request from client {} with seq {} already been served, return ...",
                              args.uuid_ % kRound, args.seq_number_));
    reply.err_ = OK;
    return reply;
  }

  auto op = GenerateJoinOp(me_, args);

  // start a new command for Raft
  auto [index, term, is_leader] = rf_->Start({op});
  if (!is_leader) {
    reply.wrong_leader_ = true;
    return reply;
  }

  // wait for the reply, max 2 seconds
  auto fut = op.promise_->get_future();
  auto status = fut.wait_for(2s);

  if (Killed()) {
    Logger::Debug(kDTrace, me_, "Join: return while waiting for the result");
    reply.wrong_leader_ = true;
    return reply;
  }

  if (status == std::future_status::timeout) {
    Logger::Debug(kDTrace, me_, "Join: Timeout (2s) waiting for the result");
    reply.wrong_leader_ = true;
    return reply;
  }

  if (status == std::future_status::ready) {
    reply.err_ = OK;
    return reply;
  }

  throw SHARDCTRLER_EXCEPTION("unreachable: should not be here");
}

LeaveReply ShardCtrler::Leave(const LeaveArgs &args) {
  LeaveReply reply;
  if (!rf_->IsLeader()) {
    reply.wrong_leader_ = true;
    return reply;
  }

  Logger::Debug(kDShardCtr, me_,
                fmt::format("Receive Leave request from client {} with leaved Gids {}", args.uuid_ % kRound,
                            common::ToString(args.gids_)));

  // check if the request've already been served
  // to deal with network unreliable
  auto [serverd, config] = lot_->HasRequestBeenServed(args.uuid_, args.seq_number_);
  if (serverd) {
    Logger::Debug(kDDrop, me_,
                  fmt::format("Leave request from client {} with seq {} already been served, return ...",
                              args.uuid_ % kRound, args.seq_number_));
    reply.err_ = OK;
    return reply;
  }

  auto op = GenerateLeaveOp(me_, args);

  // start a new command for Raft
  auto [index, term, is_leader] = rf_->Start({op});
  if (!is_leader) {
    reply.wrong_leader_ = true;
    return reply;
  }

  // wait for the reply, max 2 seconds
  auto fut = op.promise_->get_future();
  auto status = fut.wait_for(2s);

  if (Killed()) {
    Logger::Debug(kDTrace, me_, "Leave: return while waiting for the result");
    reply.wrong_leader_ = true;
    return reply;
  }

  if (status == std::future_status::timeout) {
    Logger::Debug(kDTrace, me_, "Leave: Timeout (2s) waiting for the result");
    reply.wrong_leader_ = true;
    return reply;
  }

  if (status == std::future_status::ready) {
    reply.err_ = OK;
    return reply;
  }

  throw SHARDCTRLER_EXCEPTION("unreachable: should not be here");
}

MoveReply ShardCtrler::Move(const MoveArgs &args) {
  MoveReply reply;
  if (!rf_->IsLeader()) {
    reply.wrong_leader_ = true;
    return reply;
  }

  Logger::Debug(kDShardCtr, me_,
                fmt::format("Receive Move request from client {} with shard {} will be served by Gid {}",
                            args.uuid_ % kRound, args.shard_, args.gid_));

  // check if the request've already been served
  // to deal with network unreliable
  auto [serverd, config] = lot_->HasRequestBeenServed(args.uuid_, args.seq_number_);
  if (serverd) {
    Logger::Debug(kDDrop, me_,
                  fmt::format("Move request from client {} with seq {} already been served, return ...",
                              args.uuid_ % kRound, args.seq_number_));
    reply.err_ = OK;
    return reply;
  }

  auto op = GenerateMoveOp(me_, args);

  // start a new command for Raft
  auto [index, term, is_leader] = rf_->Start({op});
  if (!is_leader) {
    reply.wrong_leader_ = true;
    return reply;
  }

  // wait for the reply, max 2 seconds
  auto fut = op.promise_->get_future();
  auto status = fut.wait_for(2s);

  if (Killed()) {
    Logger::Debug(kDTrace, me_, "Move: return while waiting for the result");
    reply.wrong_leader_ = true;
    return reply;
  }

  if (status == std::future_status::timeout) {
    Logger::Debug(kDTrace, me_, "Move: Timeout (2s) waiting for the result");
    reply.wrong_leader_ = true;
    return reply;
  }

  if (status == std::future_status::ready) {
    reply.err_ = OK;
    return reply;
  }

  throw SHARDCTRLER_EXCEPTION("unreachable: should not be here");
}

QueryReply ShardCtrler::Query(const QueryArgs &args) {
  QueryReply reply;
  if (!rf_->IsLeader()) {
    reply.wrong_leader_ = true;
    return reply;
  }

  Logger::Debug(
      kDShardCtr, me_,
      fmt::format("Receive Query request from client {} with config number {}", args.uuid_ % kRound, args.num_));

  // check if the request've already been served
  // to deal with network unreliable
  auto [serverd, config] = lot_->HasRequestBeenServed(args.uuid_, args.seq_number_);
  if (serverd) {
    Logger::Debug(kDDrop, me_,
                  fmt::format("Query request from client {} with seq {} already been served, return ...",
                              args.uuid_ % kRound, args.seq_number_));
    reply.err_ = OK;
    return reply;
  }

  auto op = GenerateQueryOp(me_, args);

  // start a new command for Raft
  auto [index, term, is_leader] = rf_->Start({op});
  if (!is_leader) {
    reply.wrong_leader_ = true;
    return reply;
  }

  // wait for the reply, max 2 seconds
  auto fut = op.promise_->get_future();
  auto status = fut.wait_for(2s);

  if (Killed()) {
    Logger::Debug(kDTrace, me_, "Query: return while waiting for the result");
    reply.wrong_leader_ = true;
    return reply;
  }

  if (status == std::future_status::timeout) {
    Logger::Debug(kDTrace, me_, "Query: Timeout (2s) waiting for the result");
    reply.wrong_leader_ = true;
    return reply;
  }

  if (status == std::future_status::ready) {
    reply.err_ = OK;
    reply.config_ = fut.get();
    return reply;
  }

  throw SHARDCTRLER_EXCEPTION("unreachable: should not be here");
}

void ShardCtrler::ListenFromRaft() {
  while (!Killed()) {
    raft::ApplyMsg m;
    apply_ch_->Dequeue(&m);
    InstallRaftAppliedMsg(m);
  }

  // dry all the cmd msgs
  while (!apply_ch_->Empty()) {
    raft::ApplyMsg m;
    apply_ch_->Dequeue(&m);
    InstallRaftAppliedMsg(m);
  }
}

void ShardCtrler::InstallRaftAppliedMsg(const raft::ApplyMsg &m) {
  if (m.command_valid_) {
    if (m.command_index_ != last_applied_ + 1) {
      throw SHARDCTRLER_EXCEPTION(
          fmt::format("Raft applies out of order, lastApplied {}, cmdIndex {}", last_applied_, m.command_index_));
    }

    try {
      auto cmd = std::any_cast<Op>(m.command_);
      InstallCmdMsg(cmd);
    } catch (const std::bad_any_cast &e) {
      Logger::Debug(kDError, me_, "Exception throw trying to cast the cmd msg");
    }
    last_applied_ = m.command_index_;
  } else {
    // ignore
  }
}

void ShardCtrler::InstallCmdMsg(const Op &cmd) {
  ShardConfig config;
  // maybe the client submits the same request multiple times in a short amount of time
  // need to check if I've already served the request
  if (auto [served, _] = lot_->HasRequestBeenServed(cmd.client_id_, cmd.seq_number_); served) {
    Logger::Debug(kDDrop, me_,
                  fmt::format("InstallCmdMsg: client %d with seq %d has already been served, return ...",
                              cmd.client_id_ % kRound, cmd.seq_number_));
    if (cmd.sender_ == me_) {
      cmd.promise_->set_value(std::move(config));
    }
    return;
  }

  auto cfg = GenerateNewConfig(cmd);

  if (cmd.type_ != OpType::QUERY) {
    configs_.push_back(std::move(cfg));
  }

  lot_->PopulateLastOpInfo(cmd.client_id_, cmd.seq_number_, cfg);

  if (cmd.sender_ == me_) {
    try {
      cmd.promise_->set_value(std::move(cfg));
    } catch (...) {
      // ok to just ignore it, since the exception will occur if we try to set value twice (why replay the log)
    }

  }
}

ShardConfig ShardCtrler::GenerateNewConfig(const Op &cmd) {
  switch (cmd.type_) {
    case OpType::JOIN:
      return GenerateNewJoinConfig(cmd);
    case OpType::LEAVE:
      return GenerateNewLeaveConfig(cmd);
    case OpType::MOVE:
      return GenerateNewMoveConfig(cmd);
    case OpType::QUERY:
      return GenerateNewQueryConfig(cmd);
    default:
      throw SHARDCTRLER_EXCEPTION("invalid cmd type");
  }
}

ShardConfig ShardCtrler::GenerateNewJoinConfig(const Op &cmd) {
  auto cfg = configs_.back();

  std::vector<int> available_shards;
  std::unordered_set<int> revoked_gids;

  // if there is no group served for now
  if (cfg.groups_.empty()) {
    for (int i = 0; i < kNShards; i++) {
      available_shards.push_back(i);
    }
    revoked_gids.insert(0);
  }

  // then add newly added group of servers
  for (const auto &[gid, servers] : cmd.servers_) {
    if (cfg.groups_.contains(gid)) {
      throw SHARDCTRLER_EXCEPTION(fmt::format("Group {} already existed in Config num %d", gid, cfg.num_));
    }
    cfg.groups_[gid] = servers;
  }

  // balance the shards
  cfg.BalanceShards(available_shards, revoked_gids);

  if (cmd.sender_ == me_) {
    Logger::Debug(
        kDShardCtr, me_,
        fmt::format("Config will change {} to {} after Join request: {} --> {}, group {}", cfg.num_, cfg.num_ + 1,
                    configs_.back().ShardsToString(), cfg.ShardsToString(), common::KeysToString(cfg.groups_)));
  }

  // assign a new configuration number
  cfg.num_ += 1;

  return cfg;
}

ShardConfig ShardCtrler::GenerateNewLeaveConfig(const Op &cmd) {
  auto cfg = configs_.back();

  // revoke the shards served by leaving GID, also remove the group
  std::vector<int> revoked_shards;
  std::unordered_set<int> revoked_gids;
  for (auto gid : cmd.gids_) {
    auto shards = cfg.ShardsServedBy(gid);
    revoked_shards.insert(revoked_shards.end(), shards.begin(), shards.end());
    cfg.RemoveGroup(gid);
    revoked_gids.insert(gid);
  }

  // balance the shards
  cfg.BalanceShards(revoked_shards, revoked_gids);

  if (cmd.sender_ == me_) {
    Logger::Debug(kDShardCtr, me_,
                  fmt::format("Config will change {} to {} after Leave request: {} --> {}", cfg.num_, cfg.num_ + 1,
                              configs_.back().ShardsToString(), cfg.ShardsToString()));
  }

  // assign a new configuration number
  cfg.num_ += 1;

  return cfg;
}

ShardConfig ShardCtrler::GenerateNewMoveConfig(const Op &cmd) {
  auto cfg = configs_.back();

  // just simply reassign the serve info
  cfg.shards_[cmd.shard_] = cmd.gid_;

  if (cmd.sender_ == me_) {
    Logger::Debug(
        kDShardCtr, me_,
        fmt::format("Config will change {} to {} after Move request: {} --> {}, group {}", cfg.num_, cfg.num_ + 1,
                    configs_.back().ShardsToString(), cfg.ShardsToString(), common::KeysToString(cfg.groups_)));
  }

  // assign a new configuration number
  cfg.num_ += 1;

  return cfg;
}

ShardConfig ShardCtrler::GenerateNewQueryConfig(const Op &cmd) {
  ShardConfig cfg;
  if (cmd.num_ == -1 || cmd.num_ >= static_cast<int>(configs_.size())) {
    cfg = configs_.back();
  } else {
    cfg = configs_[cmd.num_];
  }

  if (cmd.sender_ == me_) {
    Logger::Debug(kDShardCtr, me_,
                  fmt::format("New config num {}: {} return with Num {}, group {}", cfg.num_, cfg.ShardsToString(),
                              cmd.num_, common::KeysToString(cfg.groups_)));
  }
  return cfg;
}

}  // namespace kv::shardctrler
