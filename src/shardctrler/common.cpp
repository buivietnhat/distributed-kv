#include "shardctrler/common.h"

#include <map>

#include "common/exception.h"

namespace kv::shardctrler {

Op GenerateJoinOp(int sender, const JoinArgs &args) {
  Op op;
  op.sender_ = sender;
  op.type_ = OpType::JOIN;
  op.client_id_ = args.uuid_;
  op.seq_number_ = args.seq_number_;
  op.servers_ = args.servers_;
  op.promise_ = std::make_shared<std::promise<ShardConfig>>();

  return op;
}

Op GenerateLeaveOp(int sender, const LeaveArgs &args) {
  Op op;
  op.sender_ = sender;
  op.type_ = OpType::LEAVE;
  op.client_id_ = args.uuid_;
  op.seq_number_ = args.seq_number_;
  op.gids_ = args.gids_;
  op.promise_ = std::make_shared<std::promise<ShardConfig>>();

  return op;
}

Op GenerateMoveOp(int sender, const MoveArgs &args) {
  Op op;
  op.sender_ = sender;
  op.type_ = OpType::MOVE;
  op.client_id_ = args.uuid_;
  op.seq_number_ = args.seq_number_;
  op.shard_ = args.shard_;
  op.gid_ = args.gid_;
  op.promise_ = std::make_shared<std::promise<ShardConfig>>();

  return op;
}

Op GenerateQueryOp(int sender, const QueryArgs &args) {
  Op op;
  op.sender_ = sender;
  op.type_ = OpType::QUERY;
  op.client_id_ = args.uuid_;
  op.seq_number_ = args.seq_number_;
  op.num_ = args.num_;
  op.promise_ = std::make_shared<std::promise<ShardConfig>>();

  return op;
}

void ShardConfig::BalanceShards(const std::vector<int> &revoked_shards, const std::unordered_set<int> &revoked_gids) {
  auto num_groups = groups_.size();
  if (num_groups == 0) {
    UnserveAll();
    return;
  }

  uint32_t mean_num_shards = kNShards / num_groups;
  uint32_t remained_shards = kNShards % num_groups;

  std::map<int, std::vector<int>> serving_shards;
  std::vector<int> gids;
  for (const auto &[gid, servers] : groups_) {
    serving_shards[gid] = std::vector<int>();
    gids.push_back(gid);
  }

  for (int i = 0; i < kNShards; i++) {
    if (!revoked_gids.contains(shards_[i])) {
      serving_shards[shards_[i]].push_back(i);
    }
  }

  // first extract out the shards that should be moved into new groups
  std::vector<int> moving_shards;
  moving_shards.insert(moving_shards.end(), revoked_shards.begin(), revoked_shards.end());
  for (auto &[gid, shards] : serving_shards) {
    if (shards.size() > mean_num_shards) {
      if (remained_shards > 0) {
        moving_shards.insert(moving_shards.end(), shards.begin() + mean_num_shards + 1, shards.end());
        shards = std::vector<int>(shards.begin(), shards.begin() + mean_num_shards + 1);
        remained_shards -= 1;
      } else {
        moving_shards.insert(moving_shards.end(), shards.begin() + mean_num_shards, shards.end());
        shards = std::vector<int>(shards.begin(), shards.begin() + mean_num_shards);
      }
    }
  }

  for (auto &[gid, shards] : serving_shards) {
    if (shards.size() <= mean_num_shards) {
      int move_len = 0;
      if (remained_shards > 0) {
        move_len = mean_num_shards - shards.size() + 1;
        remained_shards -= 1;
      } else {
        move_len = mean_num_shards - shards.size();
      }

      auto move = std::vector<int>(moving_shards.begin() + moving_shards.size() - move_len, moving_shards.end());
      moving_shards = std::vector<int>(moving_shards.begin(), moving_shards.begin() + moving_shards.size() - move_len);
      shards.insert(shards.end(), move.begin(), move.end());
    }
  }

  // the remainder shards should be zero
  if (!moving_shards.empty()) {
    throw SHARDCTRLER_EXCEPTION(fmt::format("incorrect balancing algorithm, remain shard = {}", moving_shards.size()));
  }

  // re-assign
  for (const auto &[gid, shards] : serving_shards) {
    for (auto shard : shards) {
      shards_[shard] = gid;
    }
  }
}

void ShardConfig::UnserveAll() {
  for (int i = 0; i < kNShards; i++) {
    shards_[i] = 0;
  }
}

void ShardConfig::RemoveGroup(int gid) {
  groups_.erase(gid);
}

std::vector<int> ShardConfig::ShardsServedBy(int gid) const {
  std::vector<int> shard_list;
  for (int i = 0; i < kNShards; i++) {
    if (shards_[i] == gid) {
      shard_list.push_back(i);
    }
  }
  return shard_list;
}

}  // namespace kv::shardctrler