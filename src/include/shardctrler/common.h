#pragma once

#include <array>
#include <future>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <boost/fiber/all.hpp>

#include "common/logger.h"
#include "nlohmann/json.hpp"

namespace kv::shardctrler {

using common::Logger;

// the number of shards
static constexpr int kNShards = 10;
static constexpr int kRound = 10000;

struct ShardConfig {
  int num_{0};                                                // config number
  std::array<int, kNShards> shards_;                          // shard -> gid
  std::unordered_map<int, std::vector<std::string>> groups_;  // gid -> servers[]

  void BalanceShards(const std::vector<int> &revoked_shards, const std::unordered_set<int> &revoked_gids);

  void UnserveAll();

  void RemoveGroup(int gid);

  std::vector<int> ShardsServedBy(int gid) const;

  inline std::string ShardsToString() const {
    std::stringstream ss;
    ss << "[ ";
    for (unsigned long i = 0; i < kNShards; i++) {
      ss << "{" << i << ":" << shards_[i] << "} ";
    }
    ss << "]";
    return ss.str();
  }
};

enum class Err : uint8_t { RESERVED, OK };

enum class OpType : uint8_t { RESERVED, JOIN, LEAVE, MOVE, QUERY };

struct JoinArgs {
  std::unordered_map<int, std::vector<std::string>> servers_;  // new GID -> servers mappings
  uint64_t uuid_;
  uint64_t seq_number_;
};

struct JoinReply {
  bool wrong_leader_;
  Err err_;
};

struct LeaveArgs {
  std::vector<int> gids_;
  uint64_t uuid_;
  uint64_t seq_number_;
};

struct LeaveReply {
  bool wrong_leader_;
  Err err_;
};

struct MoveArgs {
  int shard_;
  int gid_;
  uint64_t uuid_;
  uint64_t seq_number_;
};

struct MoveReply {
  bool wrong_leader_;
  Err err_;
};

struct QueryArgs {
  int num_;  // desired config number
  uint64_t uuid_;
  uint64_t seq_number_;
};

struct QueryReply {
  bool wrong_leader_;
  Err err_;
  ShardConfig config_;
};

struct LastOp {
  uint64_t seq_;
  ShardConfig config_;
};

struct Op {
  int sender_{-1};
  OpType type_;
  uint64_t client_id_;
  uint64_t seq_number_;
  std::unordered_map<int, std::vector<std::string>> servers_;  // for join cmd
  std::vector<int> gids_;                                      // for leave cmd
  int shard_;                                                  // for move cmd
  int gid_;                                                    // for move cmd
  int num_;                                                    // for query cmd
  std::shared_ptr<bool> p_has_value_;
  std::shared_ptr<boost::fibers::promise<ShardConfig>> promise_;
};

Op GenerateJoinOp(int sender, const JoinArgs &args);

Op GenerateLeaveOp(int sender, const LeaveArgs &args);

Op GenerateMoveOp(int sender, const MoveArgs &args);

Op GenerateQueryOp(int sender, const QueryArgs &args);

inline void to_json(nlohmann::json &j, const ShardConfig &cfg) {
  j = nlohmann::json{{"num", cfg.num_}, {"shard", cfg.shards_}, {"group", cfg.groups_}};
}

inline void from_json(const nlohmann::json &j, ShardConfig &cfg) {
  j.at("num").get_to(cfg.num_);
  j.at("shard").get_to(cfg.shards_);
  j.at("group").get_to(cfg.groups_);
}

}  // namespace kv::shardctrler
