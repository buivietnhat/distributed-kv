#pragma once

#include <future>
#include <string>
#include <unordered_map>

#include "common/logger.h"
#include "nlohmann/json.hpp"
#include "shardctrler/common.h"

using namespace std::chrono_literals;

namespace kv::shardkv {

using common::Logger;

enum class Err : uint8_t { RESERVED, OK, ERR_NO_KEY, ERR_WRONG_GROUP, ERR_WRONG_LEADER };

enum class OpType : uint8_t { PUT, GET, APPEND, INSTALL_CONFIG, INSTALL_SHARD, REMOVE_SHARD, NOOP };

static constexpr int kMod = 10000;

inline std::string ToString(OpType type) {
  switch (type) {
    case OpType::PUT:
      return "PUT";
    case OpType::GET:
      return "GET";
    case OpType::APPEND:
      return "APPEND";
    case OpType::INSTALL_CONFIG:
      return "INSTALL_CONFIG";
    case OpType::INSTALL_SHARD:
      return "INSTALL_SHARD";
    case OpType::REMOVE_SHARD:
      return "REMOVE_SHARD";
    case OpType::NOOP:
      return "NOOP";
  }
  return "";
}

inline int KeyToShard(const std::string &key) {
  int shard = 0;
  if (key.size() > 0) {
    shard = key[0] - '0';
  }
  shard %= shardctrler::kNShards;
  return shard;
}

struct PutAppendArgs {
  std::string key_;
  std::string value_;
  OpType op_;  // Put or Append
  uint64_t uuid_;
  uint64_t seq_number_;
};

struct PutAppendReply {
  Err err_;
};

struct GetArgs {
  std::string key_;
  uint64_t uuid_;
  uint64_t seq_number_;
};

struct GetReply {
  Err err_;
  std::string value_;
};

struct InstallShardArgs {
  int shard_;
  int gid_;  // source gid
  int cfg_num_;
  std::string data_;
  uint64_t uuid_;
  uint64_t seq_number_;
};

struct InstallShardReply {
  Err err_;
};

struct Reply {
  std::string val_;
  bool wrong_group_;
};

struct LastOp {
  uint64_t seq_;
  Reply rep_;
};

template <typename T>
struct Promise {
  std::shared_ptr<bool> p_has_value_;
  std::shared_ptr<boost::fibers::promise<T>> p_;

  inline bool Valid() const { return p_has_value_ != nullptr && *p_has_value_ == false; }

  Promise() {
    p_has_value_ = std::make_shared<bool>(false);
    p_ = std::make_shared<boost::fibers::promise<T>>();
  }

  void SetValue(T val) const {
    if (Valid()) {
      p_->set_value(std::move(val));
      *p_has_value_ = true;
    }
  }
};

struct Op {
  OpType op_type_;
  std::string key_;
  std::string value_;
  uint64_t client_id_;
  uint64_t seq_number_;
  shardctrler::ShardConfig config_;
  int shard_;
  int config_num_;
  std::string data_;
  Promise<Reply> promise_;
  int sender_;
};

struct LastOpTable {
  // shard -> {clientid -> {seq, Reply}}
  std::unordered_map<int, std::unordered_map<uint64_t, LastOp>> table_;
  boost::fibers::mutex mu_;
};

inline void to_json(nlohmann::json &j, const Reply &rep) {
  j = nlohmann::json{{"val", rep.val_}, {"wg", rep.wrong_group_}};
}

inline void from_json(const nlohmann::json &j, Reply &rep) {
  j.at("val").get_to(rep.val_);
  j.at("wg").get_to(rep.wrong_group_);
}

inline void to_json(nlohmann::json &j, const LastOp &lo) { j = nlohmann::json{{"seq", lo.seq_}, {"rep", lo.rep_}}; }

inline void from_json(const nlohmann::json &j, LastOp &lo) {
  j.at("seq").get_to(lo.seq_);
  j.at("rep").get_to(lo.rep_);
}

}  // namespace kv::shardkv
