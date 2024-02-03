#pragma once

#include <functional>
#include <string_view>

#include "network/client_end.h"
#include "shardctrler/client.h"

namespace kv::shardkv {

class Clerk {
 public:
  using enum Err;

  Clerk(std::vector<network::ClientEnd *> ctrlers, std::function<network::ClientEnd *(std::string)> make_end);

  std::string Get(const std::string &key);

  void Put(const std::string &key, const std::string &value);

  void Append(const std::string &key, const std::string &value);

  bool InstallShard(int srd_gid, int des_gid, int shard, const std::string &data, const shardctrler::ShardConfig &cfg);

  inline void Kill() { dead_ = true; }

  inline bool Killed() { return dead_ == true; }

 private:
  void PutAppend(const std::string &key, const std::string &value, OpType op);

  shardctrler::Clerk sm_;
  shardctrler::ShardConfig config_;

  // turns a server name into a ClientEnd on which we can send RPCs
  std::function<network::ClientEnd *(std::string)> make_end_;

  boost::fibers::mutex mu_;
  uint64_t uuid_;
  uint64_t seq_number_;
  bool dead_{false};
};

}  // namespace kv::shardkv
