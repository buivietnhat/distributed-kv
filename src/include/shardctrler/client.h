#pragma once

#include "network/client_end.h"
#include "shardctrler/common.h"

namespace kv::shardctrler {

class Clerk {
 public:
  Clerk(std::vector<network::ClientEnd *> servers);

  ShardConfig Query(int num);

  void Join(const std::unordered_map<int, std::vector<std::string>> &servers);

  void Leave(const std::vector<int> &gids);

  void Move(int shard, int gid);

  inline void Kill() { dead_ = true; }

  inline bool Killed() { return dead_ == true; }

 private:
  std::vector<network::ClientEnd *> servers_;
  boost::fibers::mutex mu_;
  uint64_t uuid_;
  uint64_t seq_number_;
  bool dead_{false};
};

}  // namespace kv::shardctrler