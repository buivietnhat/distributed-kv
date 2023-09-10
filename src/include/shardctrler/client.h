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

 private:
  std::vector<network::ClientEnd *> servers_;
  std::mutex mu_;
  uint64_t uuid_;
  uint64_t seq_number_;
};

}  // namespace kv::shardctrler