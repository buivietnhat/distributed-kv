#include "shardctrler/client.h"

namespace kv::shardctrler {

kv::shardctrler::Clerk::Clerk(std::vector<network::ClientEnd *> servers) : servers_(std::move(servers)) {
  uuid_ = common::RandInt();
  seq_number_ = 0;
}

kv::shardctrler::ShardConfig kv::shardctrler::Clerk::Query(int num) {
  QueryArgs args;

  std::unique_lock l(mu_);
  args.seq_number_ = seq_number_;
  seq_number_ = (seq_number_ + 1) % INT64_MAX;
  l.unlock();

  args.uuid_ = uuid_;
  args.num_ = num;

  while (true) {
    // try each known server
    for (auto *srv : servers_) {
      auto reply = srv->Query(args);
      if (reply && reply->wrong_leader_ == false) {
        return reply->config_;
      }
    }

    common::SleepMs(100);
  }
}

void kv::shardctrler::Clerk::Join(const std::unordered_map<int, std::vector<std::string>> &servers) {
  JoinArgs args;
  args.servers_ = servers;

  std::unique_lock l(mu_);
  args.seq_number_ = seq_number_;
  seq_number_ = (seq_number_ + 1) % INT64_MAX;
  l.unlock();

  args.uuid_ = uuid_;

  while (true) {
    // try each known server
    for (auto *srv : servers_) {
      auto reply = srv->Join(args);
      if (reply && reply->wrong_leader_ == false) {
        return;
      }
    }

    common::SleepMs(100);
  }
}

void kv::shardctrler::Clerk::Leave(const std::vector<int> &gids) {
  LeaveArgs args;
  args.gids_ = gids;

  std::unique_lock l(mu_);
  args.seq_number_ = seq_number_;
  seq_number_ = (seq_number_ + 1) % INT64_MAX;
  l.unlock();

  args.uuid_ = uuid_;

  while (true) {
    // try each known server
    for (auto *srv : servers_) {
      auto reply = srv->Leave(args);
      if (reply && reply->wrong_leader_ == false) {
        return;
      }
    }

    common::SleepMs(100);
  }
}

void kv::shardctrler::Clerk::Move(int shard, int gid) {
  MoveArgs args;
  args.shard_ = shard;
  args.gid_ = gid;

  std::unique_lock l(mu_);
  args.seq_number_ = seq_number_;
  seq_number_ = (seq_number_ + 1) % INT64_MAX;
  l.unlock();

  args.uuid_ = uuid_;

  while (true) {
    // try each known server
    for (auto *srv : servers_) {
      auto reply = srv->Move(args);
      if (reply && reply->wrong_leader_ == false) {
        return;
      }
    }

    common::SleepMs(100);
  }
}

}  // namespace kv::shardctrler
