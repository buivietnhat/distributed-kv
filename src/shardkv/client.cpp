#include "shardkv/client.h"

#include "shardkv/common.h"

namespace kv::shardkv {

kv::shardkv::Clerk::Clerk(std::vector<network::ClientEnd *> ctrlers,
                          std::function<network::ClientEnd *(std::string)> make_end)
    : sm_(std::move(ctrlers)), make_end_(std::move(make_end)), seq_number_{0} {
  uuid_ = common::RandInt();
}

std::string kv::shardkv::Clerk::Get(const std::string &key) {
  GetArgs args;
  args.key_ = key;
  args.uuid_ = uuid_;

  std::unique_lock l(mu_);
  args.seq_number_ = seq_number_;
  seq_number_ = (seq_number_ + 1) % INT64_MAX;
  l.unlock();

  while (!Killed()) {
    auto shard = KeyToShard(key);
    auto gid = config_.shards_[shard];
    if (config_.groups_.contains(gid)) {
      auto servers = config_.groups_[gid];
      for (uint32_t si = 0; si < servers.size(); si++) {
        auto srv = make_end_(servers[si]);
        auto reply = srv->Get(args);
        if (reply && (reply->err_ == OK || reply->err_ == ERR_NO_KEY)) {
          if (reply->err_ == OK) {
            Logger::Debug(kDCler, uuid_ % kMod,
                          fmt::format("Request Get for key {} with Seq {} successfully", key, args.seq_number_));
          }
          return reply->value_;
        }
        if (reply && reply->err_ == ERR_WRONG_GROUP) {
          break;
        }
        // ... not ok, or ERR_WRONG_LEADER
      }
    }
    common::SleepMs(100);
    // ask controler for the latest configuration.
    config_ = sm_.Query(-1);
  }

  return {};
}

void kv::shardkv::Clerk::Put(const std::string &key, const std::string &value) { PutAppend(key, value, OpType::PUT); }

void kv::shardkv::Clerk::Append(const std::string &key, const std::string &value) {
  PutAppend(key, value, OpType::APPEND);
}

bool kv::shardkv::Clerk::InstallShard(int srd_gid, int des_gid, int shard, const std::string &data,
                                      const kv::shardctrler::ShardConfig &cfg) {
  InstallShardArgs args;
  args.shard_ = shard;
  args.data_ = data;
  args.uuid_ = uuid_;
  args.gid_ = srd_gid;
  args.cfg_num_ = cfg.num_;

  std::unique_lock l(mu_);
  args.seq_number_ = seq_number_;
  seq_number_ = (seq_number_ + 1) % INT64_MAX;
  l.unlock();

//  Logger::Debug(
//      kDCler, uuid_ % kMod,
//      fmt::format("Request to install Shard {}  for Group {} with Seq {} ", shard, des_gid, args.seq_number_));

  while (!Killed()) {
    if (cfg.groups_.contains(des_gid)) {
      auto servers = cfg.groups_.at(des_gid);
      for (uint32_t si = 0; si < servers.size(); si++) {
        auto srv = make_end_(servers[si]);
        auto reply = srv->InstallShard(args);
        if (reply && reply->err_ == OK) {
          Logger::Debug(kDCler, uuid_ % kMod,
                        fmt::format("Request to install Shard {} for Group {} with Seq {} successfully", shard, des_gid,
                                    args.seq_number_));
          return true;
        }
      }
    }
    common::SleepMs(100);
  }

  return false;
}

void kv::shardkv::Clerk::PutAppend(const std::string &key, const std::string &value, OpType op) {
  PutAppendArgs args;
  args.key_ = key;
  args.value_ = value;
  args.op_ = op;
  args.uuid_ = uuid_;

  std::unique_lock l(mu_);
  args.seq_number_ = seq_number_;
  seq_number_ = (seq_number_ + 1) % INT64_MAX;
  l.unlock();

  while (!Killed()) {
    auto shard = KeyToShard(key);
    auto gid = config_.shards_[shard];
    if (config_.groups_.contains(gid)) {
      auto servers = config_.groups_[gid];
      for (uint32_t si = 0; si < servers.size(); si++) {
        auto srv = make_end_(servers[si]);
        auto reply = srv->PutAppend(args);
        if (reply && reply->err_ == OK) {
          Logger::Debug(kDCler, uuid_ % kMod,
                        fmt::format("Request {} for key {} val {} with Seq {} successfully", ToString(op), key, value,
                                    args.seq_number_));
          return;
        }
        if (reply && reply->err_ == ERR_WRONG_GROUP) {
          break;
        }
        // ... not ok, or ERR_WRONG_LEADER
      }
    }
    common::SleepMs(100);
    // ask controler for the latest configuration.
    config_ = sm_.Query(-1);
  }
}

}  // namespace kv::shardkv