#include "shardkv/common.h"
#include "shardkv/server.h"

namespace kv::shardkv {

void ShardKV::InstallNewConfig(const shardctrler::ShardConfig &cfg) {
  if (cfg.num_ == 0) {
    Logger::Debug1(kDTrace, me_, gid_, "Trying to install invalid config");
    return;
  }

  std::lock_guard lock(mu_);
  InstallNewConfigUnlocked(cfg);
}

void ShardKV::InstallNewConfigUnlocked(const shardctrler::ShardConfig &cfg) {
  // first to remove the already-installed/removed info before installing new configuration
  RemoveShardInstalled();
  RemoveShardRemoved();

  config_ = std::make_unique<shardctrler::ShardConfig>(cfg);
  auto new_serving_shards = NewServingShards(cfg);

  // replace the serving shards info
  serving_shards_ = new_serving_shards;

  Logger::Debug1(
      kDServ, me_, gid_,
      fmt::format("Installed new config {} with serving shard {}", cfg.num_, common::ToString(serving_shards_)));
}

std::unordered_set<int> ShardKV::NewServingShards(const shardctrler::ShardConfig &cfg) {
  std::unordered_set<int> new_serving_shards;

  for (int i = 0; i < shardctrler::kNShards; i++) {
    if (cfg.shards_[i] == gid_) {
      new_serving_shards.insert(i);
    }
  }

  return new_serving_shards;
}

void ShardKV::SendInstallConfig(const shardctrler::ShardConfig &cfg) {
  if (Killed()) {
    return;
  }

  Logger::Debug1(kDTrace, me_, gid_, fmt::format("Send InstallConfig with num {} request to Replicas", cfg.num_));

  auto op = GenerateInstallConfigOp(cfg);
  auto [_, __, is_leader] = rf_->Start(op);
  if (!is_leader) {
    Logger::Debug1(kDTrace, me_, gid_,
                   fmt::format("I just lost Leadership trying to send install config num {}, return ...", cfg.num_));
    return;
  }

  // wait for the reply, max 2 seconds
  auto fut = op.promise_.p_->get_future();
  auto status = fut.wait_for(5s);

  if (Killed()) {
    Logger::Debug(kDTrace, me_, "InstallConfig: return while waiting for the result");
    return;
  }

  if (status == boost::fibers::future_status::timeout) {
    Logger::Debug(kDTrace, me_, "InstallConfig: Timeout (5s) waiting for the result");
    return;
  }

  if (status == boost::fibers::future_status::ready) {
    return;
  }

  throw SHARDKV_EXCEPTION("unreachable: should not be here");
}

void ShardKV::SendRemoveShard(int shard, int config_num) {
  if (Killed()) {
    return;
  }

  Logger::Debug1(kDTrace, me_, gid_,
                 fmt::format("Send Remove Shard {} with num {} request to Replicas", shard, config_num));

  auto op = GenerateRemoveShardOp(shard, config_num);
  auto [_, __, is_leader] = rf_->Start(op);
  if (!is_leader) {
    Logger::Debug1(kDTrace, me_, gid_,
                   fmt::format("I just lost Leadership trying to send remove shard {} config num {}, return ...", shard,
                               config_num));
    return;
  }

  // wait for the reply, max 2 seconds
  auto fut = op.promise_.p_->get_future();
  auto status = fut.wait_for(5s);

  if (Killed()) {
    Logger::Debug(kDTrace, me_, "RemoveShard: return while waiting for the result");
    return;
  }

  if (status == boost::fibers::future_status::timeout) {
    Logger::Debug(kDTrace, me_, "RemoveShard: Timeout (5s) waiting for the result");
    return;
  }

  if (status == boost::fibers::future_status::ready) {
    return;
  }

  throw SHARDKV_EXCEPTION("unreachable: should not be here");
}

void ShardKV::SendInstallShard(int shard, int gid, const shardctrler::ShardConfig &cfg) {
  db_.Lock();
  lot_.mu_.lock();
  auto db = db_.GetShard(shard);
  auto dtable = lot_.table_[shard];

  nlohmann::json json_shard;
  json_shard["db"] = db;
  json_shard["dtable"] = dtable;

  lot_.mu_.unlock();
  db_.Unlock();

  Logger::Debug1(
      kDLeader1, me_, gid_,
      fmt::format("Sending InstallShard request to Group {} for Shard {} config Num {}", gid, shard, cfg.num_));

  if (!kvcl_->InstallShard(gid_, gid, shard, json_shard.dump(), cfg)) {
    Logger::Debug1(kDTrace, me_, gid_, fmt::format("Send Install Shard for shard {} to group {} failed", shard, gid));
  } else {
    Logger::Debug1(
        kDLeader1, me_, gid_,
        fmt::format("Reqest to install shard {} for Group {} config Num {} successfully", shard, gid, cfg.num_));

    boost::fibers::fiber([me = shared_from_this(), shard, cfg_num = std::move(cfg.num_)] { me->SendRemoveShard(shard, cfg_num); }).detach();
  }
}

void ShardKV::ListenForConfigChanges() {
  if (rf_->RaftStateSize() > 0) {
    Logger::Debug1(kDTrace, me_, gid_, fmt::format("CF: Raft state size = {}", rf_->RaftStateSize()));
    // wait until up-to-date
    auto up_to_date = up_to_date_ch_.Receive(15000);  // max timeout 15s

    if (!up_to_date_ch_.IsClose() && !up_to_date) {
      Logger::Debug1(kDTrace, me_, gid_, "ListenForConfig: Timeout (5s) waiting for the result");
      return;
    }
  }

  while (!Killed()) {
    if (rf_->IsLeader()) {
      // get the latest config update
      //      Logger::Debug1(kDLeader1, me_, gid_, fmt::format("Querying for config number {}", CurrentConfig() + 1));
      auto cfg = mck_->Query(CurrentConfig() + 1);
      if (cfg.num_ > CurrentConfig()) {
        Logger::Debug1(kDLeader1, me_, gid_, fmt::format("There is a new config num {}, installing ...", cfg.num_));

        ProcessNewConfig(cfg);
      }
    }

    common::SleepMs(50);
  }
}

void ShardKV::ProcessNewConfig(const shardctrler::ShardConfig &cfg) {
  if (config_ != nullptr) {
    // first unserve all the shards that don't belong to me in the new config
    auto new_serving_shards = NewServingShards(cfg);
    std::vector<int> unserved_shards;

    for (auto shard : serving_shards_) {
      if (!new_serving_shards.contains(shard)) {
        unserved_shards.push_back(shard);
      }
    }

    // set the pending shard to expecting coming to serve
    std::unordered_set<int> pending_shards;
    for (auto shard : new_serving_shards) {
      if (!serving_shards_.contains(shard)) {
        pending_shards.insert(shard);
      }
    }

    std::unique_lock lock(mu_);
    Unserve(unserved_shards);
    Logger::Debug1(kDLeader1, me_, gid_, fmt::format("Set pending shards to {}", common::ToString(pending_shards)));
    pending_shards_ = pending_shards;
    auto pending_satisfied = CheckForAlreadyInstalledShards();
    auto out_standing_op_idx = outstanding_op_idx_;
    lock.unlock();

    WaitForOutStandingOpToFinish(out_standing_op_idx);

    // send those shards to the new in charge groups
    // but what if we cannot send the shard? i.e network issue, keep trying?
    if (!unserved_shards.empty()) {
      Logger::Debug1(kDLeader1, me_, gid_, fmt::format("Sending shard data for {}", common::ToString(unserved_shards)));
      SendInstallShards(unserved_shards, cfg);
    }

    // on the other side, only serve for the new shards in the new config if I have received the shards
    // from the old in charge groups
    if (!pending_shards.empty() && !pending_satisfied) {
      auto ready = pending_shards_ready_ch_.Receive(10000);  // timeout 10s
      if (Killed()) {
        return;
      }

      if (!pending_shards_ready_ch_.IsClose() && !ready) {
        Logger::Debug1(kDTrace, me_, gid_, "ProcessNewConfig: Timeout (10s) waiting for the result");
        return;
      }
    }

    if (pending_satisfied) {
      Logger::Debug1(kDLeader1, me_, gid_, "No need to wait since all pending shards are satisfied");
    }
  }

  // for now all the pending shards have been satisfied, install new config
  SendInstallConfig(cfg);
}

void ShardKV::WaitForOutStandingOpToFinish(int outstanding_idx) {
  Logger::Debug1(kDTrace, me_, gid_, fmt::format("Waiting for the outstanding op {} to be installed", outstanding_idx));

  while (!Killed()) {
    std::unique_lock l(mu_);
    if (last_applied_ >= outstanding_idx) {
      Logger::Debug1(kDTrace, me_, gid_, fmt::format("Waiting for outstanding {} finished", outstanding_idx));
      l.unlock();
      return;
    }

    l.unlock();
    common::SleepMs(5);
  }
}

void ShardKV::SendInstallShards(const std::vector<int> &shards, const shardctrler::ShardConfig &cfg) {
  std::vector<int> filter;

  std::unique_lock l(mu_);
  for (auto shard : shards) {
    if (!shards_removed_.contains(shard)) {
      filter.push_back(shard);
    } else {
      Logger::Debug1(kDTrace, me_, gid_, fmt::format("Don't need to send Shard {} since already done it", shard));
    }
  }
  l.unlock();

  if (filter.empty()) {
    return;
  }

  std::vector<boost::fibers::fiber> threads;

  for (auto shard : filter) {
    auto des_gid = cfg.shards_[shard];

    threads.push_back(boost::fibers::fiber([&, shard, des_gid] { SendInstallShard(shard, des_gid, cfg); }));
  }

  for (auto &thread : threads) {
    thread.join();
  }
}

}  // namespace kv::shardkv