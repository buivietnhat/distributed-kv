#pragma once

#include <mutex>

#include "common/container/channel.h"
#include "common/thread_registry.h"
#include "raft/raft.h"
#include "shardctrler/client.h"
#include "shardkv/client.h"
#include "shardkv/common.h"
#include "shardkv/db.h"

namespace kv::shardkv {

class ShardKV {
 public:
  ShardKV(std::vector<network::ClientEnd *> servers, int me, std::shared_ptr<storage::PersistentInterface> persister,
          int maxraftstate, int gid, std::vector<network::ClientEnd *> ctrlers,
          std::function<network::ClientEnd *(std::string)> make_end);

  ~ShardKV();

  GetReply Get(const GetArgs &args);

  PutAppendReply PutAppend(const PutAppendArgs &args);

  InstallShardReply InstallShard(const InstallShardArgs &args);

  raft::Raft *GetRaft() const { return rf_.get(); }

  inline void Kill() {
    dead_ = true;

    rf_->Kill();
    kvcl_->Kill();
    mck_->Kill();
  }

  inline bool Killed() const { return dead_ == true; }

 private:
  std::unordered_set<int> NewServingShards(const shardctrler::ShardConfig &cfg);

  void SendInstallConfig(const shardctrler::ShardConfig &cfg);

  void SendRemoveShard(int shard, int config_num);

  void SendInstallShard(int shard, int gid, const shardctrler::ShardConfig &cfg);

  void InstallCmdMsg(const Op &cmd, int cmd_index);

  void InstallNewConfig(const shardctrler::ShardConfig &cfg);

  void InstallNewConfigUnlocked(const shardctrler::ShardConfig &cfg);

  std::optional<Reply> HasRequestBeenServed(int shard, uint64_t clientid, uint64_t seq);

  inline void Unserve(const std::vector<int> &shards) {
    for (auto shard : shards) {
      serving_shards_.erase(shard);
    }
  }

  inline void UnserveOne(int shard) {
    if (serving_shards_.contains(shard)) {
      serving_shards_.erase(shard);
      Logger::Debug1(
          kDServ, me_, gid_,
          fmt::format("Just send shard {} ok, now serving list {}", shard, common::ToString(serving_shards_)));
    }
  }

  void PopulateLastOpInfo(int shard, uint64_t clientid, uint64_t seq, Reply r);

  inline bool IsServingThisShardUnLocked(int shard) const { return serving_shards_.contains(shard); }

  inline bool IsServingThisShard(int shard) const {
    std::lock_guard lock(mu_);
    return IsServingThisShardUnLocked(shard);
  }

  inline bool IsServingThisKeyUnlocked(std::string_view key) const {
    auto shard = KeyToShard(std::string(key));
    return IsServingThisShardUnLocked(shard);
  }

  inline bool IsServingThisKey(std::string_view key) const {
    std::lock_guard l(mu_);
    return IsServingThisKeyUnlocked(key);
  }

  inline bool IsPendingThisShard(int shard) const {
    std::lock_guard l(mu_);
    return pending_shards_.contains(shard);
  }

  // return true if all the pending shards removed
  bool RemovePendingShard(int shard) {
    std::lock_guard l(mu_);

    if (!pending_shards_.contains(shard)) {
      // if not pending for this shard, just save the info
      shards_installed_.insert(shard);
    }

    pending_shards_.erase(shard);
    return pending_shards_.empty();
  }

  void RemoveShard(int shard, int config_num);

  bool CheckForAlreadyInstalledShards();

  inline void RemoveShardInstalled() { shards_installed_ = std::unordered_set<int>(); }

  inline void RemoveShardRemoved() { shards_removed_ = std::unordered_set<int>(); }

  Op GenerateGetOp(const GetArgs &args, int config_num);

  Op GeneratePutAppendOp(const PutAppendArgs &args, OpType op_type, int config_num);

  Op GenerateInstallConfigOp(const shardctrler::ShardConfig &cfg);

  Op GenerateInstallShardOp(const InstallShardArgs &args);

  Op GenerateRemoveShardOp(int shard, int config_num);

  inline void SetOutstandingOpIdx(int index) {
    if (index > outstanding_op_idx_) {
      outstanding_op_idx_ = index;
    }
  }

  inline int GetOutstandingOpIdx() const {
    std::lock_guard lock(mu_);
    return outstanding_op_idx_;
  }

  inline int CurrentConfig() const {
    std::lock_guard lock(mu_);
    if (config_ == nullptr) {
      return 0;
    }
    return config_->num_;
  }

  inline int LastApplied() const {
    std::lock_guard lock(mu_);
    return last_applied_;
  }

  inline void SetLastApplied(int last_appl) {
    std::lock_guard lock(mu_);
    last_applied_ = last_appl;
  }

  void InstallNewShard(int shard, const char *data);

  void ListenFromRaft();

  void InstallRaftAppliedMsg(const raft::ApplyMsg &m);

  std::pair<raft::Snapshot, int> CaptureCurrentState();

  void InstallSnapshot(const raft::Snapshot &snapshot);

  void ObserverRaftState(int maxraftstate);

  void RetrieveAllCommittedLogs();

  void ListenForConfigChanges();

  void ProcessNewConfig(const shardctrler::ShardConfig &cfg);

  void WaitForOutStandingOpToFinish(int outstanding_idx);

  void SendInstallShards(const std::vector<int> &shards, const shardctrler::ShardConfig &cfg);

  common::ThreadRegistry thread_registry_;

  mutable boost::fibers::mutex mu_;
  int me_;
  std::unique_ptr<raft::Raft> rf_;
  raft::apply_channel_ptr apply_ch_;
  bool dead_{false};
  std::function<network::ClientEnd *(std::string)> make_end_;
  int gid_;
  std::vector<network::ClientEnd *> ctrlers_;
  int maxraftstate_;  // snapshot if log grows this big

  int last_applied_ = 0;
  Database db_;
  LastOpTable lot_;

  std::unordered_set<int> serving_shards_;
  std::unordered_set<int> pending_shards_;
  common::Channel<bool> pending_shards_ready_ch_;
  common::Channel<bool> up_to_date_ch_;

  int up_to_data_idx_ = 0;
  int outstanding_op_idx_ = 0;

  std::unordered_set<int> shards_installed_;
  std::unordered_set<int> shards_removed_;

  std::unique_ptr<shardctrler::ShardConfig> config_;
  std::unique_ptr<shardctrler::Clerk> mck_;
  std::unique_ptr<Clerk> kvcl_;

  boost::fibers::fiber raft_applied_thread_;
  boost::fibers::fiber observe_raft_thread_;
  boost::fibers::fiber config_thread_;

  std::shared_ptr<storage::PersistentInterface> persister_;
};

}  // namespace kv::shardkv
