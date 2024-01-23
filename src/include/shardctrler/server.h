#pragma once

#include <mutex>

#include "raft/raft.h"
#include "shardctrler/common.h"

namespace kv::shardctrler {

class LastOpTable {
 public:
  std::pair<bool, ShardConfig> HasRequestBeenServed(uint64_t clientid, uint64_t seq) const;

  void PopulateLastOpInfo(uint64_t clientid, uint64_t seq, ShardConfig config);

 private:
  std::unordered_map<uint64_t, LastOp> table_;
  mutable boost::fibers::mutex mu_;
};

class ShardCtrler {
 public:
  ShardCtrler(std::vector<network::ClientEnd *> servers, int me,
              std::shared_ptr<storage::PersistentInterface> persister);

  ~ShardCtrler();

  JoinReply Join(const JoinArgs &args);

  LeaveReply Leave(const LeaveArgs &args);

  MoveReply Move(const MoveArgs &args);

  QueryReply Query(const QueryArgs &args);

  inline raft::Raft *Raft() const { return rf_.get(); }

  inline void Kill() {
    rf_->Kill();
    dead_ = true;

    // wake up the applying msg thread
    apply_ch_->push({});
  }

  inline bool Killed() const { return dead_; }

 private:
  using enum Err;

  void ListenFromRaft();

  void InstallRaftAppliedMsg(const raft::ApplyMsg &m);

  void InstallCmdMsg(const Op &cmd);

  ShardConfig GenerateNewConfig(const Op &cmd);

  ShardConfig GenerateNewJoinConfig(const Op &cmd);

  ShardConfig GenerateNewLeaveConfig(const Op &cmd);

  ShardConfig GenerateNewMoveConfig(const Op &cmd);

  ShardConfig GenerateNewQueryConfig(const Op &cmd);

  boost::fibers::mutex mu_;
  int me_;
  std::unique_ptr<raft::Raft> rf_;
  raft::apply_channel_ptr apply_ch_;
  std::vector<ShardConfig> configs_;
  std::unique_ptr<LastOpTable> lot_;
  boost::fibers::fiber raft_applied_thread_;
  int last_applied_{0};
  bool dead_{false};
};

}  // namespace kv::shardctrler
