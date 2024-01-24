#include "shardkv/server.h"

namespace kv::shardkv {

kv::shardkv::ShardKV::ShardKV(std::vector<network::ClientEnd *> servers, int me,
                              std::shared_ptr<storage::PersistentInterface> persister, int maxraftstate, int gid,
                              std::vector<network::ClientEnd *> ctrlers,
                              std::function<network::ClientEnd *(std::string)> make_end)
    : me_(me),
      make_end_(std::move(make_end)),
      gid_(gid),
      ctrlers_(std::move(ctrlers)),
      maxraftstate_(maxraftstate),
      persister_(persister) {
  Logger::Debug1(kDTrace, me_, gid_, "........... Start ..........");

  apply_ch_ = std::make_shared<raft::apply_channel_t>();
  rf_ = std::make_shared<raft::Raft>(std::move(servers), me_, persister, apply_ch_);

  mck_ = std::make_unique<shardctrler::Clerk>(ctrlers_);
  kvcl_ = std::make_unique<Clerk>(ctrlers_, make_end_);

  // restore any snapshoted state
  auto snapshot = rf_->ReadSnapshot();
  if (snapshot) {
    Logger::Debug1(kDServ, me_, gid_, "Restore state from previous snapshot");
    InstallSnapshot(*snapshot);
  }

  raft_applied_thread_ = boost::fibers::fiber([&] { ListenFromRaft(); });
  observe_raft_thread_ = boost::fibers::fiber([&] { ObserverRaftState(maxraftstate_); });
  config_thread_ = boost::fibers::fiber([&] { ListenForConfigChanges(); });

  boost::fibers::fiber([&] { RetrieveAllCommittedLogs(); }).detach();
}

ShardKV::~ShardKV() {
  dead_ = true;

  pending_shards_ready_ch_.Close();
  up_to_date_ch_.Close();

  apply_ch_->close();

  if (raft_applied_thread_.joinable()) {
    raft_applied_thread_.join();
  }

  if (observe_raft_thread_.joinable()) {
    observe_raft_thread_.join();
  }

  if (config_thread_.joinable()) {
    config_thread_.join();
  }
}

void ShardKV::InstallNewShard(int shard, const char *data) {
  auto json_snap = nlohmann::json::parse(data);
  auto db = json_snap["db"].get<std::unordered_map<std::string, std::string>>();
  auto dtable = json_snap["dtable"].get<std::unordered_map<uint64_t, LastOp>>();

  mu_.lock();
  db_.Lock();
  lot_.mu_.lock();

  db_.SetShard(shard, db);
  lot_.table_[shard] = std::move(dtable);

  lot_.mu_.unlock();
  db_.Unlock();
  mu_.unlock();

  Logger::Debug1(kDServ, me_, gid_, fmt::format("Install new shard {}", shard));
}

void ShardKV::ListenFromRaft() {
  while (!Killed()) {
    //    if (!apply_ch_->Empty()) {
    raft::ApplyMsg m;
    apply_ch_->pop(m);
    //      InstallRaftAppliedMsg(m);
    //    }
    //    for (auto m : *apply_ch_) {
    InstallRaftAppliedMsg(m);
    //    }
  }

  //  // dry all the cmd msgs before return
  //  while (!apply_ch_->Empty()) {
  //    raft::ApplyMsg m;
  //    apply_ch_->Dequeue(&m);
  //    InstallRaftAppliedMsg(m);
  //  }
}

void ShardKV::InstallRaftAppliedMsg(const raft::ApplyMsg &m) {
  if (m.command_valid_) {
    if (m.command_index_ != last_applied_ + 1) {
      throw SHARDKV_EXCEPTION(
          fmt::format("Raft applies out of order, lastApplied {}, cmdIndex {}", last_applied_, m.command_index_));
    }

    try {
      auto cmd = std::any_cast<Op>(m.command_);
      InstallCmdMsg(cmd, m.command_index_);
      SetLastApplied(m.command_index_);
      Logger::Debug1(kDTrck, me_, gid_, fmt::format("New index {} applied", m.command_index_));
    } catch (const std::bad_any_cast &e) {
      Logger::Debug(kDError, me_, "Exception throw trying to cast the cmd msg");
    }
  } else if (m.snapshot_valid_) {
    // only install if I am a lagging replica, snapshot sent from Leader
    // which means last_applied_ < m.snapshot_index_
    if (LastApplied() < m.snapshot_index_) {
      Logger::Debug1(kDSnp1, me_, gid_, fmt::format("Installing snapshot upto index {}", m.snapshot_index_));
      InstallSnapshot(m.snapshot_);
    }
  }
}

void ShardKV::InstallCmdMsg(const Op &cmd, int cmd_index) {
  Reply r;

  ON_SCOPE_EXIT {
    if (cmd.sender_ == me_) {
      cmd.promise_.SetValue(std::move(r));
    }
  };

  switch (cmd.op_type_) {
    case OpType::PUT:
    case OpType::GET:
    case OpType::APPEND: {
      // maybe the client submits the same request multiple times in a short amount of time
      // need to check if I've already served the request
      auto rep = HasRequestBeenServed(KeyToShard(cmd.key_), cmd.client_id_, cmd.seq_number_);
      if (rep) {
        Logger::Debug1(kDDrop, me_, gid_,
                       fmt::format("InstallCmdMsg: client {} with seq {} has already been served, return ...",
                                   cmd.client_id_ % 10000, cmd.seq_number_));
        r = *rep;
        return;
      }

      // check if the request's config is the same as my current config, otherwise reject
      if (cmd.config_num_ != CurrentConfig()) {
        Logger::Debug1(kDTrace, me_, gid_,
                       fmt::format("The op config {} is different with my current config {}, return ...",
                                   cmd.config_num_, CurrentConfig()));
        r.wrong_group_ = true;
        return;
      }

      std::lock_guard lock(mu_);

      if (cmd.op_type_ == OpType::GET) {
        auto val = db_.Get(cmd.key_);
        if (!val) {
          Logger::Debug1(kDWarn, me_, gid_, fmt::format("The key {} is not existed in DB", cmd.key_));
          r.val_ = "";
        } else {
          r.val_ = *val;
        }
      } else if (cmd.op_type_ == OpType::PUT) {
        db_.Put(cmd.key_, cmd.value_);
      } else if (cmd.op_type_ == OpType::APPEND) {
        db_.Append(cmd.key_, cmd.value_);
      }

      PopulateLastOpInfo(KeyToShard(cmd.key_), cmd.client_id_, cmd.seq_number_, r);
      break;
    }
    case OpType::INSTALL_CONFIG: {
      // no need to cache since it's only on the local cluster
      InstallNewConfig(cmd.config_);
      break;
    }
    case OpType::INSTALL_SHARD: {
      // maybe the client submits the same request multiple times in a short amount of time
      // need to check If I've already served the request
      auto rep = HasRequestBeenServed(KeyToShard(cmd.key_), cmd.client_id_, cmd.seq_number_);
      if (rep) {
        Logger::Debug1(kDDrop, me_, gid_,
                       fmt::format("InstallCmdMsg: client {} with seq {} has already been served, return ...",
                                   cmd.client_id_ % 10000, cmd.seq_number_));
        r = *rep;
        return;
      }
      InstallNewShard(cmd.shard_, cmd.data_.c_str());
      PopulateLastOpInfo(cmd.shard_, cmd.client_id_, cmd.seq_number_, r);

      if (RemovePendingShard(cmd.shard_)) {
        // signal to install a new config
        if (Killed()) {
          return;
        }

        if (pending_shards_ready_ch_.HasReceiver()) {
          pending_shards_ready_ch_.Send(true);
        }
      }
      break;
    }
    case OpType::REMOVE_SHARD: {
      RemoveShard(cmd.shard_, cmd.config_num_);
      break;
    }
    case OpType::NOOP: {
      if (up_to_data_idx_ <= cmd_index) {
        Logger::Debug1(kDServ, me_, gid_, fmt::format("Receive Noop for idx {}, I am up-to-date now", cmd_index));
        up_to_date_ch_.Close();
      }
      break;
    }
    default:
      throw SHARDKV_EXCEPTION("invalid cmd OpType");
  }
}

std::pair<raft::Snapshot, int> ShardKV::CaptureCurrentState() {
  nlohmann::json json_snap;
  // no more access to the db while captureing the snapshot
  mu_.lock();
  db_.Lock();
  lot_.mu_.lock();

  json_snap["db"] = db_.GetSnapShot();
  json_snap["lot"] = lot_.table_;
  auto last_applied = last_applied_;
  json_snap["last_applied"] = last_applied_;
  if (config_ != nullptr) {
    json_snap["cfg"] = *config_;
  }
  json_snap["shard_installed"] = shards_installed_;
  json_snap["shard_removed"] = shards_removed_;

  lot_.mu_.unlock();
  db_.Unlock();
  mu_.unlock();

  raft::Snapshot snap;
  snap.data_ = json_snap.dump();
  return {snap, last_applied};
}

void ShardKV::InstallSnapshot(const raft::Snapshot &snapshot) {
  if (snapshot.Empty()) {
    Logger::Debug1(kDWarn, me_, gid_, "Trying to install empty snapshot");
    return;
  }

  auto json_snap = nlohmann::json::parse(snapshot.data_);

  auto db = json_snap["db"].get<std::unordered_map<int, std::unordered_map<std::string, std::string>>>();
  auto lot = json_snap["lot"].get<std::unordered_map<int, std::unordered_map<uint64_t, LastOp>>>();
  auto last_applied = json_snap["last_applied"].get<int>();
  std::optional<shardctrler::ShardConfig> cfg;
  if (json_snap.contains("cfg")) {
    cfg = json_snap["cfg"].get<shardctrler::ShardConfig>();
  }
  auto shards_installed = json_snap["shard_installed"].get<std::unordered_set<int>>();
  auto shards_removed = json_snap["shard_removed"].get<std::unordered_set<int>>();

  mu_.lock();
  db_.Lock();
  lot_.mu_.lock();

  db_.SetSnapshot(db);
  lot_.table_ = lot;
  last_applied_ = last_applied;

  if (cfg) {
    InstallNewConfigUnlocked(*cfg);
  }

  shards_installed_ = shards_installed;
  shards_removed_ = shards_removed;
  Logger::Debug1(kDServ, me_, gid_, fmt::format("Restored Shards Removed to {}", common::ToString(shards_removed_)));
  for (auto shard : shards_removed_) {
    UnserveOne(shard);
  }
  Logger::Debug1(kDServ, me_, gid_, fmt::format("Now my serving shards {}", common::ToString(serving_shards_)));

  lot_.mu_.unlock();
  db_.Unlock();
  mu_.unlock();
}

void ShardKV::ObserverRaftState(int maxraftstate) {
  if (maxraftstate == -1) {
    return;
  }

  while (!Killed()) {
    auto sz = rf_->RaftStateSize();
    if (sz >= maxraftstate) {
      //      Logger::Debug1(kDSnp1, me_, gid_, fmt::format("Raft state is now {} >= maxraftstate({})", sz,
      //      maxraftstate));
      auto [snapshot, last_applied] = CaptureCurrentState();
      Logger::Debug1(kDSnp1, me_, gid_, fmt::format("Ask Raft to install Snapshot upto index {}", last_applied));
      rf_->DoSnapshot(last_applied, snapshot);
    }

    common::SleepMs(100);
  }
}

void ShardKV::RetrieveAllCommittedLogs() {
  if (rf_->RaftStateSize() == 0) {
    return;
  }

  std::atomic<bool> up_to_date = false;
  boost::fibers::fiber f([&] {
    auto up_to_date = up_to_date_ch_.Receive(15000);  // max timeout 15s
    if (Killed()) {
      Logger::Debug1(kDTrace, me_, gid_, "RetrieveCommits: return while waiting for the result");
      return;
    }

    if (!up_to_date_ch_.IsClose() && !up_to_date) {
      Logger::Debug1(kDTrace, me_, gid_, "RetrieveCommits: Timeout (5s) waiting for the result");
      return;
    }

    up_to_date = true;
  });
  ON_SCOPE_EXIT { f.join(); };

  while (!up_to_date && !Killed()) {
    if (rf_->IsLeader()) {
      Op op;
      op.op_type_ = OpType::NOOP;
      auto [index, _, __] = rf_->Start(op);  // and an empty op to let raft commit all the logs
      up_to_data_idx_ = index;
      Logger::Debug1(kDLeader1, me_, gid_, fmt::format("Set uptodate idx to {} for recovering", index));
      break;
    }

    common::SleepMs(10);
  }
}

}  // namespace kv::shardkv