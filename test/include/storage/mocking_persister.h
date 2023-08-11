#pragma once

#include "common/macros.h"
#include "raft/raft.h"
#include "storage/persistent_interface.h"

namespace kv::storage {

class MockingPersister : public PersistentInterface {
 public:
  MockingPersister() = default;

  MockingPersister(raft::RaftPersistState state, raft::Snapshot snapshot)
      : state_(std::move(state)), snapshot_(std::move(snapshot)) {}

  MockingPersister(const MockingPersister &other) {
    std::lock_guard lock(mu_);
    state_ = other.state_;
  }

  void SaveRaftState(const raft::RaftPersistState &state) override {
    std::lock_guard lock(mu_);
    state_ = state;
  }

  void ReadRaftState(raft::RaftPersistState *state) const override {
    std::lock_guard lock(mu_);
    *state = state_;
  }

  void SaveRaftSnapshot(const raft::Snapshot &snapshot) override {
    std::lock_guard lock(mu_);
    snapshot_ = snapshot;
  }

  void ReadRaftSnapshot(raft::Snapshot *snapshot) const override {
    std::lock_guard lock(mu_);
    *snapshot = snapshot_;
  }

  raft::RaftPersistState state_;
  raft::Snapshot snapshot_;
  mutable std::mutex mu_;
};

}  // namespace kv::storage
