#pragma once

#include "common/macros.h"
#include "raft/raft.h"
#include "storage/persistent_interface.h"

namespace kv::storage {

class MockingPersister : public PersistentInterface {
 public:
  MockingPersister() = default;

  MockingPersister(std::optional<raft::RaftPersistState> state, std::optional<raft::Snapshot> snapshot)
      : state_(std::move(state)), snapshot_(std::move(snapshot)) {}

  MockingPersister(const MockingPersister &other) {
    std::lock_guard lock(mu_);
    state_ = other.state_;
  }

  void SaveRaftState(const raft::RaftPersistState &state) override {
    std::lock_guard lock(mu_);
    state_ = state;
  }

  std::optional<raft::RaftPersistState> ReadRaftState() const override {
    std::lock_guard lock(mu_);
    return state_;
  }

  void SaveRaftSnapshot(const raft::Snapshot &snapshot) override {
    std::lock_guard lock(mu_);
    snapshot_ = snapshot;
  }

  std::optional<raft::Snapshot> ReadRaftSnapshot() const override {
    std::lock_guard lock(mu_);
    return snapshot_;
  }

  void Save(const raft::RaftPersistState &state, const raft::Snapshot &snapshot) override {
    std::lock_guard lock(mu_);
    state_ = state;
    snapshot_ = snapshot;
  }

  std::optional<raft::RaftPersistState> state_;
  std::optional<raft::Snapshot> snapshot_;
  mutable std::mutex mu_;
};

}  // namespace kv::storage
