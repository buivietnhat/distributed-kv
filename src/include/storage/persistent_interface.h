#pragma once

#include <optional>

#include "raft/common.h"

namespace kv::storage {

class PersistentInterface {
 public:
  virtual void SaveRaftState(const raft::RaftPersistState &state) = 0;
  virtual std::optional<raft::RaftPersistState> ReadRaftState() const = 0;

  virtual void SaveRaftSnapshot(const raft::Snapshot &snapshot) = 0;
  virtual std::optional<raft::Snapshot> ReadRaftSnapshot() const = 0;

  virtual void ReadStateAndSnap(std::optional<raft::RaftPersistState> &state,
                                std::optional<raft::Snapshot> snapshot) const = 0;
  virtual void Save(const raft::RaftPersistState &state, const raft::Snapshot &snapshot) = 0;

  virtual int RaftStateSize() const = 0;

  virtual ~PersistentInterface() = default;
};

}  // namespace kv::storage
