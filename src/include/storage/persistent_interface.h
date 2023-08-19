#pragma once

#include <optional>

namespace kv::raft {
struct RaftPersistState;
struct Snapshot;
}  // namespace kv::raft

namespace kv::storage {

class PersistentInterface {
 public:
  virtual void SaveRaftState(const raft::RaftPersistState &state) = 0;
  virtual std::optional<raft::RaftPersistState> ReadRaftState() const = 0;

  virtual void SaveRaftSnapshot(const raft::Snapshot &snapshot) = 0;
  virtual std::optional<raft::Snapshot> ReadRaftSnapshot() const = 0;

  virtual void Save(const raft::RaftPersistState &state, const raft::Snapshot &snapshot) = 0;

  virtual ~PersistentInterface() = default;
};

}  // namespace kv::storage
