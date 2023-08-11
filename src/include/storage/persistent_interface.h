#pragma once

namespace kv::raft {
struct RaftPersistState;
struct Snapshot;
}  // namespace kv::raft

namespace kv::storage {

class PersistentInterface {
 public:
  virtual void SaveRaftState(const raft::RaftPersistState &state) = 0;
  virtual void ReadRaftState(raft::RaftPersistState *state) const = 0;

  virtual void SaveRaftSnapshot(const raft::Snapshot &snapshot) = 0;
  virtual void ReadRaftSnapshot(raft::Snapshot *state) const = 0;

  virtual ~PersistentInterface() = default;
};

// extern PersistentInterface *instance;
//
// PersistentInterface *GetPersistentInterface();
//
// void SetPersistentInterface(PersistentInterface *persister);

}  // namespace kv::storage
