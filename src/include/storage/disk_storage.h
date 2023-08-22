#pragma once

#include "common/macros.h"
#include "storage/persistent_interface.h"

namespace kv::storage {

// class DiskStorage : public PersistentInterface {
//  public:
//   DiskStorage() = default;
//
//   DISALLOW_COPY_AND_MOVE(DiskStorage);
//
//  private:
//   void DoSaveRaftState(const raft::RaftPersistState &state) override;
//   void DoReadRaftState(raft::RaftPersistState *state) const override;
// };

}  // namespace kv::storage
