#include "raft/log_manager.h"

namespace kv::raft {

int LogManager::GetLastLogIdx() const { return 0; }
int LogManager::GetLastLogTerm() const { return 0; }

}  // namespace kv::raft