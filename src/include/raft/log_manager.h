#pragma once

#include <any>

namespace kv::raft {

struct LogEntry {
  std::any command_;
  int term_;
};

class LogManager {
 public:
  int GetLastLogIdx() const;
  int GetLastLogTerm() const;
};

}  // namespace kv::raft
