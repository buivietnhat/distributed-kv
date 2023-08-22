#pragma once

#include <any>
#include <functional>
#include <mutex>
#include <vector>

#include "common/container/concurrent_blocking_queue.h"
#include "common/exception.h"
#include "common/logger.h"
#include "fmt/format.h"
#include "raft/common.h"

namespace kv::raft {

class LogManager {
 public:
  LogManager(int me, std::shared_ptr<common::ConcurrentBlockingQueue<ApplyMsg>> apply_channel);

  void CommitEntries(int start_idx, int from_idx, int to_idx);

  void ApplySnap(int snapshot_idx, int snapshot_term, const Snapshot &snap);

  std::vector<LogEntry> GetEntries(int start_idx) const;

  bool HasTerm(int term) const;

  int LastEntryFor(int term) const;

  // return true if we have persisted the data
  bool AppendEntries(const AppendEntryArgs &args, AppendEntryReply *reply, std::function<void(void)> persister);

  void AddNewEntries(int index, const std::vector<LogEntry> &entries);

  void DoDiscardLogs(int to_idx, int last_included_term);

  void ApplyLatestSnap();

  inline void DiscardLogs(int to_idx, int last_included_term) {
    std::lock_guard l(mu_);
    DoDiscardLogs(to_idx, last_included_term);
  }

  inline Snapshot DoGetSnapshot() {
    if (snapshot_ == nullptr) {
      throw LOG_MANAGER_EXCEPTION("trying to get nullptr snapshot");
    }
    return *snapshot_;
  }

  inline void Lock() const { mu_.lock(); }

  inline void Unlock() const { mu_.unlock(); }

  inline void DoSetStartIdx(int index) {
    common::Logger::Debug(kDSnap, me_, fmt::format("Set Start Idx to {}", index));
    start_idx_ = index;
  }

  inline void SetStartIdx(int index) {
    std::unique_lock l(mu_);
    DoSetStartIdx(index);
  }

  inline void DoSetCommitIndex(int index) {
    common::Logger::Debug(kDTrace, me_, fmt::format("Set Cmit Idx to {}", index));
    commid_idx_ = index;
  }

  inline void SetCommitIndex(int index) {
    std::unique_lock l(mu_);
    DoSetCommitIndex(index);
  }

  void DoSetSnapshot(const Snapshot &snap);

  inline int DoGetCommitIndex() const { return commid_idx_; }

  inline void DoSetTentativeCommitIndex(int index) { tentative_commit_idx_ = index; }

  inline int DoGetTentativeCommitIndex() const { return tentative_commit_idx_; }

  inline int DoGetStartIndex() const { return start_idx_; }

  inline int GetStartIndex() const {
    std::unique_lock l(mu_);
    return DoGetStartIndex();
  }

  inline int DoGetLastIncludedIndex() const { return last_included_idx_; }

  inline int GetLastIncludedIndex() const {
    std::unique_lock l(mu_);
    return DoGetLastIncludedIndex();
  }

  inline int DoGetLastIncludedTerm() const { return last_included_term_; }

  inline int GetLastIncludedTerm() const {
    std::unique_lock l(mu_);
    return DoGetLastIncludedTerm();
  }

  inline void DoSetLastIncludedIdx(int index) { last_included_idx_ = index; }

  inline void SetLastIncludedIdx(int index) {
    std::unique_lock l(mu_);
    DoSetLastIncludedIdx(index);
  }

  inline void DoSetLastIncludedTerm(int term) { last_included_term_ = term; }

  inline void SetLastIncludedTerm(int term) {
    std::unique_lock l(mu_);
    DoSetLastIncludedTerm(term);
  }

  inline int GetCommitIndex() const {
    std::unique_lock l(mu_);
    return commid_idx_;
  }

  inline int GetTentativeCommitIndex() const {
    std::unique_lock l(mu_);
    return tentative_commit_idx_;
  }

  inline void SetTentativeCommitIndex(int index) {
    std::unique_lock l(mu_);
    tentative_commit_idx_ = index;
    common::Logger::Debug(kDSnap, me_, fmt::format("Set tentative CMIT idx to {}", index));
  }

  inline int DoGetTerm(int index) const {
    if (index > 0 && index == last_included_idx_) {
      return last_included_term_;
    }

    auto actual_index = index - start_idx_;
    if (actual_index >= static_cast<int>(log_.size()) || actual_index < 0) {
      throw LOG_MANAGER_EXCEPTION(
          fmt::format("GetTerm: error out of bound with idx {}, len {} startIdx {}", index, log_.size(), start_idx_));
    }

    return log_[actual_index].term_;
  }

  inline int GetTerm(int index) const {
    std::lock_guard l(mu_);
    return DoGetTerm(index);
  }

  inline int DoGetLastLogIdx() const { return static_cast<int>(log_.size() - 1 + start_idx_); }

  inline int GetLastLogIdx() const {
    std::unique_lock l(mu_);
    return DoGetLastLogIdx();
  }

  inline int GetLastLogTerm() const {
    std::unique_lock l(mu_);
    if (log_.empty()) {
      return last_included_term_;
    }

    return log_[log_.size() - 1].term_;
  }

  inline int AppendLog(std::any command, int term) {
    std::unique_lock l(mu_);
    log_.emplace_back(term, command);
    return static_cast<int>(log_.size() - 1 + start_idx_);
  }

  inline const std::vector<LogEntry> &DoGetLogs() const { return log_; }

  inline void Kill() { dead_ = true; }

  inline bool Killed() const { return dead_; }

  inline void DoSetLogs(std::vector<LogEntry> log) {
    log_ = std::move(log);
  }

 private:
  void GenerateConflictingMsgReply(int conflicting_index, AppendEntryReply *reply);

  std::vector<LogEntry> CopyLog(int from_idx, int to_idx) const;

  bool IsEntryExist(int index, int term) const {
    if (index > 0 && index == last_included_idx_) {
      return last_included_term_ == term;
    }

    auto actual_index = index - start_idx_;
    if (actual_index < 0) {
      common::Logger::Debug(kDWarn, me_,
                            fmt::format("Entrie Exist: I've already discarded the log upto index {} > demand index {}",
                                        start_idx_, index));
      return false;
    }

    if (actual_index >= static_cast<int>(log_.size())) {
      common::Logger::Debug(kDWarn, me_,
                            fmt::format("Entry Exist: the index {} is too far ahead, my startIdx {}, log len {}", index,
                                        start_idx_, log_.size()));
      return false;
    }

    return log_[actual_index].term_ == term;
  }

  inline bool IsCommitIdxValid(int index) const {
    if (index <= tentative_commit_idx_) {
      return false;
    }
    return true;
  }

  uint32_t me_;
  std::shared_ptr<common::ConcurrentBlockingQueue<ApplyMsg>> apply_ch_;
  mutable std::mutex mu_;
  mutable std::mutex apply_mu_;
  bool dead_{false};

  int commid_idx_{0};
  int tentative_commit_idx_{0};
  int start_idx_{0};
  int last_included_idx_{0};
  int last_included_term_{0};
  std::vector<LogEntry> log_;

  std::unique_ptr<Snapshot> snapshot_;
  [[maybe_unused]] int last_applied_{0};
};

}  // namespace kv::raft
