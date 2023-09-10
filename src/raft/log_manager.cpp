#include "raft/log_manager.h"

namespace kv::raft {

LogManager::LogManager(int me, apply_ch_t apply_channel)
    : me_(me), apply_ch_(apply_channel) {
  log_.push_back({0, {}});
}

std::vector<LogEntry> LogManager::GetEntries(int start_idx) const {
  std::unique_lock l(mu_);

  return DoGetEntries(start_idx);
}

std::vector<LogEntry> LogManager::DoGetEntries(int start_idx) const {
  start_idx = start_idx - start_idx_;
  if (start_idx < 0) {
    throw LOG_MANAGER_EXCEPTION(
        fmt::format("error out of bound get log from startIdx {}, logStartIdx {}", start_idx, start_idx_));
  }

  if (start_idx >= static_cast<int>(log_.size())) {
    return {};
  }

  std::vector<LogEntry> entries;
  entries.reserve(log_.size() - start_idx);
  for (uint32_t i = start_idx; i < log_.size(); i++) {
    entries.push_back(log_[i]);
  }
  return entries;
}

void LogManager::AddNewEntries(int index, const std::vector<LogEntry> &entries) {
  if (log_.empty()) {
    if (index != last_included_idx_ + 1) {
      throw LOG_MANAGER_EXCEPTION(
          "Append Entry is not appropriate, at this point should be valid for snapshot previous index");
    }

    log_ = entries;
    Logger::Debug(kDLog2, me_, fmt::format("Append new entry from index {} to {}", index, index + entries.size() - 1));
    return;
  }

  index = index - start_idx_;
  if (index < 0) {
    throw LOG_MANAGER_EXCEPTION(
        fmt::format("error out of bound add new entry from index {}, logStartIdx {}", index, start_idx_));
  }
  if (index > static_cast<int>(log_.size())) {
    throw LOG_MANAGER_EXCEPTION(
        fmt::format("error trying to add a new index {} out of range (len = {})", index, log_.size()));
  }

  if (index + entries.size() < log_.size()) {
    // the new entries cover newer term, throw away the redundant logs
    if (entries[entries.size() - 1].term_ > log_[log_.size() - 1].term_) {
      while (log_.size() > index + entries.size()) {
        log_.pop_back();
      }
    }
  }

  uint32_t entries_idx = 0;
  if (index < static_cast<int>(log_.size())) {
    auto replace_len = std::min(log_.size() - index, entries.size());
    for (; entries_idx < replace_len; entries_idx++) {
      log_[index + entries_idx] = entries[entries_idx];
    }
  }

  for (; entries_idx < entries.size(); entries_idx++) {
    log_.push_back(entries[entries_idx]);
  }

  Logger::Debug(
      kDLog2, me_,
      fmt::format("Append new entry from index {} to {}", index + start_idx_, index + start_idx_ + entries.size() - 1));
}

void LogManager::ApplySnap(int snapshot_idx, int snapshot_term, const Snapshot &snap) {
  std::lock_guard apply_lock(apply_mu_);

  std::unique_lock l(mu_);
  if (commid_idx_ >= snapshot_idx) {
    Logger::Debug(
        kDSnap, me_,
        fmt::format("Drop the ApplySnap since the commit idx is already {} >= SnapIdx({})", commid_idx_, snapshot_idx));
    return;
  }
  l.unlock();

  ApplyMsg apply_msg;

  apply_msg.snapshot_valid_ = true;
  apply_msg.snapshot_index_ = snapshot_idx;
  apply_msg.snapshot_term_ = snapshot_term;
  apply_msg.snapshot_ = snap;

  apply_ch_->Enqueue(apply_msg);

  l.lock();
  commid_idx_ = snapshot_idx;
  l.unlock();

  Logger::Debug(kDCommit, me_, fmt::format("New Index idx {} applied Snap with Term {}", snapshot_idx, snapshot_term));
}

void LogManager::CommitEntries(int start_idx, int from_idx, int to_idx) {
  if (from_idx > to_idx || start_idx > from_idx) {
    Logger::Debug(kDTrace, me_,
                  fmt::format("Gave up commit since the idx is not valid startIdx {}, fromIdx {}, toIdx {}", start_idx,
                              from_idx, to_idx));
    return;
  }

  std::lock_guard apply_lock(apply_mu_);
  std::unique_lock l(mu_);
  if (commid_idx_ >= from_idx || from_idx < start_idx_) {
    Logger::Debug(kDDrop, me_,
                  "Someone has tried to commited the newer index or just installed snapshot, I should return now");
    return;
  }

  auto entries = CopyLog(from_idx - start_idx_, to_idx - start_idx);
  l.unlock();

  for (int idx = from_idx; idx <= to_idx; idx++) {
    ApplyMsg applymsg;
    applymsg.command_valid_ = true;
    applymsg.command_index_ = idx;
    applymsg.command_ = entries[idx - from_idx].command_;

    l.lock();
    commid_idx_ = idx;
    l.unlock();
    Logger::Debug(kDCommit, me_,
                  fmt::format("New Index {} committed with Term {}", idx, entries[idx - from_idx].term_));
    apply_ch_->Enqueue(applymsg);
  }
}

// copy from from_idx to to_idx, inclusion
std::vector<LogEntry> LogManager::CopyLog(int from_idx, int to_idx) const {
  if (from_idx < 0 || to_idx >= static_cast<int>(log_.size()) || from_idx > to_idx) {
    throw LOG_MANAGER_EXCEPTION("invalid copy argument");
  }
  std::vector<LogEntry> copy;
  copy.reserve(to_idx - from_idx + 1);
  for (int i = from_idx; i <= to_idx; i++) {
    copy.push_back(log_[i]);
  }

  return copy;
}

bool LogManager::HasTerm(int term) const {
  std::lock_guard l(mu_);
  int index = log_.size() - 1;
  while (index >= 0) {
    if (log_[index].term_ == term) {
      return true;
    }
    --index;
  }
  Logger::Debug(kDTrace, me_, fmt::format("Couldn't find the entry for term {}", term));
  return false;
}

int LogManager::LastEntryFor(int term) const {
  std::lock_guard l(mu_);

  int index = log_.size() - 1;
  while (index > 0) {
    if (log_[index].term_ == term) {
      return index + start_idx_;
    }
    index--;
  }

  Logger::Debug(kDTrace, me_, fmt::format("Couldn't find the entry for term {}", term));
  return index;
}

void LogManager::GenerateConflictingMsgReply(int conflicting_index, AppendEntryReply *reply) {
  // first to find the first entry of the conflicting term
  auto actual_conflicting_idx = conflicting_index - start_idx_;

  if (actual_conflicting_idx < 0) {
    // just simply discard it, since already installed the snapshot
    return;
  }

  if (actual_conflicting_idx < static_cast<int>(log_.size())) {
    auto index = actual_conflicting_idx;
    auto conflicting_term = log_[index].term_;
    while (log_[index].term_ == conflicting_term && index > 0) {
      index--;
    }
    reply->xindex_ = index + 1 + start_idx_;
    reply->term_ = conflicting_term;
  }

  reply->success_ = false;
  reply->xlen_ = log_.size() + start_idx_;
}

bool LogManager::AppendEntries(const AppendEntryArgs &args, AppendEntryReply *reply,
                               std::function<void(void)> persister) {
  std::unique_lock l(mu_);
  if (!IsEntryExist(args.prev_log_idx_, args.prev_log_term_)) {
    GenerateConflictingMsgReply(args.prev_log_idx_, reply);
    Logger::Debug(
        kDInfo, me_,
        fmt::format("I don't have the entry with idx {} and term {}, return with XIndex = {}, Term = {}, XLen = {}",
                    args.prev_log_idx_, args.prev_log_term_, reply->xindex_, reply->term_, reply->xlen_));
    return false;
  }

  if (args.commit_) {
    if (!IsCommitIdxValid(args.leader_commit_idx_)) {
      Logger::Debug(
          kDInfo, me_,
          fmt::format("Drop the commit message since the LeaderCmdIdx {} is not valid (my current TentativeComIdx {})",
                      args.leader_commit_idx_, tentative_commit_idx_));
      return false;
    }
    auto from_commit_idx = commid_idx_ + 1;
    auto to_commit_idx = std::min(args.leader_commit_idx_, static_cast<int>(log_.size() - 1 + start_idx_));
    Logger::Debug(kDLog2, me_,
                  fmt::format("Trying to commit from Index {} to Index {}", from_commit_idx, to_commit_idx));
    // set the tentative commit index so that other threads won't retry
    tentative_commit_idx_ = to_commit_idx;
    auto start_idx = start_idx_;
    l.unlock();

    // persist the log before commit
    persister();

    // maybe I am just restart and lost the cmmit infomation
    if (start_idx > from_commit_idx + 1) {
      Logger::Debug(kDSnap, me_,
                    fmt::format("My StartIdx {} > CommitIdx {}, ApplySnap first", start_idx, from_commit_idx));
      ApplyLatestSnap();

      // update new FromCommitIdx and retry
      from_commit_idx = GetCommitIndex() + 1;
    }

    CommitEntries(start_idx, from_commit_idx, to_commit_idx);
    reply->success_ = true;
    return false;
  }

  AddNewEntries(args.prev_log_idx_ + 1, args.entries_);
  l.unlock();

  persister();

  reply->success_ = true;
  return true;
}

void LogManager::DoSetSnapshot(const Snapshot &snap) { snapshot_ = std::make_unique<Snapshot>(snap); }

void LogManager::DoDiscardLogs(int to_idx, int last_included_term) {
  auto actual_to_idx = to_idx - start_idx_;
  if (actual_to_idx < 0) {
    Logger::Debug(kDSnap, me_,
                  fmt::format("Already discarded up to index {} > demanding index {}", start_idx_, to_idx));
    return;
  }

  if (actual_to_idx + 1 >= static_cast<int>(log_.size())) {
    Logger::Debug(
        kDSnap, me_,
        fmt::format("Discard toIdx {} greater the len of the Log {} startIdx {}", to_idx, log_.size(), start_idx_));
    start_idx_ = to_idx + 1;
    log_ = {};
    Logger::Debug(kDSnap, me_, fmt::format("Set Start Idx to {}, Discared the entire Log", to_idx + 1, log_.size()));
    last_included_idx_ = to_idx;
    last_included_term_ = last_included_term;
    return;
  }

  log_ = std::vector<LogEntry>(log_.begin() + actual_to_idx + 1, log_.end());
  start_idx_ = to_idx + 1;
  last_included_idx_ = to_idx;
  last_included_term_ = last_included_term;
  Logger::Debug(kDSnap, me_, fmt::format("Set Start Idx to {}, LogEntry len now {}", start_idx_, log_.size()));
}

void LogManager::ApplyLatestSnap() {
  std::unique_lock l(mu_);
  auto last_included_index = last_included_idx_;
  auto last_included_term = last_included_term_;
  if (snapshot_ == nullptr) {
    throw LOG_MANAGER_EXCEPTION("trying to apply the nullptr snapshot");
  }
  auto data = *snapshot_;
  l.unlock();

  ApplySnap(last_included_index, last_included_term, data);
}

}  // namespace kv::raft