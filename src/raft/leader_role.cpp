#include <algorithm>

#include "common/logger.h"
#include "network/rpc_interface.h"
#include "raft/raft.h"

namespace kv::raft {

using common::Logger;

bool Raft::CheckOutdateAndTransitionToFollower(int current_term, int new_term) {
  if (new_term > current_term) {
    std::unique_lock l(mu_);
    if (term_ == current_term) {
      TransitionToFollower(new_term);
    }
    l.unlock();

    Persist();
    return true;
  }

  return false;
}

void Raft::SendHeartBeat(int server, int term) {
  AppendEntryArgs args;
  args.hearbeat_ = true;
  args.leader_id_ = me_;
  args.leader_term_ = term;

  auto reply = DoRequestAppendEntry(server, args);
  if (reply && !reply->success_) {
    CheckOutdateAndTransitionToFollower(term, reply->term_);
  }
}

void Raft::BroadcastHeartBeats() {
  while (!Killed()) {
    std::unique_lock l(mu_);
    if (role_ != LEADER) {
      return;
    }
    auto term = term_;
    l.unlock();

    for (uint32_t server = 0; server < peers_.size(); server++) {
      if (server != me_) {
        std::thread([&, server = server, term = term] { SendHeartBeat(server, term); }).detach();
        //        pool_.AddTask([&, server = server, term = term] { SendHeartBeat(server, term); });
      }
    }

    common::SleepMs(250);
  }
}

void Raft::LeaderWorkLoop() {
  while (!Killed()) {
    std::unique_lock l(mu_);

    if (role_ != LEADER) {
      return;
    }

    auto [replica_list, start_idx] = NeedToRequestAppend();
    if (!replica_list.empty()) {
      l.unlock();

      Logger::Debug(
          kDLeader, me_,
          fmt::format(
              "Sending request to AE for replicas {} up to index {} and startIdx {} tentativeNextIdx {} nextIdx {} ",
              common::ToString(replica_list), lm_->GetLastLogIdx(), start_idx, common::ToString(tentative_next_index_),
              common::ToString(next_index_)));
      RequestAppendEntries(replica_list, start_idx);
      l.lock();
    } else {
      //      Logger::Debug(kDInfo, me_, "No need to send AE since all the replica is up-to-date");
    }
    l.unlock();

    common::SleepMs(10);
  }
}

std::pair<std::vector<int>, int> Raft::NeedToRequestAppend() const {
  std::vector<int> replica_list;
  replica_list.reserve(peers_.size() - 1);

  lm_->Lock();
  auto last_log_idx = lm_->DoGetLastLogIdx();
  auto start_idx = lm_->DoGetStartIndex();
  lm_->Unlock();

  if (last_log_idx == 0) {
    return {replica_list, start_idx};
  }

  for (const auto &[server, next_idx] : tentative_next_index_) {
    // don't need to send log if the leader is sending snapshots
    if (next_idx <= last_log_idx) {
      replica_list.push_back(server);
    }
  }

  return {replica_list, start_idx};
}

int Raft::ComputePreviousIndexes(int old_pre_index, const AppendEntryReply &reply) const {
  // if the replica's log is too short
  if (old_pre_index >= reply.xlen_) {
    auto prev_log_idx = reply.xlen_ - 1;
    return prev_log_idx;
  }

  // if I don't have that term
  if (!lm_->HasTerm(reply.term_)) {
    auto prev_log_idx = reply.xindex_ - 1;
    return prev_log_idx;
  }

  // I have that term
  auto prev_log_idx = lm_->LastEntryFor(reply.term_) - 1;
  if (prev_log_idx == old_pre_index) {
    prev_log_idx -= 1;
  }
  return prev_log_idx;
}

AppendEntriesResult Raft::RequestAppendEntry(int server, int prev_log_idx, int prev_log_term, bool commit,
                                             int commit_idx) {
  AppendEntriesResult result;
  result.server_ = server;

  AppendEntryArgs args;
  args.hearbeat_ = false;
  args.commit_ = commit;
  args.leader_commit_idx_ = commit_idx;
  args.leader_id_ = me_;
  args.prev_log_idx_ = prev_log_idx;
  args.prev_log_term_ = prev_log_term;
  args.entries_ = lm_->GetEntries(prev_log_idx + 1);

  auto start_idx = lm_->GetStartIndex();

  std::unique_lock l(mu_);
  if (role_ != LEADER || Killed()) {
    Logger::Debug(kDLeader, me_, "Gave up the AE request since I am no longer a Leader");
    result.last_log_idx_ = -1;
    return result;
  }

  args.leader_term_ = term_;
  auto term = term_;
  l.unlock();

  Logger::Debug(kDLog, me_,
                fmt::format("Request server {} to append entries starting from Idx {}, Commit = {}", server,
                            prev_log_idx + 1, commit));

  auto reply = DoRequestAppendEntry(server, args);
  if (reply) {
    if (reply->success_) {
      // success, now the replica has fully replicated my logs
      if (commit) {
        Logger::Debug(kDLeader, me_, fmt::format("Successfully commit idx {} to server {}", commit_idx, server));
      } else {
        Logger::Debug(kDLeader, me_,
                      fmt::format("Successfully replicated my logs to server {} up to entry {}", server,
                                  prev_log_idx + args.entries_.size()));
      }

      result.last_log_idx_ = prev_log_idx + args.entries_.size();
      return result;
    }

    // the replica've rejected
    // no need to retry if it's a commit message
    if (commit) {
      result.last_log_idx_ = -1;
      l.lock();
      // reset the tentative commit index
      tentative_next_index_[server] = 0;
      l.unlock();
      return result;
    }

    // first to check if I am outdated
    if (CheckOutdateAndTransitionToFollower(term, reply->term_)) {
      Logger::Debug(
          kDLeader, me_,
          fmt::format("Gave up the AE request since there's a server {} has a bigger term {}", server, reply->term_));
      result.last_log_idx_ = -1;
      return result;
    }

    // there's must be conflicting entries, ... retry
    if (!Killed()) {
      auto new_prev_log_idx = ComputePreviousIndexes(prev_log_idx, *reply);
      if (new_prev_log_idx >= lm_->GetLastIncludedIndex()) {
        auto new_prev_log_term = lm_->GetTerm(new_prev_log_idx);
        Logger::Debug(
            kDLeader, me_,
            fmt::format("There's a conlicting entry for Server {} at idx {} and term {}, retry with new "
                        "prevLogIdx = {}, prevLogTerm = {}",
                        server, args.prev_log_idx_, args.prev_log_term_, new_prev_log_idx, new_prev_log_term));
        RequestAppendEntry(server, new_prev_log_idx, new_prev_log_term, false, 0);
      } else {
        Logger::Debug(
            kDTrace, me_,
            fmt::format("I no longer have the info for prev indexes for server {}, send snapshot instead", server));
        // TODO(nhat): implement this
        //            rf.sendLatesSnapshot(server)
      }
    }
  } else {
    if (!Killed()) {
      Logger::Debug(
          kDWarn, me_,
          fmt::format("Request to AE for Server {} for index {} term {} commit = {} has failed due to network error",
                      server, prev_log_idx + 1, prev_log_term, commit));

      if (lm_->GetStartIndex() != start_idx && commit) {
        Logger::Debug(kDWarn, me_,
                      fmt::format("Not retry to commit for server {} with index {} since I just installed snapshot",
                                  server, prev_log_idx + 1));
        result.last_log_idx_ = -1;
        return result;
      }

      // retry if it's commit msg
      if (commit) {
        Logger::Debug(kDLog, me_,
                      fmt::format("Retry to commit for server {} starting from Idx {}", server, prev_log_idx + 1));
        RequestAppendEntry(server, prev_log_idx, prev_log_term, commit, commit_idx);
      }
    }
  }

  result.last_log_idx_ = -1;
  return result;
}

std::pair<std::vector<int>, int> Raft::AnalyseToCommit(const std::unordered_map<int, int> &matched_result, int server) {
  std::vector<int> server_list;

  auto sorted_pair_match = common::SortByValue(matched_result);
  auto commit_index = sorted_pair_match[peers_.size() / 2].second;

  Logger::Debug(kDLog2, me_,
                fmt::format("List of Matched Index {}, should commit up to index {}",
                            common::ToString(sorted_pair_match), commit_index));

  auto leader_commit = tentative_cmit_index_[me_];
  if (leader_commit < lm_->GetComminIndex()) {
    throw RAFT_EXCEPTION(fmt::format("tentative commit idx %d is smaller than the actual commit idx %d", leader_commit,
                                     lm_->GetStartIndex()));
  }

  // if the index already been commited by majority of the servers
  if (leader_commit >= commit_index) {
    if (tentative_cmit_index_[server] < commit_index) {
      server_list.push_back(server);
      Logger::Debug(kDInfo, me_,
                    fmt::format("Set tentative cmit"
                                " idx for server {} to {}",
                                server, commit_index));
      tentative_next_index_[server] = commit_index;
    } else {
      Logger::Debug(kDLeader, me_,
                    fmt::format("Drop since already tried to cmit idx for server {} to {}", server,
                                tentative_cmit_index_[server]));
    }
    return {server_list, commit_index};
  }

  for (uint32_t s = 0; s < peers_.size(); s++) {
    server_list.push_back(s);
    tentative_cmit_index_[s] = commit_index;
    Logger::Debug(kDInfo, me_, fmt::format("Set tentative cmit idx for server {} to {}", s, commit_index));
  }

  return {server_list, commit_index};
}

void Raft::RequestAppendEntries(const std::vector<int> &replica_list, int start_idx) {
  auto logs_accepted = std::make_shared<uint32_t>(1);
  auto logs_finished = std::make_shared<uint32_t>(1);
  auto log_mu = std::make_shared<std::mutex>();
  auto log_cond = std::make_shared<std::condition_variable>();

  std::unique_lock l(mu_);
  auto last_log_idx = lm_->GetLastLogIdx();
  match_index_[me_] = last_log_idx;
  l.unlock();

  if (start_idx != lm_->GetStartIndex()) {
    Logger::Debug(kDDrop, me_, "Give up sending logs to ALL replicas since just installed snapshot");
    return;
  }

  for (auto server : replica_list) {
    pool_.AddTask([&, server, start_idx, log_mu, logs_accepted, logs_finished, last_log_idx, log_cond] {
      auto log_finish_func = [&] {
        std::unique_lock log_lock(*log_mu);
        *logs_finished += 1;
        log_lock.unlock();
        log_cond->notify_all();
      };

      std::unique_lock l(mu_);
      if (next_index_[server] >= last_log_idx + 1) {
        tentative_next_index_[server] = last_log_idx + 1;
        Logger::Debug(kDLeader, me_,
                      fmt::format("Gave up sending Logs to server {} since it has already been updated", server));
        log_finish_func();
        return;
      }

      auto prev_log_idx = next_index_[server] - 1;
      tentative_next_index_[server] = last_log_idx + 1;
      Logger::Debug(kDInfo, me_, fmt::format("Set tentative nextId for server {} to {}", server, last_log_idx + 1));
      l.unlock();

      if (prev_log_idx < lm_->GetLastIncludedIndex()) {
        Logger::Debug(kDTrace, me_, fmt::format("Replica {} is too lagged behind, send Snapshot instead", server));
        // TODO(nhat) implement this
        //        SendLatestSnapshot(server)
        log_finish_func();
        return;
      }

      lm_->Lock();
      if (lm_->DoGetStartIndex() != start_idx) {
        std::cout << "Current Log Start Idx " << lm_->DoGetStartIndex() << std::endl;
        std::cout << "start_idx " << start_idx << std::endl;
        lm_->Unlock();

        l.lock();
        tentative_next_index_[server] = last_log_idx + 1;
        Logger::Debug(kDTrace, me_,
                      fmt::format("Give up sending Logs to server {} since the snapshot just been installed", server));
        l.unlock();

        log_finish_func();
        return;
      }
      auto prev_log_term = lm_->DoGetTerm(prev_log_idx);
      lm_->Unlock();

      auto r = RequestAppendEntry(server, prev_log_idx, prev_log_term, false, 0);
      if (!IsLeader() || Killed()) {
        Logger::Debug(kDLeader, me_, "My internal state has changed, I gave up requesting appending logs to replicas");
        log_finish_func();
        return;
      }

      // the request has failed
      if (r.last_log_idx_ == -1) {
        l.lock();
        // set back the change
        tentative_next_index_[server] = next_index_[server];
        l.unlock();

        Logger::Debug(kDInfo, me_,
                      fmt::format("The request to replicate the log to server {} has fail, return ...", r.server_));
        log_finish_func();
        return;
      }

      // appended successfully
      l.lock();
      next_index_[r.server_] = std::max(next_index_[server], r.last_log_idx_ + 1);
      match_index_[r.server_] = std::max(r.last_log_idx_, match_index_[r.server_]);
      auto [commit_list, cmit_idx] = AnalyseToCommit(match_index_, r.server_);
      l.unlock();

      std::unique_lock log_lock(*log_mu);
      *logs_accepted += 1;
      *logs_finished += 1;
      log_lock.unlock();

      log_cond->notify_all();

      if (!commit_list.empty()) {
        RequestCommits(commit_list, cmit_idx, start_idx);
      }
    });
  }

  // wait for all servers to finish, or the majority of servers has replicated
  // or been killed, or not a leader anymore
  std::unique_lock log_lock(*log_mu);
  log_cond->wait(log_lock, [&] {
    return Killed() || !IsLeader() || *logs_accepted > peers_.size() / 2 || *logs_finished >= peers_.size();
  });

  Logger::Debug(kDTrace, me_,
                fmt::format("Request AEs return with Killed {}, IsLeader {}, LogsAccepted {}, LogFinished {}", Killed(),
                            IsLeader(), *logs_accepted, *logs_finished));
}

void Raft::RequestCommits(const std::vector<int> &server_list, int index, int start_idx) {
  std::unique_lock l(mu_);
  if (role_ != LEADER) {
    Logger::Debug(kDLeader, me_, "Gave up the commit request since I am no longer a Leader");
    return;
  }

  l.unlock();

  lm_->Lock();
  if (lm_->DoGetStartIndex() > index) {
    lm_->Unlock();
    Logger::Debug(kDDrop, me_, "Gave up the commit request since I just installed a snapshot");
    return;
  }

  auto prev_log_term = lm_->DoGetTerm(index);
  lm_->Unlock();

  bool self_commit = false;
  for (auto s : server_list) {
    if (s == static_cast<int>(me_)) {
      self_commit = true;
      continue;
    }

    pool_.AddTask([&, s, index, prev_log_term] { RequestCommit(s, index, prev_log_term); });
  }

  if (self_commit) {
    lm_->Lock();
    auto curr_commit_idx = lm_->DoGetCommitIndex();
    lm_->DoSetTentativeCommitIndex(std::max(curr_commit_idx, index));
    Logger::Debug(kDLeader, me_, fmt::format("Set tentative CMIT to {}", lm_->DoGetTentativeCommitIndex()));
    lm_->Unlock();

    Persist();
    if (curr_commit_idx < start_idx) {
      Logger::Debug(kDSnap, me_,
                    fmt::format("My StartIdx {} > CommitIdx {}, send ApplySnap first", start_idx, curr_commit_idx));
      // TODO(nhat): implement this
      //      rf.lm.applyLatestSnap()
      //
      //      // update new FromCommitIdx and retry
      //      currCommitIdx = rf.lm.CommitIndex()
    }

    lm_->CommitEntries(start_idx, curr_commit_idx + 1, index);
  }
}

void Raft::RequestCommit(int server, int index, int prev_log_term) {
  auto prev_log_idx = index;
  RequestAppendEntry(server, prev_log_idx, prev_log_term, true, index);
}

// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
std::tuple<int, int, bool> Raft::Start(std::any command) {
  std::unique_lock l(mu_);
  auto is_leader = (role_ == LEADER);
  if (is_leader) {
    auto term = term_;
    l.unlock();

    auto index = lm_->AppendLog(command, term);
    Logger::Debug(kDClient, me_, fmt::format("Start new Cmd at Index {} Term {}", index, term));
    return {index, term, true};
  }

  return {-1, -1, false};
}

std::optional<AppendEntryReply> Raft::DoRequestAppendEntry(int server, const AppendEntryArgs &args) const {
  return peers_[server]->AppendEntries(args);
}

}  // namespace kv::raft
