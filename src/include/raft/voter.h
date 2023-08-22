#pragma once

#include <mutex>

#include "common/thread_pool.h"
#include "common/util.h"
#include "raft/common.h"

namespace kv::network {

class ClientEnd;

}  // namespace kv::network

namespace kv::raft {

class Voter {
 public:
  Voter(std::vector<network::ClientEnd *> peers, int me);

  ~Voter();

  bool IsCandidateUpToDate(const InternalState &state, int candidate_last_logidx, int candidate_last_log_term) const;

  // return true if we need to persist the changes
  bool RequestVote(const InternalState &state, const RequestVoteArgs &args, RequestVoteReply *reply);

  std::optional<VoteResult> DoRequestVote(std::shared_ptr<InternalState> state, int server);

  std::pair<bool, int> AttemptElection(std::shared_ptr<InternalState> state);

  bool LostConnectWithLeader() const;

  void ResetElectionTimer();

  inline void GiveUp() { give_up_ = true; }

  inline int GetVoteFor() const {
    std::lock_guard lock(mu_);
    return voted_for_;
  }

  inline void VoteFor(int server) {
    std::lock_guard lock(mu_);
    voted_for_ = server;
  }

 private:
  std::optional<RequestVoteReply> SendRequestVote(int server, const RequestVoteArgs &args) const;

  inline void TryAgain() { give_up_ = false; }

  inline bool IsGaveUp() const { return give_up_; }

  inline bool Killed() { return dead_; }

  inline void Kill() { dead_ = true; }

  std::vector<network::ClientEnd *> peers_;
  mutable std::mutex mu_;
  int voted_for_{-1};
  std::atomic<bool> dead_{false};
  common::time_t last_heard_from_leader_;
  std::atomic<bool> give_up_{false};
  uint32_t me_;
  common::ThreadPool pool_;

  static constexpr int MAX_WAIT_TIME = 500;
  static constexpr int NUM_THREAD = 5;
};

}  // namespace kv::raft
