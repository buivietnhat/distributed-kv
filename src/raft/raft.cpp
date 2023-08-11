#include "raft/raft.h"

#include <memory>

#include "common/logger.h"
#include "fmt/format.h"
#include "network/rpc_interface.h"
#include "storage/persistent_interface.h"

namespace kv::raft {

using common::Logger;

Raft::Raft(std::vector<network::ClientEnd *> peers, uint32_t me, storage::PersistentInterface *persister,
           std::shared_ptr<common::ConcurrentBlockingQueue<ApplyMsg>> apply_channel)
    : peers_(peers), persister_(persister), me_(me) {
  Logger::Debug(kDTrace, me_, "....... Start .......");

  voter_ = std::make_unique<Voter>(peers, me_);

  tickert_ = std::thread([&] { Ticker(); });
}

RequestVoteReply Raft::RequestVote(const RequestVoteArgs &args) {
  Logger::Debug(kDInfo, me_, fmt::format("Receive request vote from S{} for term {}", args.candidate_, args.term_));
  RequestVoteReply reply;
  std::unique_lock l(mu_);
  if (args.term_ < term_) {
    Logger::Debug(kDDrop, me_,
                  fmt::format("Drop request vote from S%{} since its term {} is smaller than mine {}", args.candidate_,
                              args.term_, term_));
    reply.vote_granted_ = false;
    reply.term_ = term_;
    return reply;
  }

  auto persist_changes = false;
  if (term_ < args.term_) {
    TransitionToFollower(args.term_);
    voter_->VoteFor(-1);
    persist_changes = true;
  }
  l.unlock();

  auto state = CaptureCurrentState();
  persist_changes = voter_->RequestVote(*state, args, &reply);

  if (persist_changes) {
    Persist();
  }

  return reply;
}

Raft::~Raft() {
  if (!Killed()) {
    dead_ = true;
  }

  if (tickert_.joinable()) {
    tickert_.join();
  }

  if (hbt_.joinable()) {
    hbt_.join();
  }

  if (ldwlt_.joinable()) {
    ldwlt_.join();
  }

  group_.wait();
}

AppendEntryReply Raft::AppendEntries(const AppendEntryArgs &args) {
  Logger::Debug(kDInfo, me_,
                fmt::format("Receive AE from Leader {} for term {} commit {} prevLogIdx {} prevLogTerm {} ",
                            args.leader_id_, args.leader_term_, args.commit_, args.prev_log_idx_, args.prev_log_term_));
  bool persist_changes = false;
  AppendEntryReply reply;

  std::unique_lock l(mu_);
  if (IsLeaderOutdate(term_, args.leader_term_)) {
    Logger::Debug(kDLog, me_,
                  fmt::format("Leader {} has smaller Term {} than mine {}, return ...", args.leader_id_,
                              args.leader_term_, term_));
    reply.success_ = false;
    reply.term_ = term_;
    return reply;
  }

  voter_->ResetElectionTimer();
  if ((term_ < args.leader_term_) || (term_ == args.leader_term_ && role_ != FOLLOWER)) {
    TransitionToFollower(args.leader_term_);
    persist_changes = true;
  }
  l.unlock();

  if (args.hearbeat_) {
    reply.success_ = true;
    if (persist_changes) {
      Persist();
    }
    return reply;
  }

  // TODO(nhat): append logs

  return AppendEntryReply();
}

RaftState Raft::GetState() const {
  RaftState state;
  std::lock_guard lock(mu_);

  state.term_ = term_;
  state.is_leader_ = (role_ == LEADER);

  return state;
}

std::shared_ptr<InternalState> Raft::CaptureCurrentState() const {
  auto state = std::make_shared<InternalState>();
  std::lock_guard lock(mu_);

  state->term_ = term_;
  state->last_log_index_ = lm_->GetLastLogIdx();
  state->last_log_term_ = lm_->GetLastLogTerm();
  state->role = role_;

  return state;
}

void Raft::TransitionToFollower(int new_term) {
  Logger::Debug(kDInfo, me_, fmt::format("Transitioning to Follower with term {}", new_term));
  if (role_ == CANDIDATE) {
    voter_->GiveUp();
  }

  term_ = new_term;
  role_ = FOLLOWER;
}

void Raft::TransitionToCandidate() {
  std::unique_lock l(mu_);
  role_ = CANDIDATE;
  term_ += 1;
  l.unlock();

  voter_->VoteFor(me_);
  Persist();
}

void Raft::TransitionToLeader() {
  std::unique_lock l(mu_);
  role_ = LEADER;
  auto term = term_;
  // TODO(nhat) implement this
  //  InitMetadataForLeader();
  //  tentative_cmit_index_[me_] = lm_->CommitIndex();
  l.unlock();

  Logger::Debug(kDTerm, me_, fmt::format("I am a leader now with term {}", term));

  hbt_ = std::thread([&] { BroadcastHeartBeats(); });

  ldwlt_ = std::thread([&] { LeaderWorkLoop(); });
}

void Raft::AttemptElection() {
  auto state = CaptureCurrentState();
  if (state->role != CANDIDATE) {
    Logger::Debug(kDTrck, me_,
                  fmt::format("Give up election since my internal state has changed from Candidate to {}",
                              ToString(state->role)));
    return;
  }

  Logger::Debug(kDVote, me_,
                fmt::format("Time's up, start a new election now with term {}, lastLogIdx {}, lastLogTerm {}",
                            state->term_, state->last_log_index_, state->last_log_term_));

  auto [succeeded, new_term] = voter_->AttemptElection(state);
  if (succeeded) {
    TransitionToLeader();
  }
}

void Raft::Ticker() {
  while (!Killed()) {
    // pause for a random amount of time between 50 and 350 milliseconds.
    auto ms = 50 + common::RandInt() % 300;
    common::SleepMs(ms);

    // check if leader election should be started
    std::unique_lock l(mu_);
    auto role = role_;
    l.unlock();

    if (role != LEADER) {
      if (voter_->LostConnectWithLeader()) {
        voter_->ResetElectionTimer();
        TransitionToCandidate();
        AttemptElection();
      }
    } else {
      voter_->ResetElectionTimer();
    }
  }
}

void Raft::Persist(std::vector<std::byte> snapshot) const {}

}  // namespace kv::raft
