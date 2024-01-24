#include "raft/raft.h"

#include <memory>

#include "common/logger.h"
#include "fmt/format.h"

namespace kv::raft {

Raft::Raft(std::vector<network::ClientEnd *> peers, uint32_t me,
           std::shared_ptr<storage::PersistentInterface> persister, apply_channel_ptr apply_channel)
    : peers_(peers), persister_(persister), me_(me) {
  //  Logger::Debug(kDTrace, me_, "....... Start .......");

  voter_ = std::make_shared<Voter>(peers, me_);
  lm_ = std::make_shared<LogManager>(me_, apply_channel);

  // initialize from state persisted before a crash
  auto state = persister_->ReadRaftState();
  if (state) {
    ReadPersistState(*state);
  }

  auto snap = persister_->ReadRaftSnapshot();
  if (snap) {
    ReadPersistSnap(*snap);
  }

  tickert_ = boost::fibers::fiber([&] { Ticker(); });
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
}

RequestVoteReply Raft::RequestVote(const RequestVoteArgs &args) {
  Logger::Debug(kDVote, me_, fmt::format("Receive request vote from S{} for term {}", args.candidate_, args.term_));
  RequestVoteReply reply;
  std::unique_lock l(mu_);
  if (args.term_ < term_) {
    Logger::Debug(kDDrop, me_,
                  fmt::format("Drop request vote from S{} since its term {} is smaller than mine {}", args.candidate_,
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

AppendEntryReply Raft::AppendEntries(const AppendEntryArgs &args) {
  Logger::Debug(kDInfo, me_,
                fmt::format("Receive AE from Leader {} for term {} commit {} prevLogIdx {} prevLogTerm {} hearbeat {} ",
                            args.leader_id_, args.leader_term_, args.commit_, args.prev_log_idx_, args.prev_log_term_,
                            args.hearbeat_));
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

  auto persister = [&]() { Persist(); };
  auto has_persisted = lm_->AppendEntries(args, &reply, persister);

  if (!has_persisted && persist_changes) {
    Persist();
  }

  return reply;
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
  InitMetaDataForLeader();
  tentative_cmit_index_[me_] = lm_->GetCommitIndex();
  l.unlock();

  Logger::Debug(kDTerm, me_, fmt::format("I am a leader now with term {}", term));

  if (hbt_.joinable()) {
    hbt_.detach();
  }
  hbt_ = boost::fibers::fiber([&] { BroadcastHeartBeats(); });

  if (ldwlt_.joinable()) {
    ldwlt_.detach();
  }
  ldwlt_ = boost::fibers::fiber([&] { LeaderWorkLoop(); });
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
  // first run, need to elect quickly
  auto ms1 = common::RandInt() % 50;
  common::SleepMs(ms1);
  if (voter_->LostConnectWithLeader()) {
    voter_->ResetElectionTimer();
    TransitionToCandidate();
    AttemptElection();
  }

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

void Raft::Persist(const Snapshot &snapshot) const {
  //  Logger::Debug(kDPersist, me_, "Persisting data ...");
  std::unique_lock l(mu_);
  RaftPersistState state;
  state.term_ = term_;
  state.voted_for_ = voter_->GetVoteFor();

  lm_->Lock();
  state.log_start_idx_ = lm_->DoGetStartIndex();
  state.last_included_idx_ = lm_->DoGetLastIncludedIndex();
  state.last_included_term_ = lm_->DoGetLastIncludedTerm();
  state.logs_ = lm_->DoGetLogs();
  lm_->Unlock();

  if (!snapshot.Empty()) {
    persister_->Save(state, snapshot);
  } else {
    persister_->SaveRaftState(state);
  }
}

void Raft::ReadPersistState(const RaftPersistState &state) {
  term_ = state.term_;
  voter_->VoteFor(state.voted_for_);
  lm_->DoSetStartIdx(state.log_start_idx_);
  lm_->DoSetLastIncludedIdx(state.last_included_idx_);
  lm_->DoSetLastIncludedTerm(state.last_included_term_);
  lm_->DoSetLogs(state.logs_);

  Logger::Debug(kDPersist, me_,
                fmt::format("Restore term {}, voted for {}, logStartIdx {}, lastLogIdx {}, lastLogTerm {}, "
                            "lastIncludedIdx {}, lastIncludedTerm {} from persisten state",
                            state.term_, state.voted_for_, state.log_start_idx_, lm_->DoGetLastLogIdx(),
                            lm_->GetLastLogTerm(), state.last_included_idx_, state.last_included_term_));
}

void Raft::ReadPersistSnap(const Snapshot &snap) {
  if (snap.Empty()) {
    Logger::Debug(kDWarn, me_, "There is no snapshot persisted");
    return;
  }

  lm_->DoSetSnapshot(snap);
}

}  // namespace kv::raft
