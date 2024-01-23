
#include "raft/voter.h"

#include <condition_variable>

#include "common/logger.h"
#include "common/util.h"
#include "fmt/format.h"
#include "network/client_end.h"
#include "raft/raft.h"

namespace kv::raft {

Voter::Voter(std::vector<network::ClientEnd *> peers, int me) : peers_(std::move(peers)), me_(me) {
  // to attempt to election right away
  last_heard_from_leader_ = common::AddTimeMs(common::Now(), MS(-500));
}

bool Voter::IsCandidateUpToDate(const InternalState &state, int candidate_last_logidx,
                                int candidate_last_log_term) const {
  if (state.last_log_term_ > candidate_last_log_term) {
    return false;
  }
  if (state.last_log_term_ == candidate_last_log_term && state.last_log_index_ > candidate_last_logidx) {
    return false;
  }
  return true;
}

bool Voter::RequestVote(const InternalState &state, const RequestVoteArgs &args, RequestVoteReply *reply) {
  auto persist_changes = false;

  std::unique_lock l(mu_);
  if (args.term_ == state.term_ && voted_for_ != -1) {
    Logger::Debug(kDInfo, me_,
                  fmt::format("Drop request vote from S{} since I've already voted for another S{}", args.candidate_,
                              voted_for_));
    reply->vote_granted_ = false;
    return persist_changes;
  }

  if (IsCandidateUpToDate(state, args.last_log_index_, args.last_log_term_)) {
    voted_for_ = args.candidate_;
    Logger::Debug(kDVote, me_, fmt::format("Granted vote for {}", args.candidate_));
    Logger::Debug(kDVote, me_,
                  fmt::format("Mine: lastLogTerm {}, lastLogidx {}, Candidate: lastLogTerm {}, lastLogIdx {}",
                              state.last_log_term_, state.last_log_index_, args.last_log_term_, args.last_log_index_));

    reply->vote_granted_ = true;
    last_heard_from_leader_ = common::Now();
    persist_changes = true;
    return persist_changes;
  }

  Logger::Debug(kDInfo, me_,
                fmt::format("Drop Vote request since the candidate {} is not Up-To-Date as Mine: "
                            "lastLogTerm {}, lastLogidx {}, Candidate: lastLogTerm {}, lastLogIdx {}",
                            args.candidate_, state.last_log_term_, state.last_log_index_, args.last_log_term_,
                            args.last_log_index_));

  return persist_changes;
}

std::optional<VoteResult> Voter::DoRequestVote(std::shared_ptr<InternalState> state, int server) {
  Logger::Debug(kDVote, me_, fmt::format("Sending request vote to S{}", server));
  RequestVoteArgs args;
  args.term_ = state->term_;
  args.last_log_index_ = state->last_log_index_;
  args.last_log_term_ = state->last_log_term_;
  args.candidate_ = me_;

  auto reply = SendRequestVote(server, args);

  Logger::Debug(kDVote, me_, fmt::format("Sending request vote to S{} finished", server));
  if (reply) {
    VoteResult result;
    result.term_ = reply->term_;
    result.vote_granted_ = reply->vote_granted_;
    result.server_ = server;
    return result;
  }

  return {};
}

std::optional<RequestVoteReply> Voter::SendRequestVote(int server, const RequestVoteArgs &args) const {
  return peers_[server]->RequestVote(args);
}

std::pair<bool, int> Voter::AttemptElection(std::shared_ptr<InternalState> state) {
  uint32_t vote_count = 1;
  uint32_t vote_finish = 1;

  // never give up :)
  TryAgain();

  auto done = std::make_shared<bool>(false);
  auto vote_channel = std::make_shared<boost::fibers::unbuffered_channel<VoteResult>>();

  for (size_t server = 0; server < peers_.size(); server++) {
    if (server != me_) {
      boost::fibers::fiber([me = shared_from_this(), state, server, done, chan = vote_channel] {
        auto vote_result = me->DoRequestVote(state, server);
        if (*done) {
          return;
        }
        if (vote_result) {
          chan->push(*vote_result);
        } else {
          chan->push({});
        }
      }).detach();
    }
  }

  while (!Killed()) {
    VoteResult result;
    vote_channel->pop(result);

    ON_SCOPE_EXIT {
      vote_channel->close();
    };

    if (IsGaveUp() || Killed()) {
      *done = true;
      Logger::Debug(kDVote, me_, "Well I have gave up or been Killed, I'm done with those election");
      VoteFor(-1);
      return {false, result.term_};
    }

    if (result.vote_granted_) {
      vote_count += 1;
      Logger::Debug(kDVote, me_, fmt::format("Yay, got a vote from S{}", result.server_));
    } else {
      if (result.term_ > state->term_) {
        Logger::Debug(kDVote, me_, fmt::format("Well there's a S{} who has bigger term that mine", result.server_));
        return {false, result.term_};
      }
    }

    vote_finish += 1;
    if (vote_count > peers_.size() / 2) {
      Logger::Debug(kDVote, me_, fmt::format("I have collected enough vote now ({}), returning", vote_count));
      *done = true;
      return {true, state->term_};
    }

    if (vote_finish == peers_.size()) {
      Logger::Debug(kDVote, me_, "Well I have tried by haven't collected enough vote, return...");
      *done = true;
      break;
    }
  }

  return {false, state->term_};
}

bool Voter::LostConnectWithLeader() const {
  std::lock_guard lock(mu_);

  return common::ElapsedTimeMs(last_heard_from_leader_, common::Now()) >= MAX_WAIT_TIME;
}

void Voter::ResetElectionTimer() {
  std::lock_guard lock(mu_);
  last_heard_from_leader_ = common::Now();
}

Voter::~Voter() {
  std::cout << me_ << " voter returning ..." << std::endl;
}

}  // namespace kv::raft