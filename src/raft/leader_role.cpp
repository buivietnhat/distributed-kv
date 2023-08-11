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

  auto reply = RequestAppendEntries(server, args);
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
        group_.run([&, server = server, term = term] { SendHeartBeat(server, term); });
      }
    }

    common::SleepMs(150);
  }
}

void Raft::LeaderWorkLoop() {
  //  std::unique_lock l(mu_, std::defer_lock);
  while (!Killed()) {
    //    mu_.lock();
    //
    //    if (role_ != LEADER) {
    //      return;
    //    }
  }
}

std::optional<AppendEntryReply> Raft::RequestAppendEntries(int server, const AppendEntryArgs &args) const {
  return peers_[server]->AppendEntries(args);
}

}  // namespace kv::raft
