#include "common/logger.h"
#include "network/client_end.h"
#include "raft/raft.h"

namespace kv::raft {

void Raft::DoSnapshot(int index, const Snapshot &snapshot) {
  Logger::Debug(
      kDSnap, me_,
      fmt::format("Install Snapshot upto Index {} From Application, Snapshot len = {}", index, snapshot.data_.size()));

  lm_->Lock();
  auto last_included_idx = lm_->DoGetLastIncludedIndex();
  if (last_included_idx >= index) {
    Logger::Debug(
        kDSnap, me_,
        fmt::format("Drop the install snapshot request with index {} since I have already installed up to index {}",
                    index, last_included_idx));
    lm_->Unlock();
    return;
  }

  auto cmit_idx = lm_->DoGetCommitIndex();
  auto last_log_idx = lm_->DoGetLastLogIdx();
  if (index > cmit_idx || index > last_log_idx) {
    Logger::Debug(
        kDTrace, me_,
        fmt::format("Drop the install snapshot request since I haven't commited up to index {} yet ({}) lastLogIdx {}",
                    index, cmit_idx, last_log_idx));
    return;
  }

  auto term = lm_->DoGetTerm(index);
  lm_->DoDiscardLogs(index, term);
  lm_->DoSetSnapshot(snapshot);
  lm_->Unlock();

  Persist(snapshot);

  if (IsLeader()) {
    CheckAndSendInstallSnapshot(index, snapshot);
  }
}

InstallSnapshotReply Raft::InstallSnapshot(const InstallSnapshotArgs &args) {
  Logger::Debug(kDSnap, me_,
                fmt::format("Receive Request to install Snapshot from Leader {}, Leader Term {}, LastIncludedIndex {}, "
                            "LastIncludedTerm {}, Snapshot len = {}",
                            args.leader_id_, args.leader_term_, args.last_included_index_, args.last_included_term_,
                            args.data_.Size()));
  InstallSnapshotReply reply;

  std::unique_lock l(mu_);
  if (term_ > args.leader_term_) {
    reply.term_ = term_;
    return reply;
  }

  if (term_ < args.leader_term_) {
    TransitionToFollower(args.leader_term_);
  }

  voter_->ResetElectionTimer();
  l.unlock();

  auto last_included_index = lm_->GetLastIncludedIndex();
  if (args.last_included_index_ <= last_included_index) {
    Logger::Debug(kDDrop, me_,
                  fmt::format("Drop the Install Snapshot for index {} since my lastIncludedIndex {}",
                              args.last_included_index_, last_included_index));
    return reply;
  }

  auto ten_commit_idx = lm_->GetTentativeCommitIndex();
  if (args.last_included_index_ < ten_commit_idx) {
    Logger::Debug(kDDrop, me_,
                  fmt::format("Drop Snapshot: The lastIncludedIdx {} is less than my current tentativeCmitIdx {}",
                              args.last_included_index_, ten_commit_idx));
    return reply;
  }

  lm_->Lock();
  lm_->DoDiscardLogs(args.last_included_index_, args.last_included_term_);
  lm_->DoSetTentativeCommitIndex(args.last_included_index_);
  lm_->DoSetSnapshot(args.data_);
  Logger::Debug(kDSnap, me_, fmt::format("Set tentative cmit to {}", lm_->DoGetTentativeCommitIndex()));
  lm_->Unlock();

  Persist(args.data_);

  lm_->ApplySnap(args.last_included_index_, args.last_included_term_, args.data_);

  return reply;
}

std::optional<InstallSnapshotReply> Raft::RequestInstallSnapshot(int server, const InstallSnapshotArgs &args) const {
  return peers_[server]->InstallSnapshot(args);
}

}  // namespace kv::raft