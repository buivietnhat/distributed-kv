
#include "gtest/gtest.h"
#include "raft/configuration.h"

namespace kv::raft {

static constexpr int RAFT_ELECTION_TIMEOUT = 1000;

TEST(RaftVoteTest, InitializeEletection) {
  int num_server = 3;
  Configuration cfg{num_server, false, false};

  cfg.Begin("Test: initial election");

  // is a Leader elected?
  EXPECT_NE(-1, cfg.CheckOneLeader());

  // sleep a bit to avoid racing with followers learing of the election, then check that all peers agree on the term
  common::SleepMs(50);
  auto term1 = cfg.CheckTerm();
  EXPECT_TRUE(term1);
  if (*term1 < 1) {
    Logger::Debug(kDTest, -1, fmt::format("term is {}, but should be at least 1", *term1));
    FAIL();
  }

  // does the leader+term stay the same if there is no network failure?
  common::SleepMs(2 * RAFT_ELECTION_TIMEOUT);
  auto term2 = cfg.CheckTerm();
  EXPECT_TRUE(term2);
  if (*term1 != *term2) {
    Logger::Debug(kDTest, -1, "warning: term changed even though there were no failures");
  }

  // there should still be a leader
  EXPECT_NE(-1, cfg.CheckOneLeader());
  EXPECT_TRUE(cfg.Cleanup());
  Logger::Debug(kDTest, -1, "  ... Passed --");
}

}  // namespace kv::raft
