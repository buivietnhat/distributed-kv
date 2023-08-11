
#include "gtest/gtest.h"
#include "raft/configuration.h"

namespace kv::raft {

static constexpr int RAFT_ELECTION_TIMEOUT = 1000;

TEST(RaftVoteTest, DISABLED_InitializeEletection) {
  int servers = 3;
  Configuration cfg{servers, false, false};

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
}

TEST(RaftVoteTest, ReElection) {
  int servers = 3;
  Configuration cfg{servers, false, false};

  cfg.Begin("Test: election after network failure");

  auto leader1 = cfg.CheckOneLeader();
  EXPECT_NE(-1, leader1);

  // if the leader disconnect, a new one should be elected
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with Leader {}", leader1));
  cfg.Disconnect(leader1);
  EXPECT_NE(-1, cfg.CheckOneLeader());


//  EXPECT_TRUE(cfg.Cleanup());
}

}  // namespace kv::raft
