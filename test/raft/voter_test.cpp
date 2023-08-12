
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

  // if the old leader rejoins, that shouldn't
  // disturb the new leader. and the old leader
  // should switch to follower.
  Logger::Debug(kDTest, -1, fmt::format("Connect with Leader {}", leader1));
  cfg.Connect(leader1);
  auto leader2 = cfg.CheckOneLeader();
  EXPECT_NE(-1, leader2);

  // if there's no quorum, no new leader should
  // be elected.
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with Leader {}", leader2));
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with Leader {}", (leader2 + 1) % servers));
  cfg.Disconnect(leader2);
  cfg.Disconnect((leader2 + 1) % servers);
  common::SleepMs(2 * RAFT_ELECTION_TIMEOUT);

  // check that the one connected server
  // does not think it is the leader.
  EXPECT_TRUE(cfg.CheckNoLeader());

  // if a quorum arises, it should elect a leader.
  Logger::Debug(kDTest, -1, fmt::format("Connect with Server {}", (leader2 + 1) % servers));
  cfg.Connect((leader2 + 1) % servers);
  EXPECT_NE(-1, cfg.CheckOneLeader());

  // re-join of last node shouldn't prevent leader from existing
  Logger::Debug(kDTest, -1, fmt::format("Connect with Server {}", leader2));
  cfg.Connect(leader2);
  EXPECT_NE(-1, cfg.CheckOneLeader());

  EXPECT_TRUE(cfg.Cleanup());
}

TEST(RaftVoteTest, DISABLED_ManyElections) {
  int servers = 7;
  Configuration cfg{servers, false, false};

  cfg.Begin("Test: multiple elections");

  int iters = 10;
  for (int ii = 1; ii < iters; ii++) {
    Logger::Debug(kDTest, -1, fmt::format("Iteration {}", ii));
    // disconnect three nodes
    auto i1 = common::RandInt() % servers;
    auto i2 = common::RandInt() % servers;
    auto i3 = common::RandInt() % servers;

    Logger::Debug(kDTest, -1, fmt::format("Disconnect with S{}", i1));
    cfg.Disconnect(i1);
    Logger::Debug(kDTest, -1, fmt::format("Disconnect with S{}", i2));
    cfg.Disconnect(i3);
    Logger::Debug(kDTest, -1, fmt::format("Disconnect with S{}", i3));
    cfg.Disconnect(i3);

    // either the current leader should still be alive.
    // or the remaining four should elect a new one
    EXPECT_NE(-1, cfg.CheckOneLeader());

    Logger::Debug(kDTest, -1, fmt::format("Connect with S{}", i1));
    cfg.Connect(i1);
    Logger::Debug(kDTest, -1, fmt::format("Connect with S{}", i2));
    cfg.Connect(i2);
    Logger::Debug(kDTest, -1, fmt::format("Connect with S{}", i3));
    cfg.Connect(i3);
  }

  EXPECT_NE(-1, cfg.CheckOneLeader());
  EXPECT_TRUE(cfg.Cleanup());
}

}  // namespace kv::raft
