#include "gtest/gtest.h"
#include "raft/configuration.h"

namespace kv::raft {

TEST(RaftLogTest, DISABLED_BasicAgreement) {
  int servers = 3;
  Configuration<int> cfg{servers, false, false};

  cfg.Begin("Test: basic agreement");

  int iters = 3;
  for (int index = 1; index < iters + 1; index++) {
    auto [nd, _] = cfg.NCommited(index);
    EXPECT_EQ(nd, 0);
    if (nd > 0) {
      throw CONFIG_EXCEPTION("some have committed before Start()");
    }

    auto xindex = cfg.One(index * 100, servers, false);
    EXPECT_EQ(xindex, index);
    if (xindex != index) {
      throw CONFIG_EXCEPTION(fmt::format("got index {} but expected {}", xindex, index));
    }
  }

  EXPECT_TRUE(cfg.Cleanup());
}

TEST(RaftLogTest, FollowerFailure) {
  int servers = 3;
  Configuration<int> cfg{servers, false, false};

  cfg.Begin("Test: test progressive failure of followers");

  cfg.One(101, servers, false);

  // disconnect one follower from the network
  auto leader1 = cfg.CheckOneLeader();
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with Server {}", (leader1 + 1) % servers));
  cfg.Disconnect((leader1 + 1) % servers);

  // the leader and remaining folloer should be able to agree despite the disconnected follower
  cfg.One(102, servers - 1, false);
  common::SleepMs(RAFT_ELECTION_TIMEOUT);
  cfg.One(103, servers - 1, false);

  // disconnect the remaining follower
  auto leader2 = cfg.CheckOneLeader();
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with Server {}", (leader2 + 1) % servers));
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with Server {}", (leader2 + 2) % servers));
  cfg.Disconnect((leader2 + 1) % servers);
  cfg.Disconnect((leader2 + 2) % servers);

  // submit a command
  auto [index, _, ok] = cfg.GetRaft(leader2)->Start(104);
  if (ok != true) {
    throw CONFIG_EXCEPTION("leader rejected Start()");
  }
  if (index != 4) {
    throw CONFIG_EXCEPTION(fmt::format("expected index 4, got {}", index));
  }

  common::SleepMs(2 * RAFT_ELECTION_TIMEOUT);

  // check that command 104 did not commit
  auto [n, __] = cfg.NCommited(index);
  if (n > 0) {
    throw CONFIG_EXCEPTION(fmt::format("{} commited but no majority", n));
  }

  EXPECT_TRUE(cfg.Cleanup());
}

}  // namespace kv::raft