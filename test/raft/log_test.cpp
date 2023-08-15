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

TEST(RaftLogTest, DISABLED_FollowerFailure) {
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

TEST(RaftLogTest, DISABLED_LeaderFailure) {
  int servers = 3;
  Configuration<int> cfg{servers, false, false};

  cfg.Begin("Test: test failure of leaders");

  cfg.One(101, servers, false);

  // disconnect the first leader;
  auto leader1 = cfg.CheckOneLeader();
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with Leader {}", leader1));
  cfg.Disconnect(leader1);

  // the remaining followers should elect a new leader
  cfg.One(102, servers - 1, false);
  common::SleepMs(2 * RAFT_ELECTION_TIMEOUT);
  cfg.One(103, servers - 1, false);

  // disconnect the new leader
  auto leader2 = cfg.CheckOneLeader();
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with Leader {}", leader2));
  cfg.Disconnect(leader2);

  // submit a command to each server
  for (int i = 0; i < servers; i++) {
    cfg.GetRaft(i)->Start(104);
  }

  common::SleepMs(2 * RAFT_ELECTION_TIMEOUT);

  // check that command 104 did not commit
  auto [n, _] = cfg.NCommited(4);
  if (n > 4) {
    throw CONFIG_EXCEPTION(fmt::format("{} committed but no majority", n));
  }

  EXPECT_TRUE(cfg.Cleanup());
}

// test that a follower participates after disconnnect and re-connect
TEST(RaftLogTest, DISABLED_FailAgree) {
  int servers = 3;
  Configuration<int> cfg{servers, false, false};

  cfg.Begin("Test: agreement after follower reconnects");

  cfg.One(101, servers, false);

  // disconnect one follower from the network
  auto leader = cfg.CheckOneLeader();
  cfg.Disconnect((leader + 1) % servers);
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with Server {}", (leader + 1) % servers));

  // the leader and remaining follower should be able to agree
  // despite the disconnected follower
  cfg.One(102, servers - 1, false);
  cfg.One(103, servers - 1, false);
  common::SleepMs(RAFT_ELECTION_TIMEOUT);
  cfg.One(104, servers - 1, false);
  cfg.One(105, servers - 1, false);

  // re-connect
  Logger::Debug(kDTest, -1, fmt::format("Connect with Server {}", (leader + 1) % servers));
  cfg.Connect((leader + 1) % servers);

  // the full set of servers should preserve previous agreements, and be able to
  // agree on new commands
  cfg.One(106, servers, true);
  common::SleepMs(RAFT_ELECTION_TIMEOUT);
  cfg.One(107, servers, true);

  EXPECT_TRUE(cfg.Cleanup());
}

TEST(RaftLogTest, NoAgree) {
  int servers = 5;
  Configuration<int> cfg{servers, false, false};

  cfg.Begin("Test: no agreement if too many followers disconnect");

  cfg.One(10, servers, false);

  // 3 of 5 followers disconnect
  auto leader = cfg.CheckOneLeader();
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with Server {}", (leader + 1) % servers));
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with Server {}", (leader + 2) % servers));
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with Server {}", (leader + 3) % servers));
  cfg.Disconnect((leader + 1) % servers);
  cfg.Disconnect((leader + 2) % servers);
  cfg.Disconnect((leader + 3) % servers);

  auto [index, _, ok] = cfg.GetRaft(leader)->Start(20);
  if (ok != true) {
    throw CONFIG_EXCEPTION("leader rejected Start()");
  }
  if (index != 2) {
    throw CONFIG_EXCEPTION(fmt::format("expected index 2, got {}", index));
  }

  common::SleepMs(2 * RAFT_ELECTION_TIMEOUT);

  auto [n, __] = cfg.NCommited(index);
  if (n > 0) {
    throw CONFIG_EXCEPTION(fmt::format("{} commited but no majority", n));
  }

  // repair
  Logger::Debug(kDTest, -1, fmt::format("Connect with Server {}", (leader + 1) % servers));
  Logger::Debug(kDTest, -1, fmt::format("Connect with Server {}", (leader + 2) % servers));
  Logger::Debug(kDTest, -1, fmt::format("Connect with Server {}", (leader + 3) % servers));
  cfg.Connect((leader + 1) % servers);
  cfg.Connect((leader + 2) % servers);
  cfg.Connect((leader + 3) % servers);

  // the disconnected majority may have chosen a leader from among their own ranks,
  // forgetting index 2
  auto leader2 = cfg.CheckOneLeader();
  auto [index2, ___, ok2] = cfg.GetRaft(leader2)->Start(30);
  if (ok2 == false) {
    throw CONFIG_EXCEPTION("leader rejected Start()");
  }

  if (index2 < 2 || index2 > 3) {
    throw CONFIG_EXCEPTION(fmt::format("unexpected index {}", index2));
  }

  cfg.One(1000, servers, true);

  EXPECT_TRUE(cfg.Cleanup());
}

}  // namespace kv::raft