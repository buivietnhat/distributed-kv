#include "gtest/gtest.h"
#include "raft/config.h"

namespace kv::raft {

TEST(RaftPersistTest, BasicPersist) {
  int servers = 3;
  Config<int> cfg{servers, false, false};

  cfg.Begin("Test: basic persistence");

  cfg.One(11, servers, true);

  // crash and re-start all
  for (int i = 0; i < servers; i++) {
    cfg.Start(i, cfg.GetApplier());
  }
  for (int i = 0; i < servers; i++) {
    cfg.Disconnect(i);
    cfg.Connect(i);
    Logger::Debug(kDTest, -1, fmt::format("Disconnect with ShardKV {}", i));
    Logger::Debug(kDTest, -1, fmt::format("Connect with ShardKV {}", i));
  }

  cfg.One(12, servers, true);

  auto leader1 = cfg.CheckOneLeader();
  cfg.Disconnect(leader1);
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with Leader {}", leader1));
  cfg.Start(leader1, cfg.GetApplier());
  cfg.Connect(leader1);
  Logger::Debug(kDTest, -1, fmt::format("Restart and Connect to Leader {}", leader1));

  cfg.One(13, servers, true);

  auto leader2 = cfg.CheckOneLeader();
  cfg.Disconnect(leader2);
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with Leader {}", leader2));

  cfg.One(14, servers - 1, true);

  cfg.Start(leader2, cfg.GetApplier());
  cfg.Connect(leader2);
  Logger::Debug(kDTest, -1, fmt::format("Restart and Connect to Leader {}", leader2));

  cfg.Wait(4, servers, -1);  // wait for leader2 to join before killing i3

  auto i3 = (cfg.CheckOneLeader() + 1) % servers;
  cfg.Disconnect(i3);
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with ShardKV {}", i3));

  cfg.One(15, servers - 1, true);

  cfg.Start(i3, cfg.GetApplier());
  cfg.Connect(i3);
  Logger::Debug(kDTest, -1, fmt::format("Restart and Connect to ShardKV {}", i3));

  cfg.One(16, servers, true);

  EXPECT_TRUE(cfg.Cleanup());
}

TEST(RaftPersistTest, MorePersistence) {
  int servers = 5;
  Config<int> cfg{servers, false, false};

  cfg.Begin("Test: more persistence");

  int index = 1;
  for (int iters = 0; iters < 5; iters++) {
    cfg.One(10 + index, servers, true);
    index++;

    auto leader1 = cfg.CheckOneLeader();

    cfg.Disconnect((leader1 + 1) % servers);
    cfg.Disconnect((leader1 + 2) % servers);
    Logger::Debug(kDTest, -1, fmt::format("Disconnect with ShardKV {}", (leader1 + 1) % servers));
    Logger::Debug(kDTest, -1, fmt::format("Disconnect with ShardKV {}", (leader1 + 2) % servers));

    cfg.One(10 + index, servers - 2, true);
    index++;

    cfg.Disconnect((leader1 + 0) % servers);
    cfg.Disconnect((leader1 + 3) % servers);
    cfg.Disconnect((leader1 + 4) % servers);
    Logger::Debug(kDTest, -1, fmt::format("Disconnect with ShardKV {}", (leader1 + 0) % servers));
    Logger::Debug(kDTest, -1, fmt::format("Disconnect with ShardKV {}", (leader1 + 3) % servers));
    Logger::Debug(kDTest, -1, fmt::format("Disconnect with ShardKV {}", (leader1 + 4) % servers));

    cfg.Start((leader1 + 1) % servers, cfg.GetApplier());
    cfg.Start((leader1 + 2) % servers, cfg.GetApplier());
    cfg.Connect((leader1 + 1) % servers);
    cfg.Connect((leader1 + 2) % servers);
    Logger::Debug(kDTest, -1, fmt::format("Restart and Connect to ShardKV {}", (leader1 + 1) % servers));
    Logger::Debug(kDTest, -1, fmt::format("Restart and Connect to ShardKV {}", (leader1 + 2) % servers));

    common::SleepMs(RAFT_ELECTION_TIMEOUT);

    cfg.Start((leader1 + 3) % servers, cfg.GetApplier());
    cfg.Connect((leader1 + 3) % servers);
    Logger::Debug(kDTest, -1, fmt::format("Restart and Connect to ShardKV {}", (leader1 + 3) % servers));

    cfg.One(10 + index, servers - 2, true);

    cfg.Connect((leader1 + 4) % servers);
    cfg.Connect((leader1 + 0) % servers);
    Logger::Debug(kDTest, -1, fmt::format("Connect with ShardKV {}", (leader1 + 4) % servers));
    Logger::Debug(kDTest, -1, fmt::format("Connect with ShardKV {}", (leader1 + 0) % servers));
  }

  cfg.One(1000, servers, true);

  EXPECT_TRUE(cfg.Cleanup());
}

TEST(RaftPersistTest, PartitionedLeader) {
  int servers = 3;
  Config<int> cfg{servers, false, false};

  cfg.Begin("Test: partitioned leader and one follower crash, leader restarts");

  cfg.One(101, 3, true);

  auto leader = cfg.CheckOneLeader();
  cfg.Disconnect((leader + 2) % servers);
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with ShardKV {}", (leader + 2) % servers));

  cfg.One(102, 2, true);

  cfg.Crash((leader + 0) % servers);
  cfg.Crash((leader + 1) % servers);
  Logger::Debug(kDTest, -1, fmt::format("Crash Leader {}", (leader + 0) % servers));
  Logger::Debug(kDTest, -1, fmt::format("Crash ShardKV {}", (leader + 1) % servers));
  cfg.Connect((leader + 2) % servers);
  Logger::Debug(kDTest, -1, fmt::format("Connect with ShardKV {}", (leader + 2) % servers));
  cfg.Start((leader + 0) % servers, cfg.GetApplier());
  Logger::Debug(kDTest, -1, fmt::format("Start Leader {}", (leader + 0) % servers));
  cfg.Connect((leader + 0) % servers);
  Logger::Debug(kDTest, -1, fmt::format("Connect with Leader {}", (leader + 0) % servers));

  cfg.One(103, 2, true);

  cfg.Start((leader + 1) % servers, cfg.GetApplier());
  cfg.Connect((leader + 1) % servers);
  Logger::Debug(kDTest, -1, fmt::format("Start and Connect ShardKV {}", (leader + 1) % servers));

  cfg.One(104, servers, true);

  EXPECT_TRUE(cfg.Cleanup());
}

// Test the scenarios described in Figure 8 of the extened Raft paper
TEST(RaftPersistTest, Figure8) {
  int servers = 5;
  Config<int> cfg{servers, false, false};

  cfg.Begin("Test: Figure 8");

  cfg.One(common::RandInt(), 1, true);

  auto nup = servers;
  for (int iters = 0; iters < 1000; iters++) {
    auto leader = -1;
    for (int i = 0; i < servers; i++) {
      if (cfg.GetRaft(i) != nullptr) {
        auto [_1, _2, ok] = cfg.GetRaft(i)->Start(common::RandInt());
        if (ok) {
          leader = i;
        }
      }
    }

    if ((common::RandInt() % 1000) < 100) {
      auto ms = common::RandInt() % (RAFT_ELECTION_TIMEOUT / 2);
      common::SleepMs(ms);
    } else {
      auto ms = common::RandInt() % 13;
      common::SleepMs(ms);
    }

    if (leader != -1) {
      Logger::Debug(kDTest, -1, fmt::format("Crash Leader {}", leader));
      cfg.Crash(leader);
      nup -= 1;
    }

    if (nup < 3) {
      auto s = common::RandInt() % servers;
      Logger::Debug(kDTest, -1, fmt::format("Start and Connect ShardKV {}", s));
      if (cfg.GetRaft(s) == nullptr) {
        cfg.Start(s, cfg.GetApplier());
        cfg.Connect(s);
        nup += 1;
      }
    }
  }

  for (int i = 0; i < servers; i++) {
    if (cfg.GetRaft(i) == nullptr) {
      Logger::Debug(kDTest, -1, fmt::format("Start and Connect ShardKV {}", i));
      cfg.Start(i, cfg.GetApplier());
      cfg.Connect(i);
    }
  }

  cfg.One(common::RandInt(), servers, true);

  EXPECT_TRUE(cfg.Cleanup());
}

TEST(RaftPersistTest, UnreliableAgree) {
  int servers = 5;
  Config<int> cfg{servers, true, false};

  cfg.Begin("Test: unreliable agreement");

  std::atomic<int> count{0};

  for (int iters = 0; iters < 50; iters++) {
    for (int j = 0; j < 4; j++) {
      count += 1;
      boost::fibers::thread([&, iters, j] {
        cfg.One(100 * iters + j, 1, true);
        count -= 1;
      }).detach();
    }
    cfg.One(iters, 1, true);
  }

  cfg.SetUnreliable(false);

  while (count > 0) {
    Logger::Debug(kDTest, -1, fmt::format("Waiting for counter to reach 0, now {}", count.load()));
    common::SleepMs(100);
  }

  cfg.One(100, servers, true);

  EXPECT_TRUE(cfg.Cleanup());
}

}  // namespace kv::raft