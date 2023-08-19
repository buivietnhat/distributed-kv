#include "gtest/gtest.h"
#include "raft/configuration.h"

namespace kv::raft {

TEST(RaftPersistTest, BasicPersist) {
  int servers = 3;
  Configuration<int> cfg{servers, false, false};

  cfg.Begin("Test: basic persistence");

  cfg.One(11, servers, true);

  // crash and re-start all
  for (int i = 0; i < servers; i++) {
    cfg.Start(i, cfg.GetApplier());
  }
  for (int i = 0; i < servers; i++) {
    cfg.Disconnect(i);
    cfg.Connect(i);
    Logger::Debug(kDTest, -1, fmt::format("Disconnect with Server {}", i));
    Logger::Debug(kDTest, -1, fmt::format("Connect with Server {}", i));
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

  cfg.Wait(4, servers, -1); // wait for leader2 to join before killing i3

  auto i3 = (cfg.CheckOneLeader() + 1) % servers;
  cfg.Disconnect(i3);
  Logger::Debug(kDTest, -1, fmt::format("Disconnect with Server {}", i3));

  cfg.One(15, servers - 1, true);

  cfg.Start(i3, cfg.GetApplier());
  cfg.Connect(i3);
  Logger::Debug(kDTest, -1, fmt::format("Restart and Connect to Server {}", i3));

  cfg.One(16, servers, true);

  EXPECT_TRUE(cfg.Cleanup());
}

}  // namespace kv::raft