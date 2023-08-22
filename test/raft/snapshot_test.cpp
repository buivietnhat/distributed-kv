#include "gtest/gtest.h"
#include "raft/configuration.h"

namespace kv::raft {

static void SnapCommon(const std::string &name, bool disconnect, bool reliable, bool crash) {
  auto iters = 30;
  auto servers = 3;
  Configuration<int> cfg{servers, !reliable, true};

  cfg.Begin(name);

  cfg.One(common::RandInt(), servers, true);

  Logger::Debug(kDTest, -1, "Check one leader");
  auto leader1 = cfg.CheckOneLeader();

  for (int i = 0; i < iters; i++) {
    auto victim = (leader1 + 1) % servers;
    [[maybe_unused]] auto sender = leader1;
    if (i % 3 == 1) {
      sender = (leader1 + 1) % servers;
      victim = leader1;
    }

    if (disconnect) {
      Logger::Debug(kDTest, -1, fmt::format("Disconnect with Server {}", victim));
      cfg.Disconnect(victim);

      cfg.One(common::RandInt(), servers - 1, true);
    }

    if (crash) {
      Logger::Debug(kDTest, -1, fmt::format("Crash the Server {}", victim));
      cfg.Crash(victim);
      cfg.One(common::RandInt(), servers - 1, true);
    }

    // perhaps send enough to get a snapshot
    auto nn = (SNAPSHOT_INTERVAL / 2) + (common::RandInt() % SNAPSHOT_INTERVAL);
    for (int j = 0; j < nn; j++) {
      cfg.GetRaft(sender)->Start(common::RandInt());
    }

    // let applier threads catch up with the Start()'s
    if (disconnect == false && crash == false) {
      // make sure all followers have caught up
      cfg.One(common::RandInt(), servers, true);
    } else {
      cfg.One(common::RandInt(), servers - 1, true);
    }

    if (cfg.LogSize() > MAXLOGSIZE) {
      throw CONFIG_EXCEPTION("log size is too large");
    }

    if (disconnect) {
      // reconnect a follower, who maybe behind and
      // needs to receive a snapshot to catch up.
      Logger::Debug(kDTest, -1, fmt::format("Connect with Server {}", victim));
      cfg.Connect(victim);

      cfg.One(common::RandInt(), servers, true);

      Logger::Debug(kDTest, -1, "Check one leader");
      leader1 = cfg.CheckOneLeader();
    }

    if (crash) {
      Logger::Debug(kDTest, -1, fmt::format("Start the Server {}", victim));
      cfg.Start(victim, cfg.GetApplierSnap());
      Logger::Debug(kDTest, -1, fmt::format("Connect with Server {}", victim));
      cfg.Connect(victim);

      cfg.One(common::RandInt(), servers, true);

      Logger::Debug(kDTest, -1, "Check one leader");
      leader1 = cfg.CheckOneLeader();
    }
  }

  cfg.Cleanup();
}

TEST(RaftSnapshotTest, DISABLED_SnapshotBasic) {
  SnapCommon("Test: snapshots basic", false, true, false);
}

TEST(RaftSnapshotTest, DISABLED_InstallSnapshots) {
  SnapCommon("Test: install snapshots (disconnect)", true, true, false);
}

TEST(RaftSnapshotTest, DISABLED_InstallSnapshotsUnreliable) {
  SnapCommon("Test: install snapshots (disconnect+unreliable)", true, false, false);
}

TEST(RaftSnapshotTest, InstallSnapshotsCrash) {
  SnapCommon("Test: install snapshots (crash)", false, true, true);
}

}  // namespace kv::raft