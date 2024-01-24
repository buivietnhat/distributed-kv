#include <string_view>

#include "common/thread_registry.h"
#include "gtest/gtest.h"
#include "shardctrler/config.h"
#include "shardkv/common.h"
#include "shardkv/config.h"

namespace kv::shardkv {

bool Check(Clerk *ck, const std::string &key, const std::string &value) {
  auto v = ck->Get(key);
  if (v != value) {
    Logger::Debug(kDTest, -1, fmt::format("Get({}): expected: {}, received: {}", key, value, v));
    //    Logger::Debug(kDTest, -1, fmt::format("{}", value));
    //    Logger::Debug(kDTest, -1, fmt::format("received:"));
    //    Logger::Debug(kDTest, -1, fmt::format("{}", v));
    //    throw SHARDKV_EXCEPTION(fmt::format("Get({}): expected:\n{}\nreceived:\n{}", key, value, v));
    return false;
  }
  return true;
}

// test static 2-way sharding, without shard movement
TEST(ShardKVTest, TestStaticShards) {
  Logger::Debug(kDTest, -1, "Test: static shards ...");

  Config cfg(3, false, -1);

  try {
    auto ck = cfg.MakeClient();

    Logger::Debug(kDTest, -1, "Join group 0");
    cfg.Join(0);
    Logger::Debug(kDTest, -1, "Join group 1");
    cfg.Join(1);

    int n = 10;
    std::vector<std::string> ka(n);
    std::vector<std::string> va(n);
    for (int i = 0; i < n; i++) {
      ka[i] = std::to_string(i);  // ensure multiple shards
      va[i] = common::RandString(20);
      ck->Put(ka[i], va[i]);
    }

    for (int i = 0; i < n; i++) {
      EXPECT_TRUE(Check(ck.get(), ka[i], va[i]));
    }

    // make sure that the data really is sharded by
    // shutting down one shard and checking that some
    // Get()s don't succeed.
    Logger::Debug(kDTest, -1, "Shutdown group 1");
    cfg.ShutdownGroup(1);
    cfg.CheckLogs();  // forbid snapshots

    std::vector<std::shared_ptr<Clerk>> cls;
    auto ch = std::make_shared<boost::fibers::unbuffered_channel<std::string>>();
    ON_SCOPE_EXIT { ch->close(); };

    std::vector<boost::fibers::fiber> threads;
    ON_SCOPE_EXIT {
      for (auto &f : threads) {
        f.join();
      }
    };
    for (int xi = 0; xi < n; xi++) {
      std::shared_ptr<Clerk> ck1 = cfg.MakeClient();  // only one call allowed per client
      cls.push_back(ck1);
      threads.push_back(boost::fibers::fiber([&, ck1, i = xi] {
        auto v = ck1->Get(ka[i]);
        if (v != va[i]) {
          ch->push(fmt::format("Get({}): expected: {} received: {}", ka[i], va[i], v));
        } else {
          ch->push("");
        }
      }));
    }

    // wait a bit, only about half the Gets should succeed.
    int ndone = 0;
    auto start = common::Now();
    boost::fibers::fiber f([&] {
      boost::this_fiber::sleep_for(MS(2000));
      ch->close();
    });
    ON_SCOPE_EXIT { f.join(); };

    while (common::ElapsedTimeS(start, common::Now()) < 2) {
      std::string err;

      if (ch->pop(err) == boost::fibers::channel_op_status::success) {
        if (err != "") {
          Logger::Debug(kDTest, -1, fmt::format("{}", err));
          throw SHARDKV_EXCEPTION(err);
        }
        ndone += 1;
      }
    }

    if (ndone != 5) {
      Logger::Debug(kDTest, -1, fmt::format("expected 5 completions with one shard dead; got {}\n", ndone));
      throw SHARDKV_EXCEPTION(fmt::format("expected 5 completions with one shard dead; got %v\n", ndone));
    }

    // bring the crashed shard/group back to life
    Logger::Debug(kDTest, -1, "Start Group 1");
    cfg.StartGroup(1);
    for (int i = 0; i < n; i++) {
      EXPECT_TRUE(Check(ck.get(), ka[i], va[i]));
    }

    Logger::Debug(kDTest, -1, "  ... Passed");

  } catch (Exception &e) {
    Logger::Debug(kDTest, -1, fmt::format("An exception thrown: {}", e.what()));
    Logger::Debug(kDTest, -1, "  ... Failed");
  }

  EXPECT_TRUE(cfg.CleanUp());
}

TEST(ShardKVTest, TestJoinLeave) {
  Logger::Debug(kDTest, -1, "Test: join then leave");

  Config cfg(3, false, -1);

  try {
    auto ck = cfg.MakeClient();

    Logger::Debug(kDTest, -1, "Join group 0");
    cfg.Join(0);

    int n = 10;
    std::vector<std::string> ka(n);
    std::vector<std::string> va(n);
    for (int i = 0; i < n; i++) {
      ka[i] = std::to_string(i);
      va[i] = common::RandString(5);
      ck->Put(ka[i], va[i]);
    }
    for (int i = 0; i < n; i++) {
      EXPECT_TRUE(Check(ck.get(), ka[i], va[i]));
    }

    Logger::Debug(kDTest, -1, "Join group 1");
    cfg.Join(1);

    for (int i = 0; i < n; i++) {
      EXPECT_TRUE(Check(ck.get(), ka[i], va[i]));
      auto x = common::RandString(5);
      ck->Append(ka[i], x);
      va[i] += x;
    }

    Logger::Debug(kDTest, -1, "Leave group 0");
    cfg.Leave(0);

    for (int i = 0; i < n; i++) {
      EXPECT_TRUE(Check(ck.get(), ka[i], va[i]));
      auto x = common::RandString(5);
      ck->Append(ka[i], x);
      va[i] += x;
    }

    // allow time for shards to transfer.
    common::SleepMs(1000);

    cfg.CheckLogs();
    Logger::Debug(kDTest, -1, "Shutdown group 0");
    cfg.ShutdownGroup(0);

    for (int i = 0; i < n; i++) {
      EXPECT_TRUE(Check(ck.get(), ka[i], va[i]));
    }

    Logger::Debug(kDTest, -1, "  ... Passed\n");
  } catch (Exception &e) {
    Logger::Debug(kDTest, -1, fmt::format("An exception thrown: {}", e.what()));
    Logger::Debug(kDTest, -1, "  ... Failed");
  }

  EXPECT_TRUE(cfg.CleanUp());
}

TEST(ShardKVTest, TestSnapshot) {
  Logger::Debug(kDTest, -1, "Test: snapshots, join, and leave ...\n");

  Config cfg(3, false, 1000);

  auto ck = cfg.MakeClient();

  Logger::Debug(kDTest, -1, "Join group 0");
  cfg.Join(0);

  int n = 30;
  std::vector<std::string> ka(n);
  std::vector<std::string> va(n);
  for (int i = 0; i < n; i++) {
    ka[i] = std::to_string(i);  // ensure multiple shards
    va[i] = common::RandString(20);
    ck->Put(ka[i], va[i]);
  }
  for (int i = 0; i < n; i++) {
    EXPECT_TRUE(Check(ck.get(), ka[i], va[i]));
  }

  Logger::Debug(kDTest, -1, "Join group 1");
  cfg.Join(1);
  Logger::Debug(kDTest, -1, "Join group 2");
  cfg.Join(2);
  Logger::Debug(kDTest, -1, "Leave group 0");
  cfg.Leave(0);

  for (int i = 0; i < n; i++) {
    EXPECT_TRUE(Check(ck.get(), ka[i], va[i]));
    auto x = common::RandString(20);
    ck->Append(ka[i], x);
    va[i] += x;
  }

  Logger::Debug(kDTest, -1, "Leave group 1");
  cfg.Leave(1);
  Logger::Debug(kDTest, -1, "Join group 0");
  cfg.Join(0);

  for (int i = 0; i < n; i++) {
    EXPECT_TRUE(Check(ck.get(), ka[i], va[i]));
    auto x = common::RandString(20);
    ck->Append(ka[i], x);
    va[i] += x;
  }

  common::SleepMs(1000);

  for (int i = 0; i < n; i++) {
    EXPECT_TRUE(Check(ck.get(), ka[i], va[i]));
  }

  common::SleepMs(1000);

  cfg.CheckLogs();

  Logger::Debug(kDTest, -1, "Shutdown groups 0,1,2");
  cfg.ShutdownGroup(0);
  cfg.ShutdownGroup(1);
  cfg.ShutdownGroup(2);

  Logger::Debug(kDTest, -1, "Start groups 0,1,2");
  cfg.StartGroup(0);
  cfg.StartGroup(1);
  cfg.StartGroup(2);

  for (int i = 0; i < n; i++) {
    EXPECT_TRUE(Check(ck.get(), ka[i], va[i]));
  }

  Logger::Debug(kDTest, -1, "  ... Passed\n");

  EXPECT_TRUE(cfg.CleanUp());
}

TEST(ShardKVTest,TestMissChange) {
  Logger::Debug(kDTest, -1, "Test: servers miss configuration changes ...");

  Config cfg(3, false, 1000);

  auto ck = cfg.MakeClient();

  cfg.Join(0);

  int n = 10;
  std::vector<std::string> ka(n);
  std::vector<std::string> va(n);
  for (int i = 0; i < n; i++) {
    ka[i] = std::to_string(i);
    va[i] = common::RandString(20);
    ck->Put(ka[i], va[i]);
  }
  for (int i = 0; i < n; i++) {
    Check(ck.get(), ka[i], va[i]);
  }

  cfg.Join(1);

  Logger::Debug(kDTest, -1, "Shutdown Server 0 of Group 0");
  cfg.ShutdownServer(0, 0);
  Logger::Debug(kDTest, -1, "Shutdown Server 0 of Group 1");
  cfg.ShutdownServer(1, 0);
  Logger::Debug(kDTest, -1, "Shutdown Server 0 of Group 2");
  cfg.ShutdownServer(2, 0);

  cfg.Join(2);
  cfg.Leave(1);
  cfg.Leave(0);

  for (int i = 0; i < n; i++) {
    Check(ck.get(), ka[i], va[i]);
    auto x = common::RandString(20);
    ck->Append(ka[i], x);
    va[i] += x;
  }

  cfg.Join(1);

  for (int i = 0; i < n; i++) {
    Check(ck.get(), ka[i], va[i]);
    auto x = common::RandString(20);
    ck->Append(ka[i], x);
    va[i] += x;
  }

  Logger::Debug(kDTest, -1, "Start Server 0 of Group 0");
  cfg.StartServer(0, 0);
  Logger::Debug(kDTest, -1, "Start Server 0 of Group 1");
  cfg.StartServer(1, 0);
  Logger::Debug(kDTest, -1, "Start Server 0 of Group 2");
  cfg.StartServer(2, 0);

  for (int i = 0; i < n; i++) {
    Check(ck.get(), ka[i], va[i]);
    auto x = common::RandString(20);
    ck->Append(ka[i], x);
    va[i] += x;
  }

  common::SleepMs(2000);

  Logger::Debug(kDTest, -1, "Shutdown Server 1 of Group 0");
  cfg.ShutdownServer(0, 1);
  Logger::Debug(kDTest, -1, "Shutdown Server 1 of Group 1");
  cfg.ShutdownServer(1, 1);
  Logger::Debug(kDTest, -1, "Shutdown Server 1 of Group 2");
  cfg.ShutdownServer(2, 1);

  cfg.Join(0);
  cfg.Leave(2);

  for (int i = 0; i < n; i++) {
    Check(ck.get(), ka[i], va[i]);
    auto x = common::RandString(20);
    ck->Append(ka[i], x);
    va[i] += x;
  }

  Logger::Debug(kDTest, -1, "Start Server 1 of Group 0");
  cfg.StartServer(0, 1);
  Logger::Debug(kDTest, -1, "Start Server 1 of Group 1");
  cfg.StartServer(1, 1);
  Logger::Debug(kDTest, -1, "Start Server 1 of Group 2");
  cfg.StartServer(2, 1);

  for (int i = 0; i < n; i++) {
    Check(ck.get(), ka[i], va[i]);
  }

  Logger::Debug(kDTest, -1, "  ... Passed");

  EXPECT_TRUE(cfg.CleanUp());
}

TEST(ShardKVTest, ConcurrentTest) {
  Logger::Debug(kDTest, -1, "Test: concurrent puts and configuration changes...\n");

  Config cfg(3, false, 100);

  auto ck = cfg.MakeClient();

  cfg.Join(0);

  int n = 10;
  std::vector<std::string> ka(n);
  std::vector<std::string> va(n);
  for (int i = 0; i < n; i++) {
    ka[i] = std::to_string(i);
    va[i] = common::RandString(5);
    ck->Put(ka[i], va[i]);
  }
  for (int i = 0; i < n; i++) {
    Check(ck.get(), ka[i], va[i]);
  }

  std::atomic<bool> done{false};

  auto ff = [&](int i) {
    auto ck1 = cfg.MakeClient();
    while (done == false) {
      auto x = common::RandString(5);
      ck1->Append(ka[i], x);
      va[i] += x;
      common::SleepMs(10);
    }
  };

  std::vector<boost::fibers::fiber> threads;
  for (int i = 0; i < n; i++) {
    threads.push_back(boost::fibers::fiber([&, i] { ff(i); }));
  }

  common::SleepMs(150);
  Logger::Debug(kDTest, -1, "Join group 1");
  cfg.Join(1);
  common::SleepMs(500);
  Logger::Debug(kDTest, -1, "Join group 2");
  cfg.Join(2);
  common::SleepMs(500);
  Logger::Debug(kDTest, -1, "Leave group 0");
  cfg.Leave(0);

  Logger::Debug(kDTest, -1, "Shutdown group 0");
  cfg.ShutdownGroup(0);
  common::SleepMs(100);
  Logger::Debug(kDTest, -1, "Shutdown group 1");
  cfg.ShutdownGroup(1);
  common::SleepMs(100);
  Logger::Debug(kDTest, -1, "Shutdown group 2");
  cfg.ShutdownGroup(2);

  Logger::Debug(kDTest, -1, "Leave group 2");
  cfg.Leave(2);

  common::SleepMs(100);
  Logger::Debug(kDTest, -1, "Start group 0");
  cfg.StartGroup(0);
  Logger::Debug(kDTest, -1, "Start group 1");
  cfg.StartGroup(1);
  Logger::Debug(kDTest, -1, "Start group 2");
  cfg.StartGroup(2);

  common::SleepMs(100);
  Logger::Debug(kDTest, -1, "Join group 0");
  cfg.Join(0);
  Logger::Debug(kDTest, -1, "Leave group 1");
  cfg.Leave(1);
  common::SleepMs(500);
  Logger::Debug(kDTest, -1, "Join group 1");
  cfg.Join(1);

  common::SleepMs(1000);

  done = true;

  for (auto &thread : threads) {
    thread.join();
  }

  for (int i = 0; i < n; i++) {
    Check(ck.get(), ka[i], va[i]);
  }

  Logger::Debug(kDTest, -1, " ... Passed\n");

  EXPECT_TRUE(cfg.CleanUp());
}

TEST(ShardKVTest, Unreliable) {
  Logger::Debug(kDTest, -1, "Test: unreliable");

  Config cfg(3, true, 100);
  ON_SCOPE_EXIT { cfg.CleanUp(); };

  auto ck = cfg.MakeClient();

  cfg.Join(0);

  int n = 10;
  std::vector<std::string> ka(n);
  std::vector<std::string> va(n);
  for (int i = 0; i < n; i++) {
    ka[i] = std::to_string(i);
    va[i] = common::RandString(5);
    ck->Put(ka[i], va[i]);
  }
  for (int i = 0; i < n; i++) {
    Check(ck.get(), ka[i], va[i]);
  }

  Logger::Debug(kDTest, -1, "Join group 1");
  cfg.Join(1);
  Logger::Debug(kDTest, -1, "Join group 2");
  cfg.Join(2);
  Logger::Debug(kDTest, -1, "Leave group 0");
  cfg.Leave(0);

  for (int ii = 0; ii < n * 2; ii++) {
    auto i = ii % n;
    Check(ck.get(), ka[i], va[i]);
    auto x = common::RandString(5);
    ck->Append(ka[i], x);
    va[i] += x;
  }

  Logger::Debug(kDTest, -1, "Join group 0");
  cfg.Join(0);
  Logger::Debug(kDTest, -1, "Leave group 1");
  cfg.Leave(1);

  for (int ii = 0; ii < n * 2; ii++) {
    auto i = ii % n;
    Check(ck.get(), ka[i], va[i]);
  }

  Logger::Debug(kDTest, -1, "  ... Passed\n");
}

}  // namespace kv::shardkv