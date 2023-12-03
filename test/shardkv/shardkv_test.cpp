#include <string_view>

#include "common/thread_registry.h"
#include "gtest/gtest.h"
#include "shardctrler/config.h"
#include "shardkv/common.h"
#include "shardkv/config.h"

namespace kv::shardkv {

void Check(Clerk *ck, const std::string &key, const std::string &value) {
  auto v = ck->Get(key);
  if (v != value) {
    Logger::Debug(kDTest, -1, fmt::format("Get({}): expected:", key));
    Logger::Debug(kDTest, -1, fmt::format("{}", value));
    Logger::Debug(kDTest, -1, fmt::format("received:"));
    Logger::Debug(kDTest, -1, fmt::format("{}", v));
    throw SHARDKV_EXCEPTION(fmt::format("Get({}): expected:\n{}\nreceived:\n{}", key, value, v));
  }
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
      Check(ck.get(), ka[i], va[i]);
    }

    // make sure that the data really is sharded by
    // shutting down one shard and checking that some
    // Get()s don't succeed.
    Logger::Debug(kDTest, -1, "Shutdown group 1");
    cfg.ShutdownGroup(1);
    cfg.CheckLogs();  // forbid snapshots

    std::vector<std::shared_ptr<Clerk>> cls;
    auto ch = std::make_shared<common::ConcurrentBlockingQueue<std::string>>();
    common::ThreadRegistry tr;
    for (int xi = 0; xi < n; xi++) {
      std::shared_ptr<Clerk> ck1 = cfg.MakeClient();  // only one call allowed per client
      cls.push_back(ck1);
      tr.RegisterNewThread([&, ck1, i = xi] {
        auto v = ck1->Get(ka[i]);
        if (v != va[i]) {
          ch->Enqueue(fmt::format("Get({}): expected:\n{}\nreceived:\n{}", ka[i], va[i], v));
        } else {
          ch->Enqueue("");
        }
      });
    }

    // wait a bit, only about half the Gets should succeed.
    int ndone = 0;
    auto start = common::Now();
    while (common::ElapsedTimeS(start, common::Now()) < 2) {
      if (!ch->Empty()) {
        std::string err;
        ch->Dequeue(&err);
        if (err != "") {
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
      Check(ck.get(), ka[i], va[i]);
    }

//    for (auto &cl : cls) {
//      cl->Kill();
//    }
    Logger::Debug(kDTest, -1, "  ... Passed");

  } catch (Exception &e) {
    Logger::Debug(kDTest, -1, fmt::format("An exception thrown: {}", e.what()));
    Logger::Debug(kDTest, -1, "  ... Failed");
  }

  EXPECT_TRUE(cfg.CleanUp());
}

}  // namespace kv::shardkv