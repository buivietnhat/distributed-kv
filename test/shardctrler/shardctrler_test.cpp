#include "common/container/channel.h"
#include "gtest/gtest.h"
#include "shardctrler/config.h"

namespace kv::shardctrler {

void Check(std::vector<int> groups, Clerk *ck) {
  auto c = ck->Query(-1);
  if (c.groups_.size() != groups.size()) {
    Logger::Debug(kDTest, -1, fmt::format("wanted {} groups, got {}", groups.size(), c.groups_.size()));
    throw SHARDCTRLER_EXCEPTION(fmt::format("wanted {} groups, got {}", groups.size(), c.groups_.size()));
  }

  // are the groups as expected?
  for (auto g : groups) {
    if (!c.groups_.contains(g)) {
      Logger::Debug(kDTest, -1, fmt::format("missing group{}", g));
      throw SHARDCTRLER_EXCEPTION(fmt::format("missing group{}", g));
    }
  }

  // any un-allocated shards?
  if (groups.size() > 0) {
    for (uint32_t s = 0; s < c.shards_.size(); s++) {
      auto g = c.shards_[s];
      if (!c.groups_.contains(g)) {
        Logger::Debug(kDTest, -1, fmt::format("shard {} -> invalid group {}", s, g));
        throw SHARDCTRLER_EXCEPTION(fmt::format("shard {} -> invalid group {}", s, g));
      }
    }
  }

  // more or less balanced sharding?
  std::unordered_map<int, int> counts;
  for (auto g : c.shards_) {
    counts[g] += 1;
  }

  auto min = 257;
  auto max = 0;
  for (const auto &[g, _] : c.groups_) {
    if (counts[g] > max) {
      max = counts[g];
    }
    if (counts[g] < min) {
      min = counts[g];
    }
  }

  if (max > min + 1) {
    Logger::Debug(kDTest, -1, fmt::format("max {} too much larger than min {}", max, min));
    throw SHARDCTRLER_EXCEPTION(fmt::format("max {} too much larger than min {}", max, min));
  }
}

void CheckSameConfig(const ShardConfig &c1, const ShardConfig &c2) {
  if (c1.num_ != c2.num_) {
    Logger::Debug(kDTest, -1, "num wrong");
    throw SHARDCTRLER_EXCEPTION("num wrong");
  }

  if (c1.shards_ != c2.shards_) {
    Logger::Debug(kDTest, -1, "shards wrong");
    Logger::Debug(kDTest, -1, fmt::format("c1 Shards {}", c1.ShardsToString()));
    Logger::Debug(kDTest, -1, fmt::format("c2 Shards {}", c2.ShardsToString()));
    throw SHARDCTRLER_EXCEPTION("shards wrong");
  }

  if (c1.groups_.size() != c2.groups_.size()) {
    Logger::Debug(kDTest, -1, "number of groups is wrong");
    throw SHARDCTRLER_EXCEPTION("number of groups is wrong");
  }

  for (const auto &[gid, sa] : c1.groups_) {
    if (!c2.groups_.contains(gid) || c2.groups_.at(gid).size() != sa.size()) {
      Logger::Debug(kDTest, -1, "len(groups) wrong");
      throw SHARDCTRLER_EXCEPTION("len(groups) wrong");
    }

    auto sa1 = c2.groups_.at(gid);
    for (uint32_t j = 0; j < sa.size(); j++) {
      if (sa[j] != sa1[j]) {
        Logger::Debug(kDTest, -1, "groups wrong");
        throw SHARDCTRLER_EXCEPTION("groups wrong");
      }
    }
  }
}

TEST(ShardCtrlerTest, TestBasic) {
  int nservers = 3;
  Config cfg{nservers, false};

  auto ck = cfg.MakeClient(cfg.All());

  Logger::Debug(kDTest, -1, "Test: Basic leave/join ...");

  std::vector<ShardConfig> cfa(6);
  cfa[0] = ck->Query(-1);

  Check({}, ck);

  int gid1 = 1;
  Logger::Debug(kDTest, -1, "Join Group 1");
  std::unordered_map<int, std::vector<std::string>> servers = {{gid1, std::vector<std::string>{"x", "y", "z"}}};
  ck->Join(servers);
  Check(std::vector<int>{gid1}, ck);
  cfa[1] = ck->Query(-1);

  int gid2 = 2;
  Logger::Debug(kDTest, -1, "Join Group 2");
  servers = {{gid2, std::vector<std::string>{"a", "b", "c"}}};
  ck->Join(servers);
  Check(std::vector<int>{gid1, gid2}, ck);
  cfa[2] = ck->Query(-1);

  auto cfx = ck->Query(-1);
  auto sa1 = cfx.groups_[gid1];
  if (sa1.size() != 3 || sa1[0] != "x" || sa1[1] != "y" || sa1[2] != "z") {
    Logger::Debug(kDTest, -1, fmt::format("wrong servers for gid {}: {}", gid1, common::ToString(sa1)));
    throw SHARDCTRLER_EXCEPTION(fmt::format("wrong servers for gid {}: {}", gid1, common::ToString(sa1)));
  }
  auto sa2 = cfx.groups_[gid2];
  if (sa2.size() != 3 || sa2[0] != "a" || sa2[1] != "b" || sa2[2] != "c") {
    Logger::Debug(kDTest, -1, fmt::format("wrong servers for gid {}: {}", gid2, common::ToString(sa2)));
    throw SHARDCTRLER_EXCEPTION(fmt::format("wrong servers for gid {}: {}", gid2, common::ToString(sa2)));
  }

  Logger::Debug(kDTest, -1, fmt::format("Leave Group {}", gid1));
  ck->Leave(std::vector<int>{gid1});
  Check(std::vector<int>{gid2}, ck);
  cfa[4] = ck->Query(-1);

  ck->Leave(std::vector<int>{gid2});
  cfa[5] = ck->Query(-1);

  Logger::Debug(kDTest, -1, "  ... Passed\n");

  Logger::Debug(kDTest, -1, "Test: Historical queries ...\n");

  for (int s = 0; s < nservers; s++) {
    cfg.ShutdownServer(s);
    for (uint32_t i = 0; i < cfa.size(); i++) {
      auto c = ck->Query(cfa[i].num_);
      CheckSameConfig(c, cfa[i]);
    }
    cfg.StartServer(s);
    cfg.ConnectAll();
  }

  Logger::Debug(kDTest, -1, "  ... Passed\n");

  Logger::Debug(kDTest, -1, "Test: Move ...\n");
  {
    int gid3 = 503;
    ck->Join({{gid3, {"3a", "3b", "3c"}}});
    int gid4 = 504;
    ck->Join({{gid4, {"4a", "4b", "4c"}}});

    for (int i = 0; i < kNShards; i++) {
      auto cf = ck->Query(-1);
      if (i < kNShards / 2) {
        ck->Move(i, gid3);
        if (cf.shards_[i] != gid3) {
          auto cf1 = ck->Query(-1);
          if (cf1.num_ <= cf.num_) {
            Logger::Debug(kDTest, -1, "Move should increase Config.Num");
            throw SHARDCTRLER_EXCEPTION("Move should increase Config.Num");
          }
        }
      } else {
        ck->Move(i, gid4);
        if (cf.shards_[i] != gid4) {
          auto cf1 = ck->Query(-1);
          if (cf1.num_ <= cf.num_) {
            Logger::Debug(kDTest, -1, "Move should increase Config.Num");
            throw SHARDCTRLER_EXCEPTION("Move should increase Config.Num");
          }
        }
      }
    }
    auto cf2 = ck->Query(-1);
    for (int i = 0; i < kNShards; i++) {
      if (i < kNShards / 2) {
        if (cf2.shards_[i] != gid3) {
          Logger::Debug(kDTest, -1, fmt::format("expected shard {} on gid {} actually {}", i, gid3, cf2.shards_[i]));
          throw SHARDCTRLER_EXCEPTION(fmt::format("expected shard {} on gid {} actually {}", i, gid3, cf2.shards_[i]));
        }
      } else {
        if (cf2.shards_[i] != gid4) {
          Logger::Debug(kDTest, -1, fmt::format("expected shard {} on gid {} actually {}", i, gid4, cf2.shards_[i]));
          throw SHARDCTRLER_EXCEPTION(fmt::format("expected shard {} on gid {} actually {}", i, gid4, cf2.shards_[i]));
        }
      }
    }
    ck->Leave({gid3});
    ck->Leave({gid4});
  }

  Logger::Debug(kDTest, -1, "  ... Passed\n");

  Logger::Debug(kDTest, -1, "Test: Concurrent leave/join ...\n");

  static constexpr int npara = 10;
  std::array<Clerk *, npara> cka;
  for (int i = 0; i < npara; i++) {
    cka[i] = cfg.MakeClient(cfg.All());
  }
  std::vector<int> gids(npara);
  common::Channel<bool> ch;

  for (int xi = 0; xi < npara; xi++) {
    gids[xi] = xi * 10 + 100;
    std::thread([&, i = xi] {
      int gid = gids[i];
      auto sid1 = fmt::format("s{}a", gid);
      auto sid2 = fmt::format("s{}b", gid);
      cka[i]->Join({{gid + 1000, {sid1}}});
      cka[i]->Join({{gid, {sid2}}});
      cka[i]->Leave({gid + 1000});
      ch.Send(true);
    }).detach();
  }

  for (int i = 0; i < npara; i++) {
    ch.Receive();
  }

  Check(gids, ck);

  Logger::Debug(kDTest, -1, "  ... Passed\n");

  Logger::Debug(kDTest, -1, "Test: Minimal transfers after joins ...\n");

  auto c1 = ck->Query(-1);
  for (int i = 0; i < 5; i++) {
    auto gid = npara + 1 + i;
    ck->Join({{gid, {fmt::format("{}a", gid), fmt::format("{}b", gid), fmt::format("{}b", gid)}}});
  }

  auto c2 = ck->Query(-1);
  for (int i = 1; i <= npara; i++) {
    for (uint32_t j = 0; j < c1.shards_.size(); j++) {
      if (c2.shards_[j] == i) {
        if (c1.shards_[j] != i) {
          Logger::Debug(kDTest, -1, "non-minimal transfer after Join()s");
          throw SHARDCTRLER_EXCEPTION("non-minimal transfer after Join()s");
        }
      }
    }
  }

  Logger::Debug(kDTest, -1, "  ... Passed\n");

  Logger::Debug(kDTest, -1, "Test: Minimal transfers after leaves ...\n");

  for (int i = 0; i < 5; i++) {
    ck->Leave({npara + 1 + i});
  }
  auto c3 = ck->Query(-1);
  for (int i = 1; i <= npara; i++) {
    for (uint32_t j = 0; j < c1.shards_.size(); j++) {
      if (c2.shards_[j] == i) {
        if (c3.shards_[j] != i) {
          Logger::Debug(kDTest, -1, "non-minimal transfer after Leave()s");
          throw SHARDCTRLER_EXCEPTION("non-minimal transfer after Leave()s");
        }
      }
    }
  }

  Logger::Debug(kDTest, -1, "  ... Passed\n");

  EXPECT_TRUE(cfg.Cleanup());
}

}  // namespace kv::shardctrler