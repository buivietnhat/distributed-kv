#include "network/network.h"

#include "common/container/channel.h"
#include "common/logger.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "raft/raft.h"

namespace kv::network {

TEST(NetworkTest, TestBasic) {
  Network rn;
  raft::Raft rf;

  auto e = rn.MakeEnd("end1-99");
  auto server = std::make_unique<Server>();
  server->AddRaft(&rf);
  rn.AddServer("server99", std::move(server));

  rn.Connect("end1-99", "server99");
  rn.Enable("end1-99", true);

  {
    int x = 10;
    auto reply = e->Test(x);
    EXPECT_TRUE(reply);
    EXPECT_EQ(110, *reply);
  }
}

TEST(NetworkTest, TestDisconnect) {
  Network rn;
  raft::Raft rf;

  auto e = rn.MakeEnd("end1-99");
  auto server = std::make_unique<Server>();
  server->AddRaft(&rf);
  rn.AddServer("server99", std::move(server));

  rn.Connect("end1-99", "server99");

  {
    int x = 10;
    auto reply = e->Test(x);
    EXPECT_FALSE(reply);
  }

  rn.Enable("end1-99", true);

  {
    auto reply = e->Test(100);
    EXPECT_TRUE(reply);
    EXPECT_EQ(200, *reply);
  }
}

// test RPCs from concurrent ClientEnds
TEST(NetworkTest, TestConcurrentMany) {
  Network rn;
  raft::Raft rf;
  auto server = std::make_unique<Server>();
  server->AddRaft(&rf);
  rn.AddServer("1000", std::move(server));

  int nclients = 20;
  int nrpcs = 10;
  common::Channel<int> ch;

  for (int ii = 0; ii < nclients; ii++) {
    std::thread([&, ii] {
      auto i = std::to_string(ii);
      int n = 0;
      auto e = rn.MakeEnd(i);
      rn.Connect(i, "1000");
      rn.Enable(i, true);

      for (int j = 0; j < nrpcs; j++) {
        auto arg = ii * 100 + j;
        auto rep = e->Test(arg);
        EXPECT_TRUE(rep);
        EXPECT_EQ(arg + 100, *rep);
        n += 1;
      }

      ch.Send(n);
    }).detach();
  }

  int total = 0;
  for (int ii = 0; ii < nclients; ii++) {
    auto x = ch.Receive();
    total += x;
  }

  auto n = rn.GetCount("1000");

  EXPECT_EQ(total, n);
}

TEST(NetworkTest, TestUnreliable) {
  Network rn;
  rn.SetReliable(false);

  raft::Raft rf;
  auto server = std::make_unique<Server>();
  server->AddRaft(&rf);
  rn.AddServer("1000", std::move(server));

  common::Channel<int> ch;
  int nclients = 300;
  for (int ii = 0; ii < nclients; ii++) {
    std::thread([&, ii] {
      auto i = std::to_string(ii);
      int n = 0;
      auto e = rn.MakeEnd(i);
      rn.Connect(i, "1000");
      rn.Enable(i, true);

      auto arg = ii * 100;
      auto rep = e->Test(arg);
      if (rep) {
        int wanted = arg + 100;
        EXPECT_EQ(wanted, *rep);
        n += 1;
      }

      ch.Send(n);
    }).detach();
  }

  int total = 0;
  for (int ii = 0; ii < nclients; ii++) {
    auto x = ch.Receive();
    total += x;
  }

  if (total == nclients || total == 0) {
    FAIL() << "all RPCs succeeded despite unreliable";
  }
}

// test concurrent RPCs from a single ClientEnd
TEST(NetworkTest, TestConcurrentOne) {
  Network rn;

  raft::Raft rf;
  auto server = std::make_unique<Server>();
  server->AddRaft(&rf);
  rn.AddServer("1000", std::move(server));

  auto e = rn.MakeEnd("c");
  rn.Connect("c", "1000");
  rn.Enable("c", true);

  common::Channel<int> ch;
  int nrpcs = 20;
  for (int ii = 0; ii < nrpcs; ii++) {
    std::thread([&, ii] {
      auto i = std::to_string(ii);
      int n = 0;

      auto arg = ii * 100;
      auto rep = e->Test(arg);
      EXPECT_TRUE(rep);
      EXPECT_EQ(arg + 100, *rep);
      n += 1;

      ch.Send(n);
    }).detach();
  }

  int total = 0;
  for (int ii = 0; ii < nrpcs; ii++) {
    auto x = ch.Receive();
    total += x;
  }

  EXPECT_EQ(total, nrpcs);
  auto n = rn.GetCount("1000");
  EXPECT_EQ(n, nrpcs);
}

// regression: an RPC that's delayed during Enabled=false
// should not delay subsequent RPCs (e.g. after Enabled=true).

TEST(NetworkTest, TestRegression) {
  Network rn;

  raft::Raft rf;
  auto server = std::make_unique<Server>();
  server->AddRaft(&rf);
  rn.AddServer("1000", std::move(server));

  auto e = rn.MakeEnd("c");
  rn.Connect("c", "1000");

  // start some RPCs while the ClientEnd is disabled.
  // they'll be delayed.
  rn.Enable("c", false);
  common::Channel<bool> ch;
  int nrpcs = 20;
  for (int ii = 0; ii < nrpcs; ii++) {
    std::thread([&, ii] {
      auto i = std::to_string(ii);
      bool ok = false;

      auto arg = ii + 100;

      // this call ought to return false
      auto rep = e->Test(arg);
      EXPECT_FALSE(rep);
      ok = true;

      ch.Send(ok);
    }).detach();
  }

  common::SleepMs(100);

  // now enable the ClientEnd and check that an RPC completes quickly.
  auto t0 = common::Now();
  rn.Enable("c", true);
  {
    auto arg = 99;
    auto rep = e->Test(arg);
    EXPECT_TRUE(rep);
    EXPECT_EQ(arg + 100, *rep);
  }

  auto dur = common::ElapsedTimeS(common::Now(), t0);
  if (dur > 0.03) {
    FAIL() << "RPC took too long "
           << "(" << dur << ") after Enable";
  }

  for (int ii = 0; ii < nrpcs; ii++) {
    ch.Receive();
  }

  auto n = rn.GetCount("1000");
  EXPECT_EQ(n, 1);
}

//
// if an RPC is stuck in a server, and the server
// is killed with DeleteServer(), does the RPC
// get un-stuck?
//
TEST(NetworkTest, DISABLED_TestKilled) {
  Network rn;

  auto e = rn.MakeEnd("end1-99");

  raft::Raft rf;
  auto server = std::make_unique<Server>();
  server->AddRaft(&rf);
  rn.AddServer("server99", std::move(server));

  rn.Connect("end1-99", "server99");
  rn.Enable("end1-99", true);

  common::Channel<bool> ch;
  std::thread([&] {
    auto rep = e->Test(99);
    if (rep) {
      ch.Send(true);
    } else {
      ch.Send(false);
    }
  }).detach();

  common::SleepMs(1000);
  bool ok = false;
  std::thread([&] {
    common::SleepMs(100);
    ok = true;
  }).detach();

  //  ch.Receive();
  //  if (!ok) {
  //    FAIL() << "Handle should not have returned yet";
  //  }

  rn.DeleteServer("server99");
  ok = true;
  std::thread([&] {
    common::SleepMs(100);
    ok = false;
  }).detach();

  auto reply_ok = ch.Receive();
  if (!ok) {
    FAIL() << "Handle should return after DeleteServer()";
  }

  if (reply_ok != false) {
    FAIL() << "Handle returned successfully despite DeleteServer()";
  }
}

}  // namespace kv::network
