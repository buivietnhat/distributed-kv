#include "gtest/gtest.h"
#include "raft/configuration.h"

namespace kv::raft {

TEST(RaftLogTest, BasicAgreement) {
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

}  // namespace kv::raft