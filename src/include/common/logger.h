#pragma once

#include <iostream>
#include <string>

#include "common/macros.h"
#include "common/util.h"
#include "fmt/format.h"

namespace kv {

static constexpr const char *kDClient = "CLNT";
static constexpr const char *kDCommit = "CMIT";
static constexpr const char *kDDrop = "DROP";
static constexpr const char *kDError = "ERRO";
static constexpr const char *kDInfo = "INFO";
static constexpr const char *kDLeader = "LEAD";
static constexpr const char *kDLog = "LOG1";
static constexpr const char *kDLog2 = "LOG2";
static constexpr const char *kDPersist = "PERS";
static constexpr const char *kDSnap = "SNAP";
static constexpr const char *kDTerm = "TERM";
static constexpr const char *kDTest = "TEST";
static constexpr const char *kDTimer = "TIMR";
static constexpr const char *kDTrace = "TRCE";
static constexpr const char *kDVote = "VOTE";
static constexpr const char *kDWarn = "WARN";
static constexpr const char *kDServ = "SERV";
static constexpr const char *kDCler = "CLER";
static constexpr const char *kDDupl = "DUPL";
static constexpr const char *kDTrck = "TRCK";
static constexpr const char *kDShardCtr = "SCTR";
static constexpr const char *kDSnp1 = "SNP1";
static constexpr const char *kDLeader1 = "LED1";

}  // namespace kv

namespace kv::common {

class Logger {
 public:
  using Topic = std::string;
  DISALLOW_INSTANTIATION(Logger);

  static inline void Debug(const Topic &topic, int server, const std::string &message) {
#ifndef NDEBUG
    std::string prefix;
    if (server == -1) {
      prefix = fmt::format("{} {} ", CurrentTimeMs(), topic);
    } else {
      prefix = fmt::format("{} {} [{}] ", CurrentTimeMs(), topic, server);
    }

    auto log = prefix + message + "\n";
    std::cout << log;
#endif
  }

  static inline void Debug1(const Topic &topic, int server, int gid, const std::string &message) {
#ifndef NDEBUG
    std::string prefix;
    if (server == -1) {
      prefix = fmt::format("{} {} ", CurrentTimeMs(), topic);
    } else {
      prefix = fmt::format("{} {} [{}]({}) ", CurrentTimeMs(), topic, gid - 100, server);
    }

    auto log = prefix + message + "\n";
    std::cout << log;
#endif
  }
};

}  // namespace kv::common
