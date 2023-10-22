#pragma once

#include <tbb/task_group.h>

#include <future>
#include <iostream>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>

#include "common/container/channel.h"
#include "common/container/concurrent_blocking_queue.h"
#include "common/macros.h"
#include "common/util.h"
#include "fmt/format.h"
#include "network/client_end.h"
#include "raft/raft.h"
#include "shardctrler/server.h"

using namespace std::chrono_literals;

namespace kv::network {

using RequestArgs =
    std::variant<int, raft::RequestVoteArgs, raft::AppendEntryArgs, raft::InstallSnapshotArgs, shardctrler::JoinArgs,
                 shardctrler::LeaveArgs, shardctrler::MoveArgs, shardctrler::QueryArgs>;
using ReplyArgs =
    std::variant<int, raft::RequestVoteReply, raft::AppendEntryReply, raft::InstallSnapshotReply,
                 shardctrler::JoinReply, shardctrler::LeaveReply, shardctrler::MoveReply, shardctrler::QueryReply>;

enum class Method : uint8_t {
  RESERVERD,
  TEST,
  REQUEST_VOTE,
  APPEND_ENTRIES,
  INSTALL_SNAPSHOT,
  QUERY,
  JOIN,
  LEAVE,
  MOVE
};

struct ReplyMessage {
  bool ok_{false};
  ReplyArgs args_;
};

struct RequestMessage {
  std::string endname_;
  Method method_;
  RequestArgs args_;
  common::Channel<ReplyMessage> *chan_;

  RequestMessage(std::string endname, Method method, RequestArgs args, common::Channel<ReplyMessage> *chan)
      : endname_(std::move(endname)), method_(method), args_(std::move(args)), chan_(chan) {}

  RequestMessage() = default;
};

struct Server {
  using enum Method;

  std::mutex mu_;
  raft::Raft *raft_;
  shardctrler::ShardCtrler *shardctrler_;

  int count_{0};  // incoming RPCs

  void AddRaft(raft::Raft *raft) {
    std::lock_guard lock(mu_);
    raft_ = raft;
  }

  void AddShardCtrler(shardctrler::ShardCtrler *shardctrler) {
    std::lock_guard lock(mu_);
    shardctrler_ = shardctrler;
  }

  int GetCount() const { return count_; }

  // TODO(nhat): refactor this to use std::visit, alot of duplicated code, and it's quite ugly
  ReplyMessage DispatchReq(const RequestMessage &req) {
    count_ += 1;
    switch (req.method_) {
      case REQUEST_VOTE: {
        if (raft_ == nullptr) {
          throw std::runtime_error("unknown raft service, expecting one");
        }
        auto rep = raft_->RequestVote(std::get<raft::RequestVoteArgs>(req.args_));
        return {true, std::move(rep)};
      }
      case APPEND_ENTRIES: {
        if (raft_ == nullptr) {
          throw std::runtime_error("unknown raft service, expecting one");
        }
        auto rep = raft_->AppendEntries(std::get<raft::AppendEntryArgs>(req.args_));
        return {true, std::move(rep)};
      }
      case INSTALL_SNAPSHOT: {
        if (raft_ == nullptr) {
          throw std::runtime_error("unknown raft service, expecting one");
        }
        auto rep = raft_->InstallSnapshot(std::get<raft::InstallSnapshotArgs>(req.args_));
        return {true, std::move(rep)};
      }
      case QUERY: {
        if (shardctrler_ == nullptr) {
          throw std::runtime_error("unknown shardctrler service, expecting one");
        }
        auto rep = shardctrler_->Query(std::get<shardctrler::QueryArgs>(req.args_));
        return {true, std::move(rep)};
      }
      case JOIN: {
        if (shardctrler_ == nullptr) {
          throw std::runtime_error("unknown shardctrler service, expecting one");
        }
        auto rep = shardctrler_->Join(std::get<shardctrler::JoinArgs>(req.args_));
        return {true, std::move(rep)};
      }
      case LEAVE: {
        if (shardctrler_ == nullptr) {
          throw std::runtime_error("unknown shardctrler service, expecting one");
        }
        auto rep = shardctrler_->Leave(std::get<shardctrler::LeaveArgs>(req.args_));
        return {true, std::move(rep)};
      }
      case MOVE: {
        if (shardctrler_ == nullptr) {
          throw std::runtime_error("unknown shardctrler service, expecting one");
        }
        auto rep = shardctrler_->Move(std::get<shardctrler::MoveArgs>(req.args_));
        return {true, std::move(rep)};
      }
      case TEST: {
        if (raft_ == nullptr) {
          throw std::runtime_error("unknown raft service, expecting one");
        }
        auto rep = raft_->Test(std::get<int>(req.args_));
        return {true, std::move(rep)};
      }
      default:
        return {};
    }
  }
};

class MockingClientEnd : public ClientEnd {
 public:
  MockingClientEnd(std::string name, common::Channel<RequestMessage> &chan) : endname_(std::move(name)), chan_(chan) {}

  std::optional<raft::RequestVoteReply> RequestVote(const raft::RequestVoteArgs &args) const override {
    // entire Network has been destroyed
    if (finished_) {
      return {};
    }

    common::Channel<ReplyMessage> chan;
    RequestMessage req_msg{endname_, REQUEST_VOTE, args, &chan};
    chan_.Send(req_msg);

    auto reply = req_msg.chan_->Receive();
    if (reply.ok_) {
      return std::get<raft::RequestVoteReply>(reply.args_);
    }

    return {};
  }

  std::optional<raft::AppendEntryReply> AppendEntries(const raft::AppendEntryArgs &args) const override {
    // entire Network has been destroyed
    if (finished_) {
      return {};
    }

    common::Channel<ReplyMessage> chan;
    RequestMessage req_msg{endname_, APPEND_ENTRIES, args, &chan};
    chan_.Send(req_msg);

    auto reply = req_msg.chan_->Receive();
    if (reply.ok_) {
      return std::get<raft::AppendEntryReply>(reply.args_);
    }

    return {};
  }

  std::optional<raft::InstallSnapshotReply> InstallSnapshot(const raft::InstallSnapshotArgs &args) const override {
    if (finished_) {
      return {};
    }

    common::Channel<ReplyMessage> chan;
    RequestMessage req_msg{endname_, INSTALL_SNAPSHOT, args, &chan};
    chan_.Send(req_msg);

    auto reply = req_msg.chan_->Receive();
    if (reply.ok_) {
      return std::get<raft::InstallSnapshotReply>(reply.args_);
    }

    return {};
  }

  std::optional<shardctrler::QueryReply> Query(const shardctrler::QueryArgs &args) const override {
    if (finished_) {
      return {};
    }

    common::Channel<ReplyMessage> chan;

    RequestMessage req_msg{endname_, QUERY, args, &chan};
    chan_.Send(req_msg);

    auto reply = req_msg.chan_->Receive();
    if (reply.ok_) {
      return std::get<shardctrler::QueryReply>(reply.args_);
    }

    return {};
  }

  std::optional<shardctrler::JoinReply> Join(const shardctrler::JoinArgs &args) const override {
    if (finished_) {
      return {};
    }

    common::Channel<ReplyMessage> chan;
    RequestMessage req_msg{endname_, JOIN, args, &chan};
    chan_.Send(req_msg);

    auto reply = req_msg.chan_->Receive();
    if (reply.ok_) {
      return std::get<shardctrler::JoinReply>(reply.args_);
    }

    return {};
  }

  std::optional<shardctrler::LeaveReply> Leave(const shardctrler::LeaveArgs &args) const override {
    if (finished_) {
      return {};
    }

    common::Channel<ReplyMessage> chan;
    RequestMessage req_msg{endname_, LEAVE, args, &chan};
    chan_.Send(req_msg);

    auto reply = req_msg.chan_->Receive();
    if (reply.ok_) {
      return std::get<shardctrler::LeaveReply>(reply.args_);
    }

    return {};
  }

  std::optional<shardctrler::MoveReply> Move(const shardctrler::MoveArgs &args) const override {
    if (finished_) {
      return {};
    }

    common::Channel<ReplyMessage> chan;
    RequestMessage req_msg{endname_, MOVE, args, &chan};
    chan_.Send(req_msg);

    auto reply = req_msg.chan_->Receive();
    if (reply.ok_) {
      return std::get<shardctrler::MoveReply>(reply.args_);
    }

    return {};
  }

  std::optional<int> Test(int input) const override {
    // entire Network has been destroyed
    if (finished_) {
      return {};
    }

    common::Channel<ReplyMessage> chan;
    RequestMessage req_msg{endname_, TEST, input, &chan};
    chan_.Send(req_msg);

    auto reply = req_msg.chan_->Receive();
    if (reply.ok_) {
      return std::get<int>(reply.args_);
    }

    return {};
  }

  void Terminate() override { finished_ = true; }

 private:
  using enum Method;

  std::string endname_;
  common::Channel<RequestMessage> &chan_;
  bool finished_{false};
};

class Network {
 public:
  explicit Network() { dp_thread_ = std::thread(&Network::DispatchRequests, this); }

  ~Network() { Cleanup(); }

  void Connect(const std::string &endname, const std::string &servername) {
    std::lock_guard lock(mu_);
    connections_[endname] = servername;
  }

  void AddServer(const std::string &servername, std::unique_ptr<Server> server) {
    std::lock_guard lock(mu_);
    servers_[servername] = std::move(server);
  }

  void Enable(const std::string &endname, bool enabled) {
    std::lock_guard lock(mu_);
    enabled_[endname] = enabled;
  }

  void DeleteServer(const std::string &servername) {
    std::lock_guard lock(mu_);
    servers_[servername] = nullptr;
  }

  void SetReliable(bool rel) {
    std::lock_guard lock(mu_);
    reliable_ = rel;
  }

  void SetLongDelay(bool longdelay) {
    std::lock_guard lock(mu_);
    long_delay_ = longdelay;
  }

  // create a client endpoint
  // start the thread that listens and delivers.
  ClientEnd *MakeEnd(const std::string &endname) {
    std::lock_guard lock(mu_);

    if (ends_.contains(endname)) {
      throw std::runtime_error(fmt::format("Make end: {} already exists", endname));
    }

    auto end = std::make_unique<MockingClientEnd>(endname, chan_);
    ends_[endname] = std::move(end);
    enabled_[endname] = false;
    connections_[endname] = "";
    return ends_[endname].get();
  }

  void Cleanup() {
    if (!finished_) {
      finished_ = true;
      // stop receiving messages
      ShutdownClientEnds();

      chan_.Close();
      dp_thread_.join();
    }
  }

  int GetCount(const std::string &servername) const { return servers_.at(servername)->GetCount(); }

 private:
  void DispatchRequests() {
    while (!finished_) {
      auto req = chan_.Receive();

      if (req.chan_ == nullptr) {
        continue;
      }
      std::thread([&, req = std::move(req)] { ProcessRequest(std::move(req)); }).detach();
    }
  }

  void ShutdownClientEnds() {
    for (auto &[endname, end] : ends_) {
      end->Terminate();
    }
  }

  void ProcessRequest(RequestMessage req) {
    bool enable, reliable, longreordering;
    std::string servername;
    auto server = ReadEndnameInfo(req.endname_, &enable, &servername, &reliable, &longreordering);

    if (enable && servername != "" && server != nullptr) {
      if (!reliable) {
        // short delay
        auto ms = common::RandInt() % 27;
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
      }

      if (!reliable && (common::RandInt() % 1000 < 100)) {
        // drop the request, return as if timeout
        req.chan_->Send({});
        return;
      }

      // execute the request (call RCP handler) in a separate thread so that
      // we can periodically check if the server has been killed and the RPC should
      // get a failure reply
      ReplyMessage reply;
      std::mutex m;
      std::condition_variable cond;
      auto reply_ok = std::make_shared<bool>(false);
      auto is_server_dead = std::make_shared<bool>(false);

      auto fut1 = std::async(std::launch::async, [&, reply_ok] {
        reply = server->DispatchReq(req);
        std::lock_guard l(m);
        *reply_ok = true;
        cond.notify_one();
      });

      *is_server_dead = IsServerDead(req.endname_, servername, server.get());

      std::unique_lock l(m);
      while (true) {
        if (cond.wait_for(l, MS(100), [&] { return *reply_ok || *is_server_dead; })) {
          break;
        }
        *is_server_dead = IsServerDead(req.endname_, servername, server.get());
      }

      // wait until we have a reply or server is dead
      while (*reply_ok == false && *is_server_dead == false) {
      }

      if (*reply_ok == false || *is_server_dead == true) {
        // server was killed while we were waiting; return error
        req.chan_->Send({});
      } else if (reliable == false && (common::RandInt() % 1000) < 100) {
        // drop the reply, return as if timeout
        req.chan_->Send({});
      } else if (longreordering == true && common::RandNInt(900) < 600) {
        // delay the response for a while
        auto ms = 200 + common::RandNInt(1 + common::RandNInt(2000));
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        req.chan_->Send(reply);
      } else {
        req.chan_->Send(reply);
      }
    } else {
      // simulate no reply and eventual timeout
      int ms = 0;
      if (long_delay_) {
        ms = common::RandInt() % 7000;
      } else {
        ms = common::RandInt() % 100;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(ms));
      req.chan_->Send({});
    }
  }

  std::shared_ptr<Server> ReadEndnameInfo(const std::string &endname, bool *enable, std::string *server_name,
                                          bool *reliable, bool *longreordering) {
    std::shared_ptr<Server> server;
    std::lock_guard lock(mu_);
    *server_name = connections_[endname];
    if (*server_name != "") {
      server = servers_[*server_name];
    }
    *enable = enabled_[endname];
    *reliable = reliable_;
    *longreordering = long_reordering_;
    return server;
  }

  bool IsServerDead(const std::string &endname, const std::string &servername, Server *server) {
    std::lock_guard lock(mu_);

    if (enabled_[endname] == false || servers_[servername].get() != server) {
      return true;
    }

    return false;
  }

  std::mutex mu_;
  bool reliable_{true};
  bool long_delay_{false};
  bool long_reordering_{false};
  std::unordered_map<std::string, std::unique_ptr<ClientEnd>> ends_;
  std::unordered_map<std::string, bool> enabled_;
  std::unordered_map<std::string, std::shared_ptr<Server>> servers_;
  std::unordered_map<std::string, std::string> connections_;  // endname -> server name
  common::Channel<RequestMessage> chan_;
  std::thread dp_thread_;
  bool finished_{false};
};

}  // namespace kv::network