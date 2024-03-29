#pragma once

#include <boost/fiber/all.hpp>
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
#include "shardkv/server.h"

using namespace std::chrono_literals;

namespace kv::network {

static constexpr int kTimeout = 10000;

using RequestArgs =
    std::variant<int, raft::RequestVoteArgs, raft::AppendEntryArgs, raft::InstallSnapshotArgs, shardctrler::JoinArgs,
                 shardctrler::LeaveArgs, shardctrler::MoveArgs, shardctrler::QueryArgs, shardkv::PutAppendArgs,
                 shardkv::GetArgs, shardkv::InstallShardArgs>;
using ReplyArgs =
    std::variant<int, raft::RequestVoteReply, raft::AppendEntryReply, raft::InstallSnapshotReply,
                 shardctrler::JoinReply, shardctrler::LeaveReply, shardctrler::MoveReply, shardctrler::QueryReply,
                 shardkv::PutAppendReply, shardkv::GetReply, shardkv::InstallShardReply>;

struct ReplyMessage {
  bool ok_{false};
  ReplyArgs args_;
};

struct RequestMessage {
  std::string endname_;
  RequestArgs args_;
  std::shared_ptr<boost::fibers::unbuffered_channel<ReplyMessage>> chan_;

  RequestMessage(std::string endname, RequestArgs args,
                 std::shared_ptr<boost::fibers::unbuffered_channel<ReplyMessage>> chan)
      : endname_(std::move(endname)), args_(std::move(args)), chan_(chan) {}

  RequestMessage() = default;
};

struct Server {
  boost::fibers::mutex mu_;
  raft::Raft *raft_;
  std::shared_ptr<shardctrler::ShardCtrler> shardctrler_;
  std::shared_ptr<shardkv::ShardKV> shardkv_;

  int count_{0};  // incoming RPCs

  void AddRaft(raft::Raft *raft) {
    std::lock_guard lock(mu_);
    raft_ = raft;
  }

  void AddShardCtrler(std::shared_ptr<shardctrler::ShardCtrler> shardctrler) {
    std::lock_guard lock(mu_);
    shardctrler_ = shardctrler;
  }

  void AddShardKV(std::shared_ptr<shardkv::ShardKV> shardkv) {
    std::lock_guard lock(mu_);
    shardkv_ = shardkv;
  }

  int GetCount() const { return count_; }

  struct RequestDispatcher {
    RequestDispatcher(Server *srv) : srv_(srv) {
      if (srv_ == nullptr) {
        throw std::runtime_error("Passing nullptr server to RequestDispatcher");
      }
    }

    ReplyMessage operator()(int args) {
      if (srv_->raft_ == nullptr) {
        throw std::runtime_error("unknown raft service, expecting one");
      }
      auto rep = srv_->raft_->Test(args);
      return {true, std::move(rep)};
    }

    ReplyMessage operator()(const raft::RequestVoteArgs &args) {
      if (srv_->raft_ == nullptr) {
        throw std::runtime_error("unknown raft service, expecting one");
      }
      auto rep = srv_->raft_->RequestVote(args);
      return {true, std::move(rep)};
    }

    ReplyMessage operator()(const raft::AppendEntryArgs &args) {
      if (srv_->raft_ == nullptr) {
        throw std::runtime_error("unknown raft service, expecting one");
      }
      auto rep = srv_->raft_->AppendEntries(args);
      return {true, std::move(rep)};
    }

    ReplyMessage operator()(const raft::InstallSnapshotArgs &args) {
      if (srv_->raft_ == nullptr) {
        throw std::runtime_error("unknown raft service, expecting one");
      }
      auto rep = srv_->raft_->InstallSnapshot(args);
      return {true, std::move(rep)};
    }

    ReplyMessage operator()(const shardctrler::QueryArgs &args) {
      if (srv_->shardctrler_ == nullptr) {
        throw std::runtime_error("unknown shardctrler service, expecting one");
      }
      auto rep = srv_->shardctrler_->Query(args);
      return {true, std::move(rep)};
    }

    ReplyMessage operator()(const shardctrler::JoinArgs &args) {
      if (srv_->shardctrler_ == nullptr) {
        throw std::runtime_error("unknown shardctrler service, expecting one");
      }
      auto rep = srv_->shardctrler_->Join(args);
      return {true, std::move(rep)};
    }

    ReplyMessage operator()(const shardctrler::LeaveArgs &args) {
      if (srv_->shardctrler_ == nullptr) {
        throw std::runtime_error("unknown shardctrler service, expecting one");
      }
      auto rep = srv_->shardctrler_->Leave(args);
      return {true, std::move(rep)};
    }

    ReplyMessage operator()(const shardctrler::MoveArgs &args) {
      if (srv_->shardctrler_ == nullptr) {
        throw std::runtime_error("unknown shardctrler service, expecting one");
      }
      auto rep = srv_->shardctrler_->Move(args);
      return {true, std::move(rep)};
    }

    ReplyMessage operator()(const shardkv::PutAppendArgs &args) {
      if (srv_->shardkv_ == nullptr) {
        throw std::runtime_error("unknown shardkv service, expecting one");
      }
      auto rep = srv_->shardkv_->PutAppend(args);
      return {true, std::move(rep)};
    }

    ReplyMessage operator()(const shardkv::GetArgs &args) {
      if (srv_->shardkv_ == nullptr) {
        throw std::runtime_error("unknown shardkv service, expecting one");
      }
      auto rep = srv_->shardkv_->Get(args);
      return {true, std::move(rep)};
    }

    ReplyMessage operator()(const shardkv::InstallShardArgs &args) {
      if (srv_->shardkv_ == nullptr) {
        throw std::runtime_error("unknown shardkv service, expecting one");
      }
      auto rep = srv_->shardkv_->InstallShard(args);
      return {true, std::move(rep)};
    }

    Server *srv_;
  };

  ReplyMessage DispatchReq(const RequestMessage &req) {
    count_ += 1;

    return std::visit(RequestDispatcher{this}, req.args_);
  }
};

class MockingClientEnd : public ClientEnd {
 public:
  MockingClientEnd(std::string name, boost::fibers::unbuffered_channel<RequestMessage> &chan)
      : endname_(std::move(name)), chan_(chan) {}

  std::optional<raft::RequestVoteReply> RequestVote(const raft::RequestVoteArgs &args) const override {
    // entire Network has been destroyed
    if (finished_) {
      return {};
    }

    auto chan = std::make_shared<boost::fibers::unbuffered_channel<ReplyMessage>>();
    RequestMessage req_msg{endname_, args, chan};
    chan_.push(req_msg);

    ReplyMessage reply;
    req_msg.chan_->pop_wait_for(reply, MS(kTimeout));
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

    auto chan = std::make_shared<boost::fibers::unbuffered_channel<ReplyMessage>>();
    RequestMessage req_msg{endname_, args, chan};
    chan_.push(req_msg);

    ReplyMessage reply;
    req_msg.chan_->pop_wait_for(reply, MS(kTimeout));
    if (reply.ok_) {
      return std::get<raft::AppendEntryReply>(reply.args_);
    }

    return {};
  }

  std::optional<raft::InstallSnapshotReply> InstallSnapshot(const raft::InstallSnapshotArgs &args) const override {
    if (finished_) {
      return {};
    }

    auto chan = std::make_shared<boost::fibers::unbuffered_channel<ReplyMessage>>();
    RequestMessage req_msg{endname_, args, chan};
    chan_.push(req_msg);

    ReplyMessage reply;
    req_msg.chan_->pop_wait_for(reply, MS(kTimeout));
    if (reply.ok_) {
      return std::get<raft::InstallSnapshotReply>(reply.args_);
    }

    return {};
  }

  std::optional<shardctrler::QueryReply> Query(const shardctrler::QueryArgs &args) const override {
    if (finished_) {
      return {};
    }

    auto chan = std::make_shared<boost::fibers::unbuffered_channel<ReplyMessage>>();
    RequestMessage req_msg{endname_, args, chan};
    chan_.push(req_msg);

    ReplyMessage reply;
    req_msg.chan_->pop_wait_for(reply, MS(kTimeout));
    if (reply.ok_) {
      return std::get<shardctrler::QueryReply>(reply.args_);
    }

    return {};
  }

  std::optional<shardctrler::JoinReply> Join(const shardctrler::JoinArgs &args) const override {
    if (finished_) {
      return {};
    }

    auto chan = std::make_shared<boost::fibers::unbuffered_channel<ReplyMessage>>();
    RequestMessage req_msg{endname_, args, chan};
    chan_.push(req_msg);

    ReplyMessage reply;
    req_msg.chan_->pop_wait_for(reply, MS(kTimeout));
    if (reply.ok_) {
      return std::get<shardctrler::JoinReply>(reply.args_);
    }

    return {};
  }

  std::optional<shardctrler::LeaveReply> Leave(const shardctrler::LeaveArgs &args) const override {
    if (finished_) {
      return {};
    }

    auto chan = std::make_shared<boost::fibers::unbuffered_channel<ReplyMessage>>();
    RequestMessage req_msg{endname_, args, chan};
    chan_.push(req_msg);

    ReplyMessage reply;
    req_msg.chan_->pop_wait_for(reply, MS(kTimeout));
    if (reply.ok_) {
      return std::get<shardctrler::LeaveReply>(reply.args_);
    }

    return {};
  }

  std::optional<shardctrler::MoveReply> Move(const shardctrler::MoveArgs &args) const override {
    if (finished_) {
      return {};
    }

    auto chan = std::make_shared<boost::fibers::unbuffered_channel<ReplyMessage>>();
    RequestMessage req_msg{endname_, args, chan};
    chan_.push(req_msg);

    ReplyMessage reply;
    req_msg.chan_->pop_wait_for(reply, MS(kTimeout));
    if (reply.ok_) {
      return std::get<shardctrler::MoveReply>(reply.args_);
    }

    return {};
  }

  std::optional<shardkv::GetReply> Get(const shardkv::GetArgs &args) const override {
    if (finished_) {
      return {};
    }

    auto chan = std::make_shared<boost::fibers::unbuffered_channel<ReplyMessage>>();
    RequestMessage req_msg{endname_, args, chan};
    chan_.push(req_msg);

    ReplyMessage reply;
    req_msg.chan_->pop_wait_for(reply, MS(kTimeout));
    if (reply.ok_) {
      return std::get<shardkv::GetReply>(reply.args_);
    }

    return {};
  }

  std::optional<shardkv::PutAppendReply> PutAppend(const shardkv::PutAppendArgs &args) const override {
    if (finished_) {
      return {};
    }

    auto chan = std::make_shared<boost::fibers::unbuffered_channel<ReplyMessage>>();
    RequestMessage req_msg{endname_, args, chan};
    chan_.push(req_msg);

    ReplyMessage reply;
    req_msg.chan_->pop_wait_for(reply, MS(kTimeout));
    if (reply.ok_) {
      return std::get<shardkv::PutAppendReply>(reply.args_);
    }

    return {};
  }

  std::optional<shardkv::InstallShardReply> InstallShard(const shardkv::InstallShardArgs &args) const override {
    if (finished_) {
      return {};
    }

    auto chan = std::make_shared<boost::fibers::unbuffered_channel<ReplyMessage>>();
    RequestMessage req_msg{endname_, args, chan};
    chan_.push(req_msg);

    ReplyMessage reply;
    req_msg.chan_->pop_wait_for(reply, MS(kTimeout));
    if (reply.ok_) {
      return std::get<shardkv::InstallShardReply>(reply.args_);
    }

    return {};
  }

  std::optional<int> Test(int input) const override {
    // entire Network has been destroyed
    if (finished_) {
      return {};
    }

    auto chan = std::make_shared<boost::fibers::unbuffered_channel<ReplyMessage>>();
    RequestMessage req_msg{endname_, input, chan};
    chan_.push(req_msg);

    ReplyMessage reply;
    req_msg.chan_->pop_wait_for(reply, MS(kTimeout));
    if (reply.ok_) {
      return std::get<int>(reply.args_);
    }

    return {};
  }

  void Terminate() override { finished_ = true; }

 private:
  std::string endname_;
  boost::fibers::unbuffered_channel<RequestMessage> &chan_;
  bool finished_{false};
};

class Network : public std::enable_shared_from_this<Network> {
 public:
  explicit Network() { dp_thread_ = boost::fibers::fiber(&Network::DispatchRequests, this); }

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

      chan_.close();
      dp_thread_.join();
    }
  }

  int GetCount(const std::string &servername) const { return servers_.at(servername)->GetCount(); }

 private:
  void DispatchRequests() {
    while (!finished_) {
      RequestMessage req;
      chan_.pop(req);

      if (req.chan_ == nullptr) {
        continue;
      }

      boost::fibers::fiber([me = shared_from_this(), req = std::move(req)] {
        me->ProcessRequest(std::move(req));
      }).detach();
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
        boost::this_fiber::sleep_for(std::chrono::milliseconds(ms));
      }

      if (!reliable && (common::RandInt() % 1000 < 100)) {
        // drop the request, return as if timeout
        req.chan_->push({});
        return;
      }

      // execute the request (call RCP handler) in a separate thread so that
      // we can periodically check if the server has been killed and the RPC should
      // get a failure reply
      ReplyMessage reply;
      boost::fibers::mutex m;
      boost::fibers::condition_variable cond;
      auto reply_ok = std::make_shared<bool>(false);
      auto is_server_dead = std::make_shared<bool>(false);

      boost::fibers::fiber f([&, reply_ok, server = server] {
        reply = server->DispatchReq(req);
        std::lock_guard l(m);
        *reply_ok = true;
        cond.notify_one();
      });

      ON_SCOPE_EXIT { f.join(); };

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
        req.chan_->push({});
      } else if (reliable == false && (common::RandInt() % 1000) < 100) {
        // drop the reply, return as if timeout
        req.chan_->push({});
      } else if (longreordering == true && common::RandNInt(900) < 600) {
        // delay the response for a while
        auto ms = 200 + common::RandNInt(1 + common::RandNInt(2000));
        boost::this_fiber::sleep_for(std::chrono::milliseconds(ms));
        req.chan_->push(reply);
      } else {
        req.chan_->push(reply);
      }
    } else {
      // simulate no reply and eventual timeout
      int ms = 0;
      if (long_delay_) {
        ms = common::RandInt() % 7000;
      } else {
        ms = common::RandInt() % 100;
      }
      boost::this_fiber::sleep_for(std::chrono::milliseconds(ms));
      req.chan_->push({});
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

  boost::fibers::mutex mu_;
  bool reliable_{true};
  bool long_delay_{false};
  bool long_reordering_{false};
  std::unordered_map<std::string, std::unique_ptr<ClientEnd>> ends_;
  std::unordered_map<std::string, bool> enabled_;
  std::unordered_map<std::string, std::shared_ptr<Server>> servers_;
  std::unordered_map<std::string, std::string> connections_;  // endname -> server name
  boost::fibers::unbuffered_channel<RequestMessage> chan_;
  boost::fibers::fiber dp_thread_;
  bool finished_{false};
  //  common::ThreadRegistry tr_;
};

}  // namespace kv::network