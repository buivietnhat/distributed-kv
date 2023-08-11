#pragma once

#include <format>
#include <future>
#include <iostream>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>

#include "common/container/concurrent_blocking_queue.h"
#include "common/macros.h"
#include "common/util.h"
#include "network/rpc_interface.h"
#include "raft/raft.h"
#include "raft/voter.h"

using namespace std::chrono_literals;

namespace kv::network {

using RequestArgs = std::variant<raft::RequestVoteArgs, raft::AppendEntryArgs>;
using ReplyMessage = std::variant<raft::RequestVoteReply, raft::AppendEntryReply>;

enum class Method : uint8_t { RESERVERD, REQUEST_VOTE, APPEND_ENTRIES };

struct RequestMessage {
  std::string endname_;
  Method method_;
  RequestArgs args_;
  common::ConcurrentBlockingQueue<ReplyMessage> *rep_queue_;

  RequestMessage(std::string endname, Method method, RequestArgs args,
                 common::ConcurrentBlockingQueue<ReplyMessage> *rep_queue)
      : endname_(std::move(endname)), method_(method), args_(std::move(args)), rep_queue_(rep_queue) {}

  RequestMessage() = default;
};

struct Server {
  using enum Method;

  std::mutex mu_;
  raft::Raft *raft_;

  void AddRaft(raft::Raft *raft) {
    std::lock_guard lock(mu_);
    raft_ = raft;
  }

  ReplyMessage DispatchReq(const RequestMessage &req) {
    switch (req.method_) {
      case REQUEST_VOTE: {
        if (raft_ == nullptr) {
          throw std::runtime_error("unknown raft service, expecting one");
        }
        return raft_->RequestVote(std::get<raft::RequestVoteArgs>(req.args_));
      }
      case APPEND_ENTRIES: {
        if (raft_ == nullptr) {
          throw std::runtime_error("unknown raft service, expecting one");
        }
        return raft_->AppendEntries(std::get<raft::AppendEntryArgs>(req.args_));
      }
      default:
        return {};
    }
  }
};

class MockingClientEnd : public ClientEnd {
 public:
  MockingClientEnd(std::string name, common::ConcurrentBlockingQueue<RequestMessage> &queue)
      : endname_(std::move(name)), msg_queue_(queue) {}

  std::optional<raft::RequestVoteReply> RequestVote(const raft::RequestVoteArgs &args) const override {
    // entire Network has been destroyed
    if (finished_) {
      return {};
    }

    common::ConcurrentBlockingQueue<ReplyMessage> rep_queue;
    RequestMessage req_msg{endname_, REQUEST_VOTE, args, &rep_queue};
    msg_queue_.Enqueue(req_msg);

    ReplyMessage reply;
    req_msg.rep_queue_->Dequeue(&reply);
    if (std::holds_alternative<raft::RequestVoteReply>(reply)) {
      return std::get<raft::RequestVoteReply>(reply);
    }

    return {};
  }

  std::optional<raft::AppendEntryReply> AppendEntries(const raft::AppendEntryArgs &args) const override {
    // entire Network has been destroyed
    if (finished_) {
      return {};
    }

    common::ConcurrentBlockingQueue<ReplyMessage> rep_queue;
    RequestMessage req_msg{endname_, APPEND_ENTRIES, args, &rep_queue};
    msg_queue_.Enqueue(req_msg);

    ReplyMessage reply;
    req_msg.rep_queue_->Dequeue(&reply);
    if (std::holds_alternative<raft::AppendEntryReply>(reply)) {
      return std::get<raft::AppendEntryReply>(reply);
    }

    return {};
  }

  void Terminate() override { finished_ = true; }

 private:
  using enum Method;

  std::string endname_;
  common::ConcurrentBlockingQueue<RequestMessage> &msg_queue_;
  bool finished_{false};
};

class Network {
 public:
  explicit Network() { dp_thread_ = std::thread(&Network::DispatchRequests, this); }

  ~Network() { Cleanup(); }

  void Connect(const std::string &endname, const std::string servername) {
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

    auto end = std::make_unique<MockingClientEnd>(endname, msg_queue_);
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

      // enqueue an empty message to wake up the dispatching thread
      msg_queue_.Enqueue({});
      dp_thread_.join();
    }
  }

 private:
  void DispatchRequests() {
    while (!finished_) {
      RequestMessage req;
      msg_queue_.Dequeue(&req);

      if (req.rep_queue_ == nullptr) {
        continue;
      }
      ProcessRequest(req);
    }
    // drain all the queueing requests, ask as the server has been killed
    while (!msg_queue_.Empty()) {
      RequestMessage req;
      msg_queue_.Dequeue(&req);
      if (req.rep_queue_ != nullptr) {
        req.rep_queue_->Enqueue({});
      }
    }
  }

  void ShutdownClientEnds() {
    for (auto &[endname, end] : ends_) {
      end->Terminate();
    }
  }

  void ProcessRequest(const RequestMessage &req) {
    bool enable, reliable, longreordering;
    std::string servername("");
    auto *server = ReadEndnameInfo(req.endname_, &enable, &servername, &reliable, &longreordering);

    if (enable && servername != "" && server != nullptr) {
      if (!reliable) {
        // short delay
        auto ms = common::RandInt() % 27;
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
      }

      if (!reliable && (common::RandInt() % 1000 < 100)) {
        // drop the request, return as if timeout
        req.rep_queue_->Enqueue({});
        return;
      }

      // execute the request (call RCP handler) in a separate thread so that
      // we can periodically check if the server has been killed and the RPC should
      // get a failure reply
      ReplyMessage reply;
      bool reply_ok{false};
      bool is_server_dead{false};

      auto dp_fut = std::async(std::launch::async, [&] {
        reply = server->DispatchReq(req);
        reply_ok = true;
      });

      auto timer_fut = std::async(std::launch::async, [&] {
        while (!reply_ok) {
          std::this_thread::sleep_for(100ms);
          is_server_dead = IsServerDead(req.endname_, servername, server);
          if (is_server_dead) {
            return;
          }
        }
      });

      // wait until we have a reply or server is dead
      while (reply_ok == false && is_server_dead == false) {
      }

      if (reply_ok == false || is_server_dead == true) {
        // server was killed while we were waiting; return error
        req.rep_queue_->Enqueue({});
      } else if (reliable == false && (common::RandInt() % 1000) < 100) {
        // drop the reply, return as if timeout
        req.rep_queue_->Enqueue({});
      } else if (longreordering == true && common::RandNInt(900) < 600) {
        // delay the response for a while
        auto ms = 200 + common::RandNInt(1 + common::RandNInt(2000));
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        req.rep_queue_->Enqueue(reply);
      } else {
        req.rep_queue_->Enqueue(reply);
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
      req.rep_queue_->Enqueue({});
    }
  }

  Server *ReadEndnameInfo(const std::string &endname, bool *enable, std::string *server_name, bool *reliable,
                          bool *longreordering) {
    Server *server{nullptr};
    std::lock_guard lock(mu_);
    *server_name = connections_[endname];
    if (*server_name != "") {
      server = servers_[*server_name].get();
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
  std::unordered_map<std::string, std::unique_ptr<Server>> servers_;
  std::unordered_map<std::string, std::string> connections_;  // endname -> server name
  common::ConcurrentBlockingQueue<RequestMessage> msg_queue_;
  std::thread dp_thread_;
  bool finished_{false};
};

}  // namespace kv::network