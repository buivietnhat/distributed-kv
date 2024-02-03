#pragma once

#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <boost/fiber/all.hpp>

namespace kv::shardkv {

class Database {
 public:
  std::optional<std::string> Get(const std::string &key) const;

  void Put(const std::string &key, const std::string &value);
  void Append(const std::string &key, const std::string &value);

  std::unordered_map<int, std::unordered_map<std::string, std::string>> GetSnapShot();

  void SetSnapshot(const std::unordered_map<int, std::unordered_map<std::string, std::string>> &data);

  std::unordered_map<std::string, std::string> GetShard(int shard) const;

  void SetShard(int shard, const std::unordered_map<std::string, std::string> &data);

  void Lock() const;

  void Unlock() const;

  ~Database() = default;

 private:
  std::unordered_map<int, std::unordered_map<std::string, std::string>> data_;  // shardid -> {key-value}
  mutable boost::fibers::mutex mu_;
};

}  // namespace kv::shardkv
