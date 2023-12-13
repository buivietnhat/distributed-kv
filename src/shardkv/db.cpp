#include "shardkv/db.h"

#include "shardkv/common.h"

namespace kv::shardkv {

std::optional<std::string> Database::Get(const std::string &key) const {
  auto shard = KeyToShard(key);

  std::lock_guard l(mu_);

  if (data_.contains(shard) && data_.at(shard).contains(key)) {
    return data_.at(shard).at(key);
  }

  return {};
}

void Database::Put(const std::string &key, const std::string &value) {
  auto shard = KeyToShard(key);

  std::lock_guard l(mu_);

  data_[shard][key] = value;
}

void Database::Append(const std::string &key, const std::string &value) {
  auto shard = KeyToShard(key);

  std::lock_guard l(mu_);
  data_[shard][key] += value;
}

std::unordered_map<int, std::unordered_map<std::string, std::string>> Database::GetSnapShot() {
  return data_;
}

void Database::SetSnapshot(const std::unordered_map<int, std::unordered_map<std::string, std::string>> &data) {
  data_ = data;
}

std::unordered_map<std::string, std::string> Database::GetShard(int shard) const {
  if (data_.contains(shard)) {
    return data_.at(shard);
  }

  return {};
}

void Database::SetShard(int shard, const std::unordered_map<std::string, std::string> &data) {
  data_[shard] = data;
}

void Database::Lock() const {
  mu_.lock();
}

void Database::Unlock() const {
  mu_.unlock();
}

}  // namespace kv::shardkv