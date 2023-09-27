#pragma once

#include <chrono>
#include <ctime>
#include <random>
#include <sstream>
#include <thread>

namespace kv {

#define MS(num) std::chrono::milliseconds(num)

}  // namespace kv

namespace kv::common {

static std::mt19937 rng(std::random_device{}());

inline int RandInt() {
  // Generate a random integer
  std::uniform_int_distribution<int> distribution(0, std::numeric_limits<int>::max());
  int randomInt = distribution(rng);
  return randomInt;
}

inline int RandNInt(int n) {
  // Create a random number generator engine and seed it with the current time
  std::random_device rd;
  std::mt19937 gen(rd());

  // Define the range for the random number
  std::uniform_int_distribution<> dis(0, n - 1);

  // Generate and return a random integer within the range [0, n-1]
  return dis(gen);
}

inline std::string RandString(int length) {
  static std::string char_set = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, char_set.size() - 1);

  std::string random_string;
  for (int i = 0; i < length; ++i) {
    random_string += char_set[dis(gen)];
  }

  return random_string;
}

using time_t = std::chrono::system_clock::time_point;
inline time_t Now() { return std::chrono::system_clock::now(); }

inline time_t AddTimeMs(time_t original, const std::chrono::milliseconds &duration) { return original + duration; }
// calculate elapsed time in ms
inline double ElapsedTimeMs(time_t before, time_t after) {
  std::chrono::duration<double, std::milli> elapsed_time = after - before;
  return elapsed_time.count();
}

inline double ElapsedTimeS(time_t before, time_t after) {
  std::chrono::duration<double> elapsed_time = after - before;
  return elapsed_time.count();
}

inline void SleepMs(uint32_t duration) { std::this_thread::sleep_for(MS(duration)); }

inline std::vector<std::pair<int, int>> SortByValue(const std::unordered_map<int, int> &map) {
  std::vector<std::pair<int, int>> pairs;
  pairs.reserve(map.size());
  for (const auto &[key, val] : map) {
    pairs.emplace_back(key, val);
  }
  std::sort(pairs.begin(), pairs.end(), [](const auto &a, const auto &b) { return a.second < b.second; });

  return pairs;
}

inline double CurrentTimeMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(Now().time_since_epoch()).count() % 100000;
}

template <typename T>
inline std::string ToString(const std::vector<T> vect) {
  std::stringstream ss;
  ss << "[ ";
  for (const auto &v : vect) {
    ss << v << " ";
  }
  ss << "]";
  return ss.str();
}

template <typename T>
inline std::string ToString(const std::vector<std::pair<T, T>> vect) {
  std::stringstream ss;
  ss << "[ ";
  for (const auto &[f, s] : vect) {
    ss << "(" << f << "," << s << ")"
       << " ";
  }
  ss << "]";
  return ss.str();
}

template <typename K, typename V>
inline std::string ToString(const std::unordered_map<K, V> &map) {
  std::stringstream ss;
  ss << "[ ";
  for (const auto [k, v] : map) {
    ss << "{" << k << "," << v << "} ";
  }
  ss << "]";
  return ss.str();
}

template <typename K, typename V>
inline std::string KeysToString(const std::unordered_map<K, V> &map) {
  std::stringstream ss;
  ss << "[ ";
  for (const auto &[k, _] : map) {
    ss << k << " ";
  }
  ss << "]";
  return ss.str();
}

}  // namespace kv::common
