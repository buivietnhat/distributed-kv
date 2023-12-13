#pragma once

#include <cassert>
#include <iostream>

//===--------------------------------------------------------------------===//
// ALWAYS_ASSERT
//===--------------------------------------------------------------------===//

#ifdef NDEBUG
#define KV_ASSERT(expr, message) ((void)0)
#else
/*
 * On assert failure, most existing implementations of C++ will print out the condition.
 * By ANDing the truthy not-null message and our initial expression together, we get
 * asserts-with-messages without needing to bring in iostream or logging.
 */
#define KV_ASSERT(expr, message) assert((expr) && (message))
#endif /* NDEBUG */

//===----------------------------------------------------------------------===//
// Handy macros to hide move/copy class constructors
//===----------------------------------------------------------------------===//

// Macros to disable copying and moving
#ifndef DISALLOW_COPY
#define DISALLOW_COPY(cname)     \
  /* Delete copy constructor. */ \
  cname(const cname &) = delete; \
  /* Delete copy assignment. */  \
  cname &operator=(const cname &) = delete;

#define DISALLOW_MOVE(cname)     \
  /* Delete move constructor. */ \
  cname(cname &&) = delete;      \
  /* Delete move assignment. */  \
  cname &operator=(cname &&) = delete;

/**
 * Disable copy and move.
 */
#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

/** Disallow instantiation of the class. This should be used for classes that only have static functions. */
#define DISALLOW_INSTANTIATION(cname) \
  /* Prevent instantiation. */        \
  cname() = delete;

#endif

//===----------------------------------------------------------------------===//
// Scope guard
//===----------------------------------------------------------------------===//
#define CONCAT2(x, y) x##y
#define CONCAT(x, y) CONCAT2(x, y)
#ifdef __COUNTER__
#define ANON_VAR(x) CONCAT(x, __COUNTER__)
#else
#define ANON_VAR(x) CONCAT(x, __LINE__)
#endif

#define ON_SCOPE_EXIT auto ANON_VAR(SCOPE_EXIT_STATE) = ScopeGuardOnExit() + [&]()

#define ON_SCOPE_EXIT_ROLLBACK(NAME) auto NAME = ScopeGuardOnExit() + [&]()

class ScopeGuardBase {
 public:
  ScopeGuardBase() {}
  //  void Commit() const noexcept { commit_ = true; }

  auto operator=(const ScopeGuardBase &other) -> ScopeGuardBase & = delete;

  // protected:
  //  ScopeGuardBase(ScopeGuardBase &&other) : commit_(other.commit_) { other.Commit(); }
  //  mutable bool commit_;
};

template <typename Func>
class ScopeGuard : public ScopeGuardBase {
 public:
  ScopeGuard(Func &&func) : func_(func) {}

  ScopeGuard(const Func &func) : func_(func) {}

  ScopeGuard(ScopeGuard &&other) : ScopeGuardBase(std::move(other)), func_(other.func_) {}

  ~ScopeGuard() {
    //    if (!commit_) {
    func_();
    //    }
  }

 private:
  Func func_;
};

// this trick is for c++14, in c++17 we can call the constructor directly
template <typename Func>
auto MakeGuard(Func &&func) -> ScopeGuard<Func> {
  return ScopeGuard<Func>(std::forward<Func>(func));
}

struct ScopeGuardOnExit {};

template <typename Func>
ScopeGuard<Func> operator+(ScopeGuardOnExit, Func &&func) {
  return ScopeGuard<Func>(std::forward<Func>(func));
}