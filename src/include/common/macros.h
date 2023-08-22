#pragma once

#include <cassert>

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
