#pragma once

#include <ostream>
#include <stdexcept>
#include <string>

namespace kv {

#define NOT_IMPLEMENTED_EXCEPTION(msg) NotImplementedException(msg, __FILE__, __LINE__)
#define LOG_MANAGER_EXCEPTION(msg) LogManagerException(msg, __FILE__, __LINE__)
#define VOTER_EXCEPTION(msg) VoterException(msg, __FILE__, __LINE__)
#define RAFT_EXCEPTION(msg) RaftException(msg, __FILE__, __LINE__)
#define CONFIG_EXCEPTION(msg) ConfigException(msg, __FILE__, __LINE__)

/**
 * Exception types
 */
enum class ExceptionType : uint8_t { RESERVED, NOT_IMPLEMENTED, LOG_MANAGER, VOTER, RAFT, CONFIG };

/**
 * Exception base class.
 */
class Exception : public std::runtime_error {
 public:
  /**
   * Creates a new Exception with the given parameters.
   * @param type exception type
   * @param msg exception message to be displayed
   * @param file name of the file in which the exception occurred
   * @param line line number at which the exception occurred
   */
  Exception(const ExceptionType type, const char *msg, const char *file, int line)
      : std::runtime_error(msg), type_(type), file_(file), line_(line) {}

  /**
   * Allows type and source location of the exception to be recorded in the log
   * at the catch point.
   */
  friend std::ostream &operator<<(std::ostream &out, const Exception &ex) {
    out << ex.GetType() << " exception:";
    out << ex.GetFile() << ":";
    out << ex.GetLine() << ":";
    out << ex.what();
    return out;
  }

  /**
   * @return the exception type
   */
  const char *GetType() const {
    switch (type_) {
      case ExceptionType::NOT_IMPLEMENTED:
        return "Not Implemented";
      case ExceptionType::LOG_MANAGER:
        return "Log Manager";
      case ExceptionType::VOTER:
        return "Voter";
      case ExceptionType::RAFT:
        return "Raft";
      default:
        return "Unknown exception type";
    }
  }

  /**
   * @return the file that threw the exception
   */
  const char *GetFile() const { return file_; }

  /**
   * @return the line number that threw the exception
   */
  int GetLine() const { return line_; }

 protected:
  /**
   * The type of exception.
   */
  const ExceptionType type_;
  /**
   * The name of the file in which the exception was raised.
   */
  const char *file_;
  /**
   * The line number at which the exception was raised.
   */
  const int line_;
};

#define DEFINE_EXCEPTION(e_name, e_type)                                                                       \
  class e_name : public Exception {                                                                            \
    e_name() = delete;                                                                                         \
                                                                                                               \
   public:                                                                                                     \
    e_name(const char *msg, const char *file, int line) : Exception(e_type, msg, file, line) {}                \
    e_name(const std::string &msg, const char *file, int line) : Exception(e_type, msg.c_str(), file, line) {} \
  }

DEFINE_EXCEPTION(NotImplementedException, ExceptionType::NOT_IMPLEMENTED);
DEFINE_EXCEPTION(LogManagerException, ExceptionType::LOG_MANAGER);
DEFINE_EXCEPTION(VoterException, ExceptionType::VOTER);
DEFINE_EXCEPTION(RaftException, ExceptionType::RAFT);
DEFINE_EXCEPTION(ConfigException, ExceptionType::CONFIG);

}  // namespace kv