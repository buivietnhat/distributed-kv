cmake_minimum_required(VERSION 3.25)
project(distributedkv)

set(CMAKE_CXX_STANDARD 20)

if (NOT KV_SANITIZER)
    set(KV_SANITIZER address)
endif ()

message("Build mode: ${CMAKE_BUILD_TYPE}")
message("${KV_SANITIZER} sanitizer enabled in debug mode.")

# Compiler flags.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-parameter -Wno-attributes")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb -fsanitize=${KV_SANITIZER} -fno-omit-frame-pointer -fno-optimize-sibling-calls")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS_DEBUG: ${CMAKE_CXX_FLAGS_DEBUG}")
message(STATUS "CMAKE_EXE_LINKER_FLAGS: ${CMAKE_EXE_LINKER_FLAGS}")
message(STATUS "CMAKE_SHARED_LINKER_FLAGS: ${CMAKE_SHARED_LINKER_FLAGS}")

set(KV_BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build_support")
set(KV_CLANG_SEARCH_PATH "/usr/local/bin" "/usr/bin" "/usr/local/opt/llvm/bin" "/usr/local/opt/llvm@16/bin"
        "/usr/local/Cellar/llvm/16.0.1/bin" "/opt/homebrew/opt/llvm@16/bin/")

# attempt to find the binary if user did not specify
find_program(CLANG_FORMAT_BIN
        NAMES clang-format clang-format-16
        HINTS ${KV_CLANG_SEARCH_PATH})

if ("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND")
    message(WARNING "KV/main couldn't find clang-format.")
else ()
    message(STATUS "KV/main found clang-format at ${CLANG_FORMAT_BIN}")
endif ()

find_program(CPPLINT_BIN
        NAMES cpplint cpplint.py
        HINTS "${KV_BUILD_SUPPORT_DIR}")

if ("${CPPLINT_BIN}" STREQUAL "CPPLINT_BIN-NOTFOUND")
    message(WARNING "KV/main couldn't find cpplint.")
else ()
    message(STATUS "KV/main found cpplint at ${CPPLINT_BIN}")
endif ()

# TBB library
find_package(TBB REQUIRED)
message(STATUS "[FOUND] TBB ${TBB_VERSION}")

# Includes.
set(DISTRIBUTEDKV_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(DISTRIBUTEDKV_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)

include_directories(
        ${DISTRIBUTEDKV_SRC_INCLUDE_DIR}
        ${DISTRIBUTEDKV_TEST_INCLUDE_DIR}
        ${PROJECT_SOURCE_DIR}/third_party/fmt/include
        ${PROJECT_SOURCE_DIR}/third_party/json/include
)

enable_testing()

add_subdirectory(test)
add_subdirectory(third_party)

add_dependencies(gtest distributedkv)

file(GLOB_RECURSE
        DISTRIBUTEDKV_SRCS                  # Store the list of files into the variable ${DISTRIBUTEDKV_SRCS}.
        CONFIGURE_DEPENDS                   # Ask CMake to regenerate the build system if these files change.
        ${PROJECT_SOURCE_DIR}/src/*.cpp
)

set(KV_THIRDPARTY_LIBS
        TBB::tbb
        fmt)

add_library(distributedkv STATIC ${DISTRIBUTEDKV_SRCS})
target_link_libraries(distributedkv ${KV_THIRDPARTY_LIBS})

add_executable(main main/main.cpp)
target_link_libraries(main distributedkv)

string(CONCAT KV_FORMAT_DIRS
        "${CMAKE_CURRENT_SOURCE_DIR}/src,"
        "${CMAKE_CURRENT_SOURCE_DIR}/test,"
        "${CMAKE_CURRENT_SOURCE_DIR}/main,"
)

# Runs clang format and updates files in place.
add_custom_target(format ${KV_BUILD_SUPPORT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        ${KV_BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
        --source_dirs
        ${KV_FORMAT_DIRS}
        --fix
        --quiet
)

# Runs clang format and exits with a non-zero exit code if any files need to be reformatted
add_custom_target(check-format ${KV_BUILD_SUPPORT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        ${KV_BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
        --source_dirs
        ${KV_FORMAT_DIRS}
        --quiet
)

file(GLOB_RECURSE KV_LINT_FILES
        "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/test/*.h"
        "${CMAKE_CURRENT_SOURCE_DIR}/test/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/main/*.h"
        "${CMAKE_CURRENT_SOURCE_DIR}/main/*.cpp"
)

# Balancing act: cpplint.py takes a non-trivial time to launch,
# so process 12 files per invocation, while still ensuring parallelism
add_custom_target(check-lint echo '${KV_LINT_FILES}' | xargs -n12 -P8
        ${CPPLINT_BIN}
        --verbose=2 --quiet
        --linelength=120
        --filter=-legal/copyright,-build/header_guard,-runtime/references # https://github.com/cpplint/cpplint/issues/148
)



