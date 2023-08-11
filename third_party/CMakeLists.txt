set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)  # don't override our compiler/linker options when building gtest
add_subdirectory(googletest)
add_subdirectory(fmt)