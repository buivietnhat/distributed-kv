cmake_minimum_required(VERSION 3.10)

include(GoogleTest)

file(GLOB DISTRIBUTEDKV_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*/*test.cpp")

add_custom_target(build-tests COMMAND ${CMAKE_CTEST_COMMAND} --show-only)
add_custom_target(check-tests COMMAND ${CMAKE_CTEST_COMMAND} --verbose)

foreach (distributedkv_test_source ${DISTRIBUTEDKV_TEST_SOURCES})
    # Create a human readable name.
    get_filename_component(distributedkv_test_filename ${distributedkv_test_source} NAME)
    string(REPLACE ".cpp" "" distributedkv_test_name ${distributedkv_test_filename})

    # Add the test target separately and as part of "make check-tests".
    add_executable(${distributedkv_test_name} EXCLUDE_FROM_ALL ${distributedkv_test_source})
    add_dependencies(build-tests ${distributedkv_test_name})
    add_dependencies(check-tests ${distributedkv_test_name})

    gtest_discover_tests(${distributedkv_test_name}
            EXTRA_ARGS
            --gtest_color=yes
            --gtest_output=xml:${CMAKE_BINARY_DIR}/test/${distributedkv_test_name}.xml)
    target_link_libraries(${distributedkv_test_name} distributedkv gtest gmock_main)

    # Set test target properties and dependencies.
    set_target_properties(${distributedkv_test_name}
            PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
            COMMAND ${distributedkv_test_name}
            )
endforeach (distributedkv_test_source ${DISTRIBUTEDKV_TEST_SOURCES})