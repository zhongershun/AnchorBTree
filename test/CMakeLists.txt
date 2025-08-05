# CMAKE_MINIMUM_REQUIRED(VERSION 2.8.12)

include(GoogleTest)

file(GLOB_RECURSE DASET_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*/*test.cpp")

# #########################################
# "make check-tests"
# #########################################
add_custom_target(build-tests COMMAND ${CMAKE_CTEST_COMMAND} --show-only)
add_custom_target(check-tests COMMAND ${CMAKE_CTEST_COMMAND} --verbose)
add_custom_target(check-public-ci-tests COMMAND ${CMAKE_CTEST_COMMAND} --verbose)


# #########################################
# "make XYZ_test"
# #########################################
foreach (daset_test_source ${DASET_TEST_SOURCES})
    # Create a human readable name.
    get_filename_component(daset_test_filename ${daset_test_source} NAME)
    string(REPLACE ".cpp" "" daset_test_name ${daset_test_filename})

    # Add the test target separately and as part of "make check-tests".
    add_executable(${daset_test_name} EXCLUDE_FROM_ALL ${daset_test_source})
#     add_backward(${daset_test_name})
    add_dependencies(build-tests ${daset_test_name})
    add_dependencies(check-tests ${daset_test_name})

    gtest_discover_tests(${daset_test_name}
            EXTRA_ARGS
            --gtest_output=xml:${CMAKE_BINARY_DIR}/test/${daset_test_name}.xml
            --gtest_catch_exceptions=0
            DISCOVERY_TIMEOUT 120
            PROPERTIES
            TIMEOUT 120
            )

    target_link_libraries(${daset_test_name} daset gtest gmock_main)

    # Set test target properties and dependencies.
    set_target_properties(${daset_test_name}
            PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
            COMMAND ${daset_test_name}
            )
endforeach ()

# add_dependencies(build-tests)
# add_dependencies(check-tests)