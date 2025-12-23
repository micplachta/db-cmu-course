add_executable(db_tests)

add_subdirectory(buffer)

target_link_libraries(db_tests PRIVATE
    db_core
    gtest
    gtest_main
)

include(GoogleTest)
gtest_discover_tests(db_tests)
