project(test_table)

set(${PROJECT_NAME}_SOURCES
        test_column.cpp
        test_table.cpp
        test_block_manager.cpp
        test_metadata.cpp
        test_checkpoint_load.cpp
        test_transaction_manager.cpp
        test_mvcc_operations.cpp
        test_statistics.cpp
        test_parallel_scan.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::expressions
    otterbrix::table
    otterbrix::file
    otterbrix::types
    otterbrix::vector
    otterbrix::session
    Catch2::Catch2
    magic_enum::magic_enum
    absl::crc32c
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
