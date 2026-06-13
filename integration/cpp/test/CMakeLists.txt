project(test_otterbrix)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_instances.cpp
        test_collection.cpp
        test_collection_logical_plan.cpp
        test_collection_sql.cpp
        test_index.cpp
        test_insert.cpp
        test_wal_pool.cpp
        test_persistence.cpp
        test_sql_features.cpp
        test_correctness_bugs.cpp
        test_production_scenarios.cpp
        test_join.cpp
        test_join_raw.cpp
        test_connections.cpp
        test_udfs.cpp
        test_batch_execution.cpp
        test_group_by_node_data.cpp
        test_group_operator_contracts.cpp
        test_arithmetic.cpp
        test_clean_break_startup.cpp
        test_column_projection.cpp
        test_computed_schema.cpp
        test_pushdown_filter.cpp
        test_subqueries.cpp
        test_hash_join.cpp
        test_returning.cpp
        test_parser_extension.cpp
        test_engine_lifecycle.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE

        cpp_otterbrix
        non_thread_scheduler

        otterbrix::demo_extension
        otterbrix::test_generaty
        otterbrix::log
        otterbrix::cursor
        otterbrix::session
        otterbrix::services
        otterbrix::planner
        otterbrix::locks

        spdlog::spdlog
        msgpackc-cxx
        #CONAN_PKG::date
        Catch2::Catch2
        absl::crc32c
        absl::int128
        benchmark::benchmark
        actor-zeta::actor-zeta
        Boost::boost
        fmt::fmt
        #CONAN_PKG::libcurl
        ZLIB::ZLIB
        BZip2::BZip2
        #libbacktrace::libbacktrace

)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME} PROPERTIES TIMEOUT 1800)

# Profiling binary for arithmetic
add_executable(profile_arithmetic profile_arithmetic.cpp)
target_link_libraries(profile_arithmetic PRIVATE
        cpp_otterbrix
        otterbrix::test_generaty
        otterbrix::log
        otterbrix::cursor
        otterbrix::session
        otterbrix::services
        otterbrix::locks
        spdlog::spdlog
        msgpackc-cxx
        Catch2::Catch2
        absl::crc32c
        absl::int128
        benchmark::benchmark
        actor-zeta::actor-zeta
        Boost::boost
        fmt::fmt
        ZLIB::ZLIB
        BZip2::BZip2
)
