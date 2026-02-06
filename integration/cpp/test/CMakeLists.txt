project(test_otterbrix)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_instances.cpp
        test_collection.cpp
        test_collection_logical_plan.cpp
        test_collection_sql.cpp
        test_save_load.cpp
        test_index.cpp
        test_disk_index.cpp
        test_join.cpp
        test_connections.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE

        cpp_otterbrix

        otterbrix::test_generaty
        otterbrix::log
        otterbrix::collection
        otterbrix::cursor
        otterbrix::session
        otterbrix::document
        otterbrix::dispatcher
        otterbrix::wal
        otterbrix::disk
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
catch_discover_tests(${PROJECT_NAME} PROPERTIES TIMEOUT 900)

# Note: set_tests_properties must be called after tests are discovered
# For catch_discover_tests, use TEST_SPEC to filter and set properties at runtime
# set_tests_properties(
#     "integration::cpp::test_disk_index::concurrent_queries"
#     "integration::cpp::test_disk_index::concurrent_large_dataset"
#     PROPERTIES TIMEOUT 1800
# )
