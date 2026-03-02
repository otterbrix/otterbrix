project(test_wal)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_wal.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        non_thread_scheduler
        otterbrix::log
        otterbrix::logical_plan
        otterbrix::session
        otterbrix::wal
        otterbrix::test_generaty

        Catch2::Catch2
        absl::crc32c
        actor-zeta::actor-zeta
        absl::int128
        Boost::boost
        magic_enum::magic_enum
        msgpackc-cxx
)

target_include_directories(${PROJECT_NAME} PUBLIC
    ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
