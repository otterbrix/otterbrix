project(test_dispatcher)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        test_dispatcher_catalog.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        non_thread_scheduler
        otterbrix::dispatcher
        otterbrix::sql
        otterbrix::wal
        otterbrix::disk
        otterbrix::test_generaty

        Catch2::Catch2
        spdlog::spdlog
        actor-zeta::actor-zeta
        absl::int128
        magic_enum::magic_enum
        msgpackc-cxx
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
