project(test_dispatcher)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        test_dispatcher_catalog.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        non_thread_scheduler
        otterbrix::sql
        otterbrix::services
        otterbrix::test_generaty

        absl::int128
        actor-zeta::actor-zeta
        Catch2::Catch2
        spdlog::spdlog
        msgpackc-cxx
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
