project(test_dispatcher)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        test_dispatcher_catalog.cpp
        test_variant_e3_differential.cpp
        test_resolve_function.cpp
        test_stamp_scalar_types.cpp
        test_dispatcher_admin_errors.cpp
        test_oid_alloc_operator_refusal.cpp
        test_wave_exec_dispatcher.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        non_thread_scheduler
        otterbrix::sql
        otterbrix::services
        otterbrix::test_generaty

        absl::int128
        actor-zeta::actor-zeta
        Catch2::Catch2WithMain
        spdlog::spdlog
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
