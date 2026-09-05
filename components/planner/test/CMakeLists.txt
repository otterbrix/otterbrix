project(test_translator)

set(${PROJECT_NAME}_SOURCES
        test_logical_plan.cpp
        test_optimizer.cpp
        test_promote_multiway.cpp
        test_promote_star.cpp
        test_pushed_spec_build.cpp
        test_create_plan_pushdown.cpp
        test_create_plan_sequence.cpp
        test_view_expansion.cpp
        test_ddl_oid_batch_refusal.cpp
        test_ddl_unresolved_refusal.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::expressions
        otterbrix::logical_plan
        otterbrix::test_generaty
        otterbrix::planner
        otterbrix::vector
        otterbrix::services

        absl::int128
        actor-zeta::actor-zeta
        Boost::boost
        Catch2::Catch2WithMain
        spdlog::spdlog
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
