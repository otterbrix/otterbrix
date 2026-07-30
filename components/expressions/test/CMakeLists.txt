project(test_expressions)

set(${PROJECT_NAME}_SOURCES
        test_bound_expression.cpp
        test_bound_executor.cpp
        test_bound_binder.cpp
        test_bound_function.cpp
        test_bound_coverage.cpp
        test_compare_expression.cpp
        test_aggregate_expression.cpp
        test_key.cpp
        test_param_storage.cpp
        test_scalar_expression.cpp
        test_sort_expression.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::expressions
        # The bound layer reads plan parameters live (logical_plan::get_parameter), exactly as
        # update_expression.cpp already does; the test binary needs that definition.
        otterbrix::logical_plan
        # A bound function call resolves its kernel through the registry at BIND time, so the
        # function tests build one of their own instead of borrowing the process-global default.
        otterbrix::compute

        absl::int128
        actor-zeta::actor-zeta
        Boost::boost
        Catch2::Catch2WithMain
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
