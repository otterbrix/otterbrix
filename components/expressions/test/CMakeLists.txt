project(test_expressions)

set(${PROJECT_NAME}_SOURCES
        test_compare_expression.cpp
        test_aggregate_expression.cpp
        test_scalar_expression.cpp
        test_sort_expression.cpp
        test_execution_graph_builder.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::expressions

        absl::int128
        actor-zeta::actor-zeta
        Boost::boost
        Catch2::Catch2WithMain
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
