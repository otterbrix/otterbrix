project(test_expressions)

set(${PROJECT_NAME}_SOURCES
        test_compare_expression.cpp
        test_aggregate_expression.cpp
        test_scalar_expression.cpp
        test_sort_expression.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::expressions
        Catch2::Catch2
        actor-zeta::actor-zeta
        magic_enum::magic_enum
        msgpackc-cxx
        absl::int128
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
