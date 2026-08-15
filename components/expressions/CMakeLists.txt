project(expressions CXX)

set(SOURCE_${PROJECT_NAME}
        forward.cpp
        expression.cpp
        clone_expression.cpp
        compare_expression.cpp
        aggregate_expression.cpp
        scalar_expression.cpp
        sort_expression.cpp
        function_expression.cpp
        cast_expression.cpp
        execution_dag_builder.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::casts
        otterbrix::compute
        otterbrix::execution_context
        otterbrix::execution_dag
        otterbrix::operators
        otterbrix::vector

        absl::int128
        Boost::boost
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(test)
endif ()
