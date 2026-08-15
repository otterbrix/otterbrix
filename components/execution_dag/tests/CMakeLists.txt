project(test_execution_dag)

set(${PROJECT_NAME}_SOURCES
        test_execution_dag.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::execution_dag

        absl::int128
        Boost::boost
        Catch2::Catch2WithMain
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})