project(test_physical_plan)

add_definitions(-DDEV_MODE)

# operator.cpp is compiled into this target directly instead of linking
# otterbrix::physical_plan: that OBJECT library carries every DDL/DML operator, and those
# reach into services::*, which would drag the whole engine into a unit test about which
# arena an error message lives on. operator_data / operator_write_data come along because
# operator_t holds intrusive pointers to them.
set(${PROJECT_NAME}_SOURCES
        test_operator_error_residency.cpp
        test_operator_raw_data_resource.cpp
        ../operators/operator.cpp
        ../operators/operator_data.cpp
        ../operators/operator_raw_data.cpp
        ../operators/operator_write_data.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::log
        otterbrix::types
        otterbrix::vector
        otterbrix::catalog
        otterbrix::context
        Boost::boost
        Catch2::Catch2WithMain
        actor-zeta::actor-zeta
        absl::int128
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
