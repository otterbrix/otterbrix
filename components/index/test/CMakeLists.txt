project(test_index)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        test_hash_single_field_index.cpp
        test_create_index.cpp
        test_index_mvcc.cpp
        test_logical_value_binary_codec.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::log
        otterbrix::services
        otterbrix::test_generaty

        absl::int128
        actor-zeta::actor-zeta
        Boost::boost
        Catch2::Catch2WithMain
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
