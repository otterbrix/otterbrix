project(test_index)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        test_single_field_index.cpp
        test_create_index.cpp
        test_index_mvcc.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::log
        otterbrix::services
        otterbrix::test_generaty

        actor-zeta::actor-zeta
        absl::int128
        Boost::boost
        Catch2::Catch2
        magic_enum::magic_enum
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
