project(test_index_service)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_index_disk.cpp
    test_bitcask_index_disk.cpp
    test_disk_hash_table.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::services
    otterbrix::index
    otterbrix::test_generaty

    absl::int128
    actor-zeta::actor-zeta
    Boost::boost
    Catch2::Catch2
    actor-zeta::actor-zeta
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
