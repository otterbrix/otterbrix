project(test_disk)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_disk_database.cpp
    test_disk_collection.cpp
    test_metadata.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::disk
    otterbrix::test_generaty
    Boost::boost
    Catch2::Catch2
    actor-zeta::actor-zeta
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
