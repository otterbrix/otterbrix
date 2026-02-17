project(test_index_service)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_index_disk.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::index_service
    otterbrix::test_generaty
    Boost::boost
    Catch2::Catch2
    actor-zeta::actor-zeta
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
