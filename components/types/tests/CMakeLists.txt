project(test_types)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_types.cpp
        test_type_switch.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::log
        otterbrix::file
        otterbrix::types
        Catch2::Catch2
)

target_include_directories(${PROJECT_NAME} PUBLIC
    ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
