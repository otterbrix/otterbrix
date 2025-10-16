project(test_vector)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_vector.cpp
        test_arrow_conversion.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::types
        otterbrix::vector
        Catch2::Catch2
)

target_include_directories(${PROJECT_NAME} PUBLIC
    ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
