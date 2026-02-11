project(test_serialization)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        test_serializer.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::serialization
        otterbrix::logical_plan
        otterbrix::types
        otterbrix::test_generaty
        otterbrix::expressions
        magic_enum::magic_enum
        msgpackc-cxx
        absl::int128
        Boost::boost
        Catch2::Catch2
)

target_include_directories(${PROJECT_NAME} PUBLIC
        ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
