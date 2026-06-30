project(test_unified_format)

set(${PROJECT_NAME}_SOURCES
        test_unified_format_roundtrip.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::table
        otterbrix::vector
        otterbrix::types

        absl::crc32c
        Catch2::Catch2
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
