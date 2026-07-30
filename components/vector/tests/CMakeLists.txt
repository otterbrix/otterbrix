project(test_vector)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_vector.cpp
        test_arrow_conversion.cpp
        test_validity_mask.cpp
        test_arithmetic_empty.cpp
        test_arithmetic_result_type.cpp
        test_vector_error_channel.cpp
        test_data_chunk_schema.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::types
        otterbrix::vector
        Catch2::Catch2WithMain
)

target_include_directories(${PROJECT_NAME} PUBLIC
    ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
