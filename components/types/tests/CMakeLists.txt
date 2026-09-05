project(test_types)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_types.cpp
        test_type_switch.cpp
        test_type_spec_codec.cpp
        test_tri_bool.cpp
        test_decimal_descale_refusal.cpp
        test_name_accessors_totality.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::log
        otterbrix::file
        otterbrix::types
        Catch2::Catch2WithMain
)

target_include_directories(${PROJECT_NAME} PUBLIC
    ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
