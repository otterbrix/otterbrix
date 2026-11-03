project(test_c_otterbrix)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        main.cpp
        error_code_pins.cpp
)

# otterbrix.h is a C header: this C11 unit stops the build when it is not. The engine's C++-only warning
# flags are replaced, not inherited.
add_library(test_c_header_is_c OBJECT header_is_c.c)
set_target_properties(test_c_header_is_c PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED YES C_EXTENSIONS OFF)
set_property(TARGET test_c_header_is_c PROPERTY COMPILE_OPTIONS -Werror -Wall -Wextra -Wpedantic)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES} $<TARGET_OBJECTS:test_c_header_is_c>)
set(otterbrix_h ${CMAKE_CURRENT_SOURCE_DIR}/../otterbrix.h)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${otterbrix_h})
file(READ ${otterbrix_h} otterbrix_h_text)
file(CONFIGURE OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/otterbrix_h_text.inc
     CONTENT "R\"otterbrix_h(@otterbrix_h_text@)otterbrix_h\"\n" @ONLY)
target_include_directories(${PROJECT_NAME} PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        c_otterbrix
        Catch2::Catch2WithMain
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
