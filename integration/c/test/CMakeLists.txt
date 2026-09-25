project(test_c_otterbrix)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        main.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

# main.cpp includes only ../otterbrix.h: link libotterbrix alone, as the Rust and C# bindings do
target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        c_otterbrix
        Catch2::Catch2WithMain
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
