project(test_index)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        test_single_field_index.cpp
        test_create_index.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::log
        otterbrix::document
        otterbrix::test_generaty
        Boost::boost
        Catch2::Catch2
)

# Handle circular dependency: index <-> disk
# Use linker group on Linux to resolve mutual symbol references
if(UNIX AND NOT APPLE)
    target_link_libraries(${PROJECT_NAME} PRIVATE
        -Wl,--start-group
        otterbrix::index
        otterbrix::disk
        otterbrix::dispatcher
        otterbrix::wal
        -Wl,--end-group
    )
else()
    # macOS/other: link twice to resolve circular deps
    target_link_libraries(${PROJECT_NAME} PRIVATE
        otterbrix::index
        otterbrix::disk
        otterbrix::dispatcher
        otterbrix::wal
        otterbrix::dispatcher
        otterbrix::disk
    )
endif()

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
