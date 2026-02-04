project(test_cursor)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_cursor.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::log
        otterbrix::test_generaty
        otterbrix::cursor
        Boost::boost
        Catch2::Catch2
        actor-zeta::actor-zeta
        absl::int128
        msgpackc-cxx
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
