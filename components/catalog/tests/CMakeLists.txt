project(test_catalog)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        utils.cpp
        test_catalog.cpp
        test_transactions.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::catalog
        otterbrix::table

        absl::int128
        Boost::boost
        Catch2::Catch2
        msgpackc-cxx
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
