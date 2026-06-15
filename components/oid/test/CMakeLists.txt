project(test_oid)

set( ${PROJECT_NAME}_SOURCES
        test_oid.cpp
)

add_executable(${PROJECT_NAME} main.cpp  ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        Catch2::Catch2
)

include(CTest)
include(Catch)
otterbrix_discover_tests(${PROJECT_NAME})
