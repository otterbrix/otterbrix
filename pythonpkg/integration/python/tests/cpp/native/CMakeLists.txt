project(test_native)

set(${PROJECT_NAME}_SOURCES
        test_hello.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES} ${ALL_OBJECT_FILES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        Catch2::Catch2
        pybind11::pybind11

        ${external_LIBS}
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
