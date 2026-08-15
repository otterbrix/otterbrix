project(benchmark_casts)

add_executable(${PROJECT_NAME} benchmark_registry.cpp)

target_include_directories(${PROJECT_NAME} PRIVATE ${CMAKE_SOURCE_DIR})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::casts
        benchmark::benchmark
        Boost::boost
)
