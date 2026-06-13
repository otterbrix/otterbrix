project(test_vector_search)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        test_distance_metrics.cpp
        test_top_k_and_knn.cpp
        test_recall.cpp
        test_hnsw_index.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::vector_search
        Catch2::Catch2
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
