project(vector_search)

set(${PROJECT_NAME}_SOURCES
        hnsw_index.cpp
)

add_library(otterbrix_${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})
add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
        ${CMAKE_SOURCE_DIR}/components
)

target_link_libraries(
        otterbrix_${PROJECT_NAME}
        PRIVATE hnswlib
)

if (DEV_MODE)
    add_subdirectory(test)
    add_subdirectory(benchmark)
endif ()
