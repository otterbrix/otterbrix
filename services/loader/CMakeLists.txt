project(otterbrix_loader)

set(${PROJECT_NAME}_HEADERS
    loaded_state.hpp
    loader.hpp
)

set(${PROJECT_NAME}_SOURCES
    loader.cpp
)

add_library(${PROJECT_NAME}
    ${${PROJECT_NAME}_HEADERS}
    ${${PROJECT_NAME}_SOURCES}
)

target_link_libraries(${PROJECT_NAME} PUBLIC
    otterbrix_document
    otterbrix_log
    otterbrix_logical_plan
    otterbrix_serialization
    otterbrix_disk
    otterbrix_wal
)
