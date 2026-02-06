project(disk)

set(${PROJECT_NAME}_HEADERS
        command.hpp
        disk.hpp
        agent_disk.hpp
        index_disk.hpp
        index_agent_disk.hpp
        manager_disk.hpp
        metadata.hpp
        result.hpp
)

set(${PROJECT_NAME}_SOURCES
        command.cpp
        disk.cpp
        agent_disk.cpp
        index_disk.cpp
        index_agent_disk.cpp
        manager_disk.cpp
        metadata.cpp
        result.cpp
)

add_library(otterbrix_${PROJECT_NAME}
    ${${PROJECT_NAME}_HEADERS}
    ${${PROJECT_NAME}_SOURCES}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
    otterbrix_${PROJECT_NAME} PUBLIC
    otterbrix::file
    otterbrix::document
    otterbrix::collection
    otterbrix::b_plus_tree
    otterbrix::locks
    spdlog::spdlog
    Boost::boost
    actor-zeta::actor-zeta
)

target_include_directories(otterbrix_${PROJECT_NAME} PUBLIC
)

if (DEV_MODE)
    add_subdirectory(tests)
endif()
