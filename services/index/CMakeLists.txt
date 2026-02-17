project(index_service)

set(${PROJECT_NAME}_HEADERS
        index_contract.hpp
        index_disk.hpp
        index_agent_disk.hpp
        manager_index.hpp
)

set(${PROJECT_NAME}_SOURCES
        index_disk.cpp
        index_agent_disk.cpp
        manager_index.cpp
)

add_library(otterbrix_${PROJECT_NAME}
    ${${PROJECT_NAME}_HEADERS}
    ${${PROJECT_NAME}_SOURCES}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
    otterbrix_${PROJECT_NAME} PUBLIC
    otterbrix::disk
    otterbrix::index
    otterbrix::file
    otterbrix::b_plus_tree
    otterbrix::serialization
    otterbrix::logical_plan
    otterbrix::locks
    otterbrix::log
    spdlog::spdlog
    Boost::boost
    actor-zeta::actor-zeta
)

target_include_directories(otterbrix_${PROJECT_NAME} PUBLIC
)

if (DEV_MODE)
    add_subdirectory(tests)
endif()
