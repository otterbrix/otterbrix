project(services)

set( ${PROJECT_NAME}_HEADERS
    # wal
    wal/base.hpp
)

set(${PROJECT_NAME}_SOURCES
    # collection
        collection/create_index.cpp
        collection/executor.cpp

    # disk
        disk/agent_disk.cpp
        disk/invalidation_ring_buffer.cpp
        disk/manager_disk.cpp
        disk/manager_disk_io.cpp
        disk/manager_disk_bootstrap.cpp
        disk/manager_disk_ddl.cpp
        disk/manager_disk_resolve.cpp
        disk/manager_disk_storage.cpp

    # dispatcher
       dispatcher/catalog_view.cpp
       dispatcher/dispatcher.cpp
       dispatcher/enrich_logical_plan.cpp
       dispatcher/validate_logical_plan.cpp
       dispatcher/versioned_plan_cache.cpp

    # index
        index/index_disk.cpp
        index/index_agent_disk.cpp
        index/manager_index.cpp

    # wal
        wal/wal_binary.cpp
        wal/wal_page.cpp
        wal/wal_page_writer.cpp
        wal/wal_page_reader.cpp
        wal/manager_wal_replicate.cpp
        wal/wal.cpp
        wal/wal_reader.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_HEADERS}
        ${${PROJECT_NAME}_SOURCES}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PRIVATE
        otterbrix::file
        otterbrix::table
        otterbrix::types
        otterbrix::context
        otterbrix::physical_plan_generator
        otterbrix::storage
        otterbrix::catalog
        otterbrix::cursor
        otterbrix::session
        otterbrix::planner
        otterbrix::sql
        otterbrix::index
        otterbrix::b_plus_tree
        otterbrix::log
        otterbrix::logical_plan
        otterbrix::locks

        absl::crc32c
        absl::int128
        absl::flat_hash_map
        absl::node_hash_map
        actor-zeta::actor-zeta
        Boost::boost
        msgpackc-cxx
        spdlog::spdlog
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
        ${CMAKE_SOURCE_DIR}
)

if (DEV_MODE)
    add_definitions(-DDEV_MODE)
    add_subdirectory(disk/tests)
    add_subdirectory(dispatcher/tests)
    add_subdirectory(index/tests)
    add_subdirectory(wal/tests)
endif()
