project(collection)

set(${PROJECT_NAME}_SOURCES
        create_index.cpp
        executor.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::table
        otterbrix::types
        otterbrix::cursor
        otterbrix::session
        otterbrix::context
        otterbrix::index
        otterbrix::logical_plan
        otterbrix::physical_plan_generator
        spdlog::spdlog
        absl::int128
        absl::flat_hash_map
        absl::node_hash_map
        Boost::boost
        actor-zeta::actor-zeta
)

target_include_directories(otterbrix_${PROJECT_NAME} PUBLIC
)
