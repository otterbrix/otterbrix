project(physical_plan_generator)

set(${PROJECT_NAME}_SOURCES
        create_plan.cpp
        impl/create_plan_aggregate.cpp
        impl/create_plan_data.cpp
        impl/create_plan_delete.cpp
        impl/create_plan_group.cpp
        impl/create_plan_insert.cpp
        impl/create_plan_match.cpp
        impl/create_plan_sort.cpp
        impl/create_plan_update.cpp
        impl/create_plan_join.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_HEADERS}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::cursor
        otterbrix::session
        otterbrix::index
        otterbrix::logical_plan
        otterbrix::planner
        otterbrix::physical_plan
        spdlog::spdlog
        absl::int128
        absl::flat_hash_map
        absl::node_hash_map
        Boost::boost
        actor-zeta::actor-zeta
)

target_include_directories(otterbrix_${PROJECT_NAME} PUBLIC
)

if (DEV_MODE)
    add_definitions(-DDEV_MODE)
    add_subdirectory(tests)
endif ()
