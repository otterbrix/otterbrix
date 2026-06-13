project(catalog CXX)

set(SOURCE_${PROJECT_NAME}
        cascade_planner.cpp
        catalog_oids.cpp
        ddl_metadata_builder.cpp
        alter_column_validators.cpp
        dependency_walker.cpp
        helpers.cpp
        system_table_schemas.cpp
        table_id.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::date
        otterbrix::compute
        otterbrix::cursor
        otterbrix::vector
        otterbrix::types
        otterbrix::table
        absl::int128
        Boost::boost
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
        ${CMAKE_SOURCE_DIR}
)

if (DEV_MODE)
    add_subdirectory(tests)
endif ()