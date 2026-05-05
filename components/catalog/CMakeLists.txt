project(catalog CXX)

set(SOURCE_${PROJECT_NAME}
        builtin_seed.cpp
        cascade_planner.cpp
        catalog_oids.cpp
        check_predicate_compiler.cpp
        constraint_evaluator.cpp
        ddl_metadata_builder.cpp
        dependency_walker.cpp
        helpers.cpp
        pg_catalog_decoders.cpp
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
        otterbrix::compute
        otterbrix::cursor
        otterbrix::vector
        otterbrix::types
        absl::int128
        Boost::boost
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(tests)
endif ()