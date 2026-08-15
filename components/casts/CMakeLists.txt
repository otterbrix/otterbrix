project(casts)

set(${PROJECT_NAME}_SOURCES
        cast_type_identity.cpp
        cast_registry.cpp
        composite_cast.cpp
        default_casts.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_SOURCES}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::execution_context
        otterbrix::types
        otterbrix::vector
        otterbrix::date
        Boost::boost
        absl::strings
)

if (DEV_MODE)
    add_subdirectory(tests)
endif ()

if (ALLOW_BENCHMARK)
    add_subdirectory(benchmark)
endif ()