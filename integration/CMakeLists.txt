include_directories(${CMAKE_SOURCE_DIR})

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")

endif()

add_subdirectory(cpp)

# libotterbrix.a: the whole engine in one archive, built from the objects of the component archives
# (tests and benchmarks keep linking the components).
set(otterbrix_engine_targets
        otterbrix_assert
        otterbrix_b_plus_tree
        otterbrix_date
        otterbrix_file
        otterbrix_regex
        otterbrix_locks
        otterbrix_string_buffer
        otterbrix_casts
        otterbrix_catalog
        otterbrix_compute
        otterbrix_context
        otterbrix_cursor
        otterbrix_execution_dag
        otterbrix_expressions
        otterbrix_log
        otterbrix_logical_plan
        otterbrix_operators
        otterbrix_physical_plan
        otterbrix_physical_plan_generator
        otterbrix_planner
        otterbrix_session
        otterbrix_sql
        otterbrix_table
        otterbrix_types
        otterbrix_vector
        otterbrix_services
        cpp_otterbrix
)

add_library(otterbrix STATIC)
foreach(engine_target IN LISTS otterbrix_engine_targets)
    target_sources(otterbrix PRIVATE $<TARGET_OBJECTS:${engine_target}>)
endforeach()
add_library(otterbrix::otterbrix ALIAS otterbrix)

target_link_libraries(
        otterbrix PUBLIC
        absl::crc32c
        absl::int128
        absl::flat_hash_map
        absl::node_hash_map
        absl::strings
        re2::re2
        actor-zeta::actor-zeta
        Boost::boost
        spdlog::spdlog
        fmt::fmt
        ${CMAKE_DL_LIBS}
)

add_subdirectory(c)
if(BUILD_PYTHON)
    add_subdirectory(python)
endif()

# The Python wheel is assembled by its own install rules (integration/python), so these stay out of it.
if(NOT SKBUILD)
    install(TARGETS otterbrix c_otterbrix
            ARCHIVE DESTINATION lib
            LIBRARY DESTINATION lib
            RUNTIME DESTINATION bin)
    install(DIRECTORY ${PROJECT_SOURCE_DIR}/core ${PROJECT_SOURCE_DIR}/components ${PROJECT_SOURCE_DIR}/services
            DESTINATION include
            FILES_MATCHING PATTERN "*.hpp" PATTERN "*.h" PATTERN "*.ipp"
            PATTERN "test" EXCLUDE
            PATTERN "tests" EXCLUDE)
    install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/cpp ${CMAKE_CURRENT_SOURCE_DIR}/c
            DESTINATION include/integration
            FILES_MATCHING PATTERN "*.hpp" PATTERN "*.h"
            PATTERN "test" EXCLUDE)
    install(FILES ${PROJECT_BINARY_DIR}/components/sql/parser/gram.hpp
            DESTINATION include/components/sql/parser)
endif()
