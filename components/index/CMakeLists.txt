project(index)

# HEADER-ONLY, and that is the shape the teardown left rather than a choice made for it.
# This target used to compile four translation units: index.cpp (the polymorphic base),
# index_engine.cpp (the per-table registry), and the two disk_*_single_field_index.cpp
# facades. All four are gone -- the rows and the search were always the storage agent's,
# the per-transaction buffer moved down to sit beside them, and the routing the registry
# did moved up into manager_index_t's one per-oid record map. What is left here is the
# binary key codec and the type aliases the planner and the index service share, both
# header-only.
#
# INTERFACE, not a STATIC library with no sources: an empty archive is a link error on
# some toolchains and a silent no-op on others, and neither is a thing to leave behind.
# Dependents keep linking otterbrix::index exactly as before -- CMake propagates an
# INTERFACE target's usage requirements through target_link_libraries unchanged.
add_library(otterbrix_${PROJECT_NAME} INTERFACE)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} INTERFACE
        otterbrix::cursor
        otterbrix::context
        otterbrix::logical_plan
        otterbrix::log
        dl
        Boost::boost
)

if (DEV_MODE)
    add_subdirectory(test)
endif ()
