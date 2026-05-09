project(test_catalog)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        # V4 migration: catalog state is owned by manager_disk_t (pg_catalog.* tables).
        # Read-side coverage lives in services/disk/tests (resolve / DDL) and
        # services/dispatcher/tests (catalog_view_t / validate). Per-component tests here
        # cover only the structural primitives.
        test_oids.cpp
        test_set_membership_filter.cpp
        test_system_schemas.cpp
        test_type_spec_roundtrip.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::catalog
        otterbrix::services
        otterbrix::table

        absl::int128
        actor-zeta::actor-zeta
        Boost::boost
        Catch2::Catch2
        msgpackc-cxx
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
