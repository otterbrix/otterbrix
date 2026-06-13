project(test_disk)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_table_storage.cpp
    test_torn_checkpoint.cpp
    test_system_table_bootstrap.cpp
    test_resolve.cpp
    test_pg_depend.cpp
    test_fk_composite.cpp
    test_ddl_methods.cpp
    test_wal_catalog.cpp
    test_mvcc_ddl.cpp
    test_error_handling.cpp
    test_recovery.cpp
    test_d4_lazy_load.cpp
    test_persistence.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::services
    otterbrix::test_generaty
    non_thread_scheduler

    absl::int128
    actor-zeta::actor-zeta
    Boost::boost
    Catch2::Catch2
    msgpackc-cxx
    spdlog::spdlog
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
