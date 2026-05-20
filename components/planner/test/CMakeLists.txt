project(test_translator)

set(${PROJECT_NAME}_SOURCES
        test_logical_plan.cpp
        test_optimizer.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::expressions
        otterbrix::logical_plan
        otterbrix::test_generaty
        otterbrix::planner
        otterbrix::vector
        otterbrix::log

        absl::int128
        actor-zeta::actor-zeta
        Boost::boost
        Catch2::Catch2
        msgpackc-cxx
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})

add_executable(test_substrait
        main.cpp
        test_substrait_e2e.cpp
        test_substrait_roundtrip.cpp
)

target_link_libraries(
        test_substrait PRIVATE
        otterbrix::expressions
        otterbrix::logical_plan
        otterbrix::catalog
        otterbrix::vector

        absl::int128
        Boost::boost
        Catch2::Catch2
        msgpackc-cxx
)

catch_discover_tests(test_substrait)

add_executable(substrait_export_samples substrait_export_samples.cpp)

target_link_libraries(
        substrait_export_samples PRIVATE
        otterbrix::expressions
        otterbrix::logical_plan
        otterbrix::catalog
        otterbrix::vector

        absl::int128
        Boost::boost
        msgpackc-cxx
)


add_executable(substrait_import_external_samples substrait_import_external_samples.cpp)

target_link_libraries(
        substrait_import_external_samples PRIVATE
        otterbrix::expressions
        otterbrix::logical_plan
        otterbrix::catalog
        otterbrix::vector

        absl::int128
        Boost::boost
        msgpackc-cxx
)

set(OTTERBRIX_DATAFUSION_PYTHON "" CACHE FILEPATH "Python interpreter with Apache DataFusion installed")
if (OTTERBRIX_DATAFUSION_PYTHON)
    set(SUBSTRAIT_CONSUMER_PYTHON ${OTTERBRIX_DATAFUSION_PYTHON})
else ()
    find_program(SUBSTRAIT_CONSUMER_PYTHON NAMES python3 python)
endif ()

set(OTTERBRIX_SUBSTRAIT_VALIDATOR "" CACHE FILEPATH "substrait-validator executable")
if (OTTERBRIX_SUBSTRAIT_VALIDATOR)
    set(SUBSTRAIT_VALIDATOR_EXECUTABLE ${OTTERBRIX_SUBSTRAIT_VALIDATOR})
else ()
    find_program(SUBSTRAIT_VALIDATOR_EXECUTABLE NAMES substrait-validator)
endif ()

set(SUBSTRAIT_VALIDATOR_ARGS)
if (SUBSTRAIT_VALIDATOR_EXECUTABLE)
    list(APPEND SUBSTRAIT_VALIDATOR_ARGS --validator ${SUBSTRAIT_VALIDATOR_EXECUTABLE})
endif ()

if (SUBSTRAIT_CONSUMER_PYTHON)
    add_test(
            NAME components::planner::substrait::external_producer_datafusion
            COMMAND
            ${SUBSTRAIT_CONSUMER_PYTHON}
            ${CMAKE_CURRENT_SOURCE_DIR}/test_substrait_datafusion_producer.py
            --importer $<TARGET_FILE:substrait_import_external_samples>
            --work-dir ${CMAKE_CURRENT_BINARY_DIR}/external_producers/datafusion
            --samples-dir ${CMAKE_CURRENT_SOURCE_DIR}/substrait_test_samples
    )
    set_tests_properties(
            components::planner::substrait::external_producer_datafusion
            PROPERTIES
            DEPENDS substrait_import_external_samples
            SKIP_RETURN_CODE 77
    )

    add_test(
            NAME components::planner::substrait::external_consumer_datafusion
            COMMAND
            ${SUBSTRAIT_CONSUMER_PYTHON}
            ${CMAKE_CURRENT_SOURCE_DIR}/test_substrait_datafusion_consumer.py
            --exporter $<TARGET_FILE:substrait_export_samples>
            --work-dir ${CMAKE_CURRENT_BINARY_DIR}/external_consumers/datafusion
            --samples-dir ${CMAKE_CURRENT_SOURCE_DIR}/substrait_test_samples
    )
    set_tests_properties(
            components::planner::substrait::external_consumer_datafusion
            PROPERTIES
            DEPENDS substrait_export_samples
            SKIP_RETURN_CODE 77
    )

    add_test(
            NAME components::planner::substrait::external_consumer_datafusion_write_nodes
            COMMAND
            ${SUBSTRAIT_CONSUMER_PYTHON}
            ${CMAKE_CURRENT_SOURCE_DIR}/test_substrait_datafusion_write_consumer.py
            --exporter $<TARGET_FILE:substrait_export_samples>
            --work-dir ${CMAKE_CURRENT_BINARY_DIR}/external_consumers/datafusion_write_nodes
            --samples-dir ${CMAKE_CURRENT_SOURCE_DIR}/substrait_test_samples
    )
    set_tests_properties(
            components::planner::substrait::external_consumer_datafusion_write_nodes
            PROPERTIES
            DEPENDS substrait_export_samples
            SKIP_RETURN_CODE 77
    )

    add_test(
            NAME components::planner::substrait::external_consumer_acero
            COMMAND
            ${SUBSTRAIT_CONSUMER_PYTHON}
            ${CMAKE_CURRENT_SOURCE_DIR}/test_substrait_acero_consumer.py
            --exporter $<TARGET_FILE:substrait_export_samples>
            --work-dir ${CMAKE_CURRENT_BINARY_DIR}/external_consumers/acero
            --samples-dir ${CMAKE_CURRENT_SOURCE_DIR}/substrait_test_samples
    )
    set_tests_properties(
            components::planner::substrait::external_consumer_acero
            PROPERTIES
            DEPENDS substrait_export_samples
            SKIP_RETURN_CODE 77
    )

    add_test(
            NAME components::planner::substrait::external_producer_acero
            COMMAND
            ${SUBSTRAIT_CONSUMER_PYTHON}
            ${CMAKE_CURRENT_SOURCE_DIR}/test_substrait_acero_producer.py
            --work-dir ${CMAKE_CURRENT_BINARY_DIR}/external_producers/acero
            --samples-dir ${CMAKE_CURRENT_SOURCE_DIR}/substrait_test_samples
    )
    set_tests_properties(
            components::planner::substrait::external_producer_acero
            PROPERTIES
            SKIP_RETURN_CODE 77
    )

    add_test(
            NAME components::planner::substrait::external_consumer_validator
            COMMAND
            ${SUBSTRAIT_CONSUMER_PYTHON}
            ${CMAKE_CURRENT_SOURCE_DIR}/test_substrait_validator_consumer.py
            --exporter $<TARGET_FILE:substrait_export_samples>
            --work-dir ${CMAKE_CURRENT_BINARY_DIR}/external_consumers/substrait_validator
            --samples-dir ${CMAKE_CURRENT_SOURCE_DIR}/substrait_test_samples
            ${SUBSTRAIT_VALIDATOR_ARGS}
    )
    set_tests_properties(
            components::planner::substrait::external_consumer_validator
            PROPERTIES
            DEPENDS substrait_export_samples
            SKIP_RETURN_CODE 77
    )
endif ()
