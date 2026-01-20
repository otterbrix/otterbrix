project(physical_plan)


set(${PROJECT_NAME}_SOURCES
        base/operators/operator.cpp
        base/operators/operator_data.cpp
        base/operators/operator_write_data.cpp
        base/operators/operator_empty.cpp
        base/operators/operator_raw_data.cpp
        base/operators/operator_add_index.cpp
        base/operators/operator_drop_index.cpp

        collection/operators/aggregate/operator_aggregate.cpp
        collection/operators/aggregate/operator_count.cpp
        collection/operators/aggregate/operator_min.cpp
        collection/operators/aggregate/operator_max.cpp
        collection/operators/aggregate/operator_sum.cpp
        collection/operators/aggregate/operator_avg.cpp

        collection/operators/get/operator_get.cpp
        collection/operators/get/simple_value.cpp

        collection/operators/merge/operator_merge.cpp
        collection/operators/merge/operator_and.cpp
        collection/operators/merge/operator_or.cpp
        collection/operators/merge/operator_not.cpp

        collection/operators/predicates/predicate.cpp
        collection/operators/predicates/simple_predicate.cpp

        collection/operators/scan/full_scan.cpp
        collection/operators/scan/index_scan.cpp
        collection/operators/scan/primary_key_scan.cpp
        collection/operators/scan/transfer_scan.cpp

        collection/operators/sort/sort.cpp

        collection/operators/aggregation.cpp
        collection/operators/operator_insert.cpp
        collection/operators/operator_delete.cpp
        collection/operators/operator_update.cpp
        collection/operators/operator_match.cpp
        collection/operators/operator_group.cpp
        collection/operators/operator_sort.cpp
        collection/operators/operator_join.cpp

        table/operators/aggregate/operator_aggregate.cpp
        table/operators/aggregate/operator_func.cpp

        table/operators/get/operator_get.cpp
        table/operators/get/simple_value.cpp

        # aren`t used for now
        #table/operators/merge/operator_merge.cpp
        #table/operators/merge/operator_and.cpp
        #table/operators/merge/operator_or.cpp
        #table/operators/merge/operator_not.cpp

        table/operators/predicates/predicate.cpp
        table/operators/predicates/simple_predicate.cpp

        table/operators/scan/full_scan.cpp
        table/operators/scan/index_scan.cpp
        table/operators/scan/primary_key_scan.cpp
        table/operators/scan/transfer_scan.cpp

        table/operators/sort/sort.cpp

        table/operators/aggregation.cpp
        table/operators/operator_insert.cpp
        table/operators/operator_delete.cpp
        table/operators/operator_update.cpp
        table/operators/operator_match.cpp
        table/operators/operator_group.cpp
        table/operators/operator_sort.cpp
        table/operators/operator_join.cpp

        table/operators/transformation.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_HEADERS}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::document
        otterbrix::types
        otterbrix::vector
        otterbrix::cursor
        otterbrix::session
        otterbrix::context
        otterbrix::index
        otterbrix::logical_plan
        spdlog::spdlog
        absl::flat_hash_map
        absl::node_hash_map
        absl::int128
        Boost::boost
)

target_include_directories(otterbrix_${PROJECT_NAME} PUBLIC
)

if (DEV_MODE)
    add_definitions(-DDEV_MODE)
    add_subdirectory(tests)
endif ()
