project(physical_plan)


set(${PROJECT_NAME}_SOURCES
        operators/operator.cpp
        operators/operator_data.cpp
        operators/operator_write_data.cpp
        operators/operator_empty.cpp
        operators/operator_raw_data.cpp

        operators/aggregate/operator_aggregate.cpp
        operators/aggregate/operator_func.cpp

        operators/get/operator_get.cpp
        operators/get/simple_value.cpp

        operators/predicates/predicate.cpp
        operators/predicates/simple_predicate.cpp
        operators/predicates/function_predicate.cpp
        operators/predicates/utils.cpp

        operators/scan/full_scan.cpp
        operators/scan/index_scan.cpp
        operators/scan/primary_key_scan.cpp
        operators/scan/transfer_scan.cpp

        operators/sort/sort.cpp

        operators/aggregation.cpp
        operators/operator_insert.cpp
        operators/operator_delete.cpp
        operators/operator_update.cpp
        operators/operator_match.cpp
        operators/operator_group.cpp
        operators/operator_sort.cpp
        operators/operator_join.cpp

        operators/arithmetic_eval.cpp
        operators/transformation.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_HEADERS}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
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

