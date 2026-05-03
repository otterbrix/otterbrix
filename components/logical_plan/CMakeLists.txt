project(logical_plan CXX)

find_package(protobuf CONFIG REQUIRED)

set(SOURCE_${PROJECT_NAME}
        forward.cpp
        node.cpp
        node_aggregate.cpp
        node_checkpoint.cpp
        node_create_collection.cpp
        node_create_database.cpp
        node_create_index.cpp
        node_create_macro.cpp
        node_create_sequence.cpp
        node_create_type.cpp
        node_create_view.cpp
        node_data.cpp
        node_delete.cpp
        node_drop_collection.cpp
        node_drop_database.cpp
        node_drop_index.cpp
        node_drop_macro.cpp
        node_drop_sequence.cpp
        node_drop_type.cpp
        node_drop_view.cpp
        node_function.cpp
        node_group.cpp
        node_having.cpp
        node_insert.cpp
        node_join.cpp
        node_limit.cpp
        node_match.cpp
        node_select.cpp
        node_sort.cpp
        node_update.cpp
        node_vacuum.cpp
        param_storage.cpp
        substrait_adapter/to_substrait.cpp
        substrait_adapter/from_substrait.cpp
        substrait_adapter/substrait_adapter_types.cpp
        substrait_adapter/substrait_extension_contracts.cpp
        substrait_adapter/substrait_function_mapping.cpp
        substrait_adapter/substrait_type_mapping.cpp
        substrait_adapter/substrait_expression_importer.cpp
        substrait_adapter/substrait_expression_exporter.cpp
        substrait_adapter/substrait/algebra.pb.cc
        substrait_adapter/substrait/capabilities.pb.cc
        substrait_adapter/substrait/extended_expression.pb.cc
        substrait_adapter/substrait/extensions/extensions.pb.cc
        substrait_adapter/substrait/function.pb.cc
        substrait_adapter/substrait/parameterized_types.pb.cc
        substrait_adapter/substrait/plan.pb.cc
        substrait_adapter/substrait/type.pb.cc
        substrait_adapter/substrait/type_expressions.pb.cc
)

add_library(otterbrix_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::types
        otterbrix::expressions
        otterbrix::catalog
        otterbrix::serialization
        otterbrix::table

        absl::int128
        Boost::boost
        msgpackc-cxx
        protobuf::libprotobuf
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/substrait_adapter
)
