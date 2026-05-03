project(logical_plan CXX)

find_package(protobuf CONFIG REQUIRED)

if (TARGET protobuf::protoc)
        set(SUBSTRAIT_PROTOC_EXECUTABLE $<TARGET_FILE:protobuf::protoc>)
else ()
        find_program(SUBSTRAIT_PROTOC_EXECUTABLE protoc REQUIRED)
endif ()

set(SUBSTRAIT_PROTO_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/substrait_adapter/proto)
set(SUBSTRAIT_PROTO_GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
set(SUBSTRAIT_PROTO_FILES
        ${SUBSTRAIT_PROTO_ROOT}/substrait/algebra.proto
        ${SUBSTRAIT_PROTO_ROOT}/substrait/extended_expression.proto
        ${SUBSTRAIT_PROTO_ROOT}/substrait/extensions/extensions.proto
        ${SUBSTRAIT_PROTO_ROOT}/substrait/plan.proto
        ${SUBSTRAIT_PROTO_ROOT}/substrait/type.proto
)

set(SUBSTRAIT_GENERATED_SOURCES)
set(SUBSTRAIT_GENERATED_HEADERS)
foreach (SUBSTRAIT_PROTO_FILE ${SUBSTRAIT_PROTO_FILES})
        file(RELATIVE_PATH SUBSTRAIT_PROTO_RELATIVE ${SUBSTRAIT_PROTO_ROOT} ${SUBSTRAIT_PROTO_FILE})
        get_filename_component(SUBSTRAIT_PROTO_DIR ${SUBSTRAIT_PROTO_RELATIVE} DIRECTORY)
        get_filename_component(SUBSTRAIT_PROTO_NAME ${SUBSTRAIT_PROTO_RELATIVE} NAME_WE)
        set(SUBSTRAIT_PROTO_OUT_DIR ${SUBSTRAIT_PROTO_GENERATED_DIR}/${SUBSTRAIT_PROTO_DIR})
        set(SUBSTRAIT_PROTO_CC ${SUBSTRAIT_PROTO_OUT_DIR}/${SUBSTRAIT_PROTO_NAME}.pb.cc)
        set(SUBSTRAIT_PROTO_H ${SUBSTRAIT_PROTO_OUT_DIR}/${SUBSTRAIT_PROTO_NAME}.pb.h)
        add_custom_command(
                OUTPUT ${SUBSTRAIT_PROTO_CC} ${SUBSTRAIT_PROTO_H}
                COMMAND ${CMAKE_COMMAND} -E make_directory ${SUBSTRAIT_PROTO_OUT_DIR}
                COMMAND ${SUBSTRAIT_PROTOC_EXECUTABLE}
                        --proto_path=${SUBSTRAIT_PROTO_ROOT}
                        --cpp_out=${SUBSTRAIT_PROTO_GENERATED_DIR}
                        ${SUBSTRAIT_PROTO_FILE}
                DEPENDS ${SUBSTRAIT_PROTO_FILE} ${SUBSTRAIT_PROTO_FILES}
                COMMENT "Generating C++ protobuf bindings for ${SUBSTRAIT_PROTO_RELATIVE}"
                VERBATIM
        )
        list(APPEND SUBSTRAIT_GENERATED_SOURCES ${SUBSTRAIT_PROTO_CC})
        list(APPEND SUBSTRAIT_GENERATED_HEADERS ${SUBSTRAIT_PROTO_H})
endforeach ()

if (MSVC)
        set_source_files_properties(${SUBSTRAIT_GENERATED_SOURCES} PROPERTIES COMPILE_OPTIONS /w)
else ()
        set_source_files_properties(${SUBSTRAIT_GENERATED_SOURCES} PROPERTIES COMPILE_OPTIONS -w)
endif ()

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
        ${SUBSTRAIT_GENERATED_SOURCES}
        ${SUBSTRAIT_GENERATED_HEADERS}
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
        ${SUBSTRAIT_PROTO_GENERATED_DIR}
)
