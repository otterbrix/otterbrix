project(table CXX)

set(SOURCE_${PROJECT_NAME}
        column_definition.cpp
        column_data.cpp
        standard_column_data.cpp
        array_column_data.cpp
        list_column_data.cpp
        struct_column_data.cpp
        validity_column_data.cpp
        column_segment.cpp
        update_segment.cpp
        column_state.cpp
        row_group.cpp
        table_state.cpp
        collection.cpp
        data_table.cpp
        row_version_manager.cpp
        transaction.cpp
        transaction_manager.cpp
        base_statistics.cpp
        persistent_column_data.cpp
        column_checkpoint_state.cpp
        column_data_checkpointer.cpp

        storage/file_buffer.cpp
        storage/block_handle.cpp
        storage/block_manager.cpp
        storage/buffer_handle.cpp
        storage/buffer_manager.cpp
        storage/standard_buffer_manager.cpp
        storage/buffer_pool.cpp
        storage/single_file_block_manager.cpp
        storage/metadata_manager.cpp
        storage/metadata_writer.cpp
        storage/metadata_reader.cpp
        storage/data_pointer.cpp
        storage/partial_block_manager.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PRIVATE
        otterbrix::expressions
        otterbrix::file
        otterbrix::types
        otterbrix::vector
        magic_enum::magic_enum
        msgpackc-cxx
        absl::crc32c
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(test)
endif ()
