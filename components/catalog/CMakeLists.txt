project(catalog CXX)

set(SOURCE_${PROJECT_NAME}
        versioned_trie/trie_utils.cpp

        transaction/schema_diff.cpp
        transaction/metadata_diff.cpp

        transaction/metadata_transaction.cpp
        transaction/transaction_scope.cpp
        transaction/transaction_list.cpp

        table_id.cpp
        schema.cpp
        computed_schema.cpp
        table_metadata.cpp
        namespace_storage.cpp
        catalog.cpp
        catalog_error.cpp
        catalog_types.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PRIVATE
        otterbrix::compute
        otterbrix::cursor
        otterbrix::vector
        otterbrix::types
        absl::int128
        Boost::boost
        msgpackc-cxx
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(tests)
endif ()