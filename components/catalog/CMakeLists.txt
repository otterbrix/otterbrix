project(catalog CXX)

set(SOURCE_${PROJECT_NAME}
        versioned_trie/trie_utils.cpp

        # M6c: transaction/* physically removed — DDL transactional semantics moved to
        # executor's ctx.txn. The 5 source pairs (transaction_scope/metadata_transaction/
        # schema_diff/metadata_diff/transaction_list) and tests/test_transactions.cpp
        # are gone.

        catalog_oids.cpp
        system_table_schemas.cpp
        table_id.cpp
        schema.cpp
        computed_schema.cpp
        table_metadata.cpp
        catalog_error.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
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