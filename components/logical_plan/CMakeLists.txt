project(logical_plan CXX)

set(SOURCE_${PROJECT_NAME}
        node.cpp
        node_aggregate.cpp
        node_create_collection.cpp
        node_create_database.cpp
        node_create_index.cpp
        node_create_type.cpp
        node_data.cpp
        node_delete.cpp
        node_drop_collection.cpp
        node_drop_database.cpp
        node_drop_index.cpp
        node_drop_type.cpp
        node_function.cpp
        node_group.cpp
        node_insert.cpp
        node_join.cpp
        node_limit.cpp
        node_match.cpp
        node_sort.cpp
        node_update.cpp
        param_storage.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PRIVATE
        otterbrix::document
        otterbrix::types
        otterbrix::expressions
        otterbrix::serialization
        magic_enum::magic_enum
        msgpackc-cxx
        absl::int128
        Boost::boost
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
)
