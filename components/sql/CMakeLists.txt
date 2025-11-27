project(sql CXX)

set(SOURCE_${PROJECT_NAME}
    parser/gram.cpp
    parser/pg_functions.cpp
    parser/pg_std_list.cpp
    parser/scan.cpp
    parser/scansup.cpp
    parser/keyword.cpp
    parser/parser.cpp
    parser/expr_location.cpp
    parser/guc.cpp

    parser/nodes/value.cpp
    parser/nodes/makefuncs.cpp
    parser/nodes/nodes.cpp
    parser/nodes/parsenodes.h

    transformer/utils.cpp
    transformer/transformer.cpp
    transformer/transform_result.cpp

    transformer/impl/transform_database.cpp
    transformer/impl/transform_table.cpp
    transformer/impl/transform_select.cpp
    transformer/impl/transform_table.cpp
    transformer/impl/transform_types.cpp
    transformer/impl/transfrom_common.cpp
    transformer/impl/transform_update.cpp
    transformer/impl/transform_insert.cpp
    transformer/impl/transform_delete.cpp
    transformer/impl/transform_index.cpp
)

include_directories(${CMAKE_SOURCE_DIR})

add_library(otterbrix_${PROJECT_NAME} ${SOURCE_${PROJECT_NAME}})
add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
    otterbrix_${PROJECT_NAME} PUBLIC
    otterbrix::document
    otterbrix::expressions
    otterbrix::logical_plan
    magic_enum::magic_enum
    msgpackc-cxx
    absl::int128
)

target_include_directories(
    otterbrix_${PROJECT_NAME}
    PUBLIC
)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-all")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-all")

if (DEV_MODE)
    add_subdirectory(test)
endif ()
