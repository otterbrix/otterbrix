project(sql CXX)

find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)

bison_target(
        Parser
        parser/gram.y
        ${CMAKE_CURRENT_BINARY_DIR}/parser/gram.cpp
        DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/parser/gram.hpp
)
flex_target(Lexer parser/scan.l ${CMAKE_CURRENT_BINARY_DIR}/parser/scan.cpp)
add_flex_bison_dependency(Lexer Parser)

set(SOURCE_${PROJECT_NAME}
    ${BISON_Parser_OUTPUTS}
    ${FLEX_Lexer_OUTPUTS}

    parser/pg_functions.cpp
    parser/pg_std_list.cpp
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
    transformer/impl/transform_checkpoint.cpp
    transformer/impl/transform_vacuum.cpp
    transformer/impl/transform_sequence.cpp
    transformer/impl/transform_view.cpp
    transformer/impl/transform_macro.cpp
)

include_directories(${CMAKE_SOURCE_DIR})

add_library(otterbrix_${PROJECT_NAME} ${SOURCE_${PROJECT_NAME}})
add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
    otterbrix_${PROJECT_NAME} PUBLIC
    otterbrix::expressions
    otterbrix::logical_plan
    magic_enum::magic_enum
    msgpackc-cxx
    absl::int128
)

target_include_directories(
    otterbrix_${PROJECT_NAME} PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}/parser
)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-all")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-all")

if (DEV_MODE)
    add_subdirectory(test)
endif ()
