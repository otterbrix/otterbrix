# otterbrix_add_parser_extension(<lib_name>
#     PREFIX  <grammar_prefix>     # optional, defaults to <lib_name>
#     SOURCES <cpp files...>)
#
# Builds a parser extension (its own flex+bison grammar) as a static library
# named otterbrix_<lib_name>, aliased otterbrix::<lib_name>, linked against
# otterbrix::sql. It hides the per-extension bison/flex wiring.
#
# Layout expected in calling directory:
#   <prefix>_gram.y   — bison grammar, %name-prefix="<prefix>_yy"
#   <prefix>_scan.l   — flex scanner,  %option prefix="<prefix>_yy"
#   plus the SOURCES (the parse driver + transform stage, plain C++)
#
function(otterbrix_add_parser_extension lib_name)
    cmake_parse_arguments(EXT "" "PREFIX" "SOURCES" ${ARGN})
    if (NOT EXT_PREFIX)
        set(EXT_PREFIX ${lib_name})
    endif ()

    find_package(BISON REQUIRED)
    find_package(FLEX REQUIRED)

    bison_target(${EXT_PREFIX}_parser
            ${CMAKE_CURRENT_SOURCE_DIR}/${EXT_PREFIX}_gram.y
            ${CMAKE_CURRENT_BINARY_DIR}/${EXT_PREFIX}_gram.cpp
            DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/${EXT_PREFIX}_gram.hpp)
    flex_target(${EXT_PREFIX}_lexer
            ${CMAKE_CURRENT_SOURCE_DIR}/${EXT_PREFIX}_scan.l
            ${CMAKE_CURRENT_BINARY_DIR}/${EXT_PREFIX}_scan.cpp
            DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/${EXT_PREFIX}_scan.h)
    add_flex_bison_dependency(${EXT_PREFIX}_lexer ${EXT_PREFIX}_parser)

    add_library(otterbrix_${lib_name} STATIC
            ${BISON_${EXT_PREFIX}_parser_OUTPUTS}
            ${FLEX_${EXT_PREFIX}_lexer_OUTPUTS}
            ${EXT_SOURCES})
    add_library(otterbrix::${lib_name} ALIAS otterbrix_${lib_name})

    target_include_directories(otterbrix_${lib_name} PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}   # <lib_name>.hpp, <prefix>_ast.hpp
            ${CMAKE_CURRENT_BINARY_DIR})  # generated <prefix>_gram.hpp
    target_link_libraries(otterbrix_${lib_name} PUBLIC otterbrix::sql)
endfunction()
