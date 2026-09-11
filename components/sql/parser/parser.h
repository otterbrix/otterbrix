#pragma once
#include "nodes/parsenodes.h"

namespace components::sql::parser {
    class parser_extension_registry_t;
} // namespace components::sql::parser

/* The three-argument form falls back to `extensions` (see extension.hpp) for syntax the
 * core grammar rejects. An empty result means no statement found, not failure — the list
 * is never null (NIL sentinel), so check list_length(tree), not `if (!tree)`. */
extern List* raw_parser(std::pmr::memory_resource* resource, const char* str);
extern List* raw_parser(std::pmr::memory_resource* resource,
                        const char* str,
                        const components::sql::parser::parser_extension_registry_t& extensions);

// Utility functions exported by gram.y.
extern List* SystemFuncName(std::pmr::memory_resource* resource, char* name);
extern TypeName* SystemTypeName(std::pmr::memory_resource* resource, char* name);