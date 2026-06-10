#pragma once
#include "nodes/parsenodes.h"

namespace components::sql::parser {
    class parser_extension_registry_t;
} // namespace components::sql::parser

/* Primary entry point for the raw parsing functions
 *
 * resource is a pointer to arena allocator and used for various objects,
 * lifetime of which exceeds parser scope.
 *
 * The two-argument form parses core SQL only. The three-argument form also
 * consults `extensions` for any syntax the core grammar rejects (see
 * extension.hpp); pass the registry owned by the database instance, or a
 * standalone one when using the parser as a component. */
extern List* raw_parser(std::pmr::memory_resource* resource, const char* str);
extern List* raw_parser(std::pmr::memory_resource* resource,
                        const char* str,
                        const components::sql::parser::parser_extension_registry_t& extensions);

/* Utility functions exported by gram.y (perhaps these should be elsewhere) */
extern List* SystemFuncName(std::pmr::memory_resource* resource, char* name);
extern TypeName* SystemTypeName(std::pmr::memory_resource* resource, char* name);