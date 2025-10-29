#pragma once
#include "nodes/parsenodes.h"

/* Primary entry point for the raw parsing functions
 *
 * resource is a pointer to arena allocator and used for various objects,
 * lifetime of which exceeds parser scope */
extern List* raw_parser(std::pmr::memory_resource* resource, const char* str);

/* Utility functions exported by gram.y (perhaps these should be elsewhere) */
extern List* SystemFuncName(std::pmr::memory_resource* resource, char* name);
extern TypeName* SystemTypeName(std::pmr::memory_resource* resource, char* name);