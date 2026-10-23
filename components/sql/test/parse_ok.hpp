#pragma once

#include <catch2/catch_test_macros.hpp>
#include <components/sql/parser/extension.hpp>
#include <components/sql/parser/parser.h>

// raw_parser for SQL the test expects to parse: a parse error fails the test with the parser's message.
inline List* parse_ok(std::pmr::memory_resource* resource, const char* sql) {
    auto parsed = raw_parser(resource, sql);
    if (parsed.has_error()) {
        FAIL("raw_parser refused \"" << sql << "\": " << parsed.error().what.c_str());
    }
    return parsed.value();
}

inline List* parse_ok(std::pmr::memory_resource* resource,
                      const char* sql,
                      const components::sql::parser::parser_extension_registry_t& extensions) {
    auto parsed = raw_parser(resource, sql, extensions);
    if (parsed.has_error()) {
        FAIL("raw_parser refused \"" << sql << "\": " << parsed.error().what.c_str());
    }
    return parsed.value();
}
