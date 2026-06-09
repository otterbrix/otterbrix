#pragma once

#include <memory_resource>
#include <string>

#include <components/sql/parser/extension.hpp>

/*
 * Example parser extension: a tiny "DEMO <arithmetic>" language.
 *
 * It demonstrates the two stages every extension implements, wired together by
 * make_demo_extension():
 *   - parse     (demo_scan.l)        : raw query  -> ExtensionNode wrapping demo's own AST
 *   - transform (demo_extension.cpp) : ExtensionNode -> logical_plan
 */
namespace demo_ext {
    components::sql::parser::parse_extension_result_t parse(std::pmr::memory_resource* resource,
                                                            const std::string& query);

    components::logical_plan::node_ptr transform(std::pmr::memory_resource* resource,
                                                 ExtensionNode* node,
                                                 components::logical_plan::parameter_node_t* params);
} // namespace demo_ext

inline components::sql::parser::parser_extension_t make_demo_extension() {
    return components::sql::parser::parser_extension_t{"demo", &demo_ext::parse, &demo_ext::transform};
}
