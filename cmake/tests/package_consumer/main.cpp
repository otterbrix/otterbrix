#include "demo_extension.hpp"

#include <components/sql/parser/extension.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>

#include <memory_resource>

int main() {
    components::sql::parser::parser_extension_registry_t registry;
    if (registry.add(make_demo_extension()).has_error()) {
        return 1;
    }
    std::pmr::monotonic_buffer_resource arena;
    auto* statements = raw_parser(&arena, "DEMO 2 + 3 * 4", registry);
    if (statements == nullptr) {
        return 2;
    }
    return nodeTag(reinterpret_cast<Node*>(linitial(statements))) == T_ExtensionNode ? 0 : 3;
}
