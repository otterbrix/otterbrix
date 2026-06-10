#include "demo_extension.hpp"

#include "demo_ast.hpp"
#include "demo_gram.hpp"
#include "demo_scan.h"

#include <cassert>
#include <string_view>

#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/flex_scanner_guard.hpp>
#include <components/sql/parser/nodes/parsenodes.h>
#include <components/sql/parser/pg_std_list.h>
#include <components/types/logical_value.hpp>

namespace {
    // demo's scanner: the shared macro binds the demo_yy entry points into the RAII guard type
    using demo_scanner = EXTENSION_FLEX_SCANNER(demo_yy);

    long evaluate(const demo_ext::demo_node* node) {
        switch (node->kind) {
            case demo_ext::demo_kind::number:
                return node->value;
            case demo_ext::demo_kind::add:
                return evaluate(node->lhs) + evaluate(node->rhs);
            case demo_ext::demo_kind::subtract:
                return evaluate(node->lhs) - evaluate(node->rhs);
            case demo_ext::demo_kind::multiply:
                return evaluate(node->lhs) * evaluate(node->rhs);
            case demo_ext::demo_kind::divide:
                return evaluate(node->lhs) / evaluate(node->rhs);
        }
        return 0;
    }
} // namespace

namespace demo_ext {
    using namespace components::sql::parser;

    parse_extension_result_t parse(std::pmr::memory_resource* resource, const std::string& query) {
        demo_scanner scanner(query.c_str());
        if (!scanner.valid()) {
            return NIL; // scanner failed — not ours
        }

        // route by our own keyword, consume the first token here, the grammar parses the rest
        YYSTYPE first_value{};
        if (scanner.next_token(&first_value) != KW_DEMO) {
            return NIL;
        }

        Node* root = nullptr;
        if (demo_yyparse(scanner.handle(), resource, &root) != 0 || root == nullptr) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"demo: invalid arithmetic expression", resource});
        }
        return list_make1(resource, root);
    }

    components::logical_plan::node_ptr transform(std::pmr::memory_resource* resource,
                                                 ExtensionNode* node,
                                                 components::logical_plan::parameter_node_t* /*params*/) {
        // The transformer routes by extension_id, so this only ever runs for our own nodes
        assert(node->extension_id != nullptr && std::string_view(node->extension_id) == "demo");
        const auto* root = static_cast<const demo_node*>(node->data);

        components::types::logical_value_t value(resource, static_cast<int64_t>(evaluate(root)));
        components::vector::data_chunk_t chunk(resource, {}, 1);
        chunk.set_cardinality(1);
        chunk.data.emplace_back(resource, value.type(), chunk.capacity());
        chunk.set_value(0, 0, std::move(value));
        return components::logical_plan::make_node_raw_data(resource, std::move(chunk));
    }
} // namespace demo_ext
