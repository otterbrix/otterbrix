#include <components/logical_plan/node_create_index.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {
    logical_plan::node_ptr transformer::transform_create_index(IndexStmt& node) {
        if (!(node.relation->relname && node.relation->catalogname && node.idxname)) {
            throw parser_exception_t{"incorrect create index arguments", ""};
        }

        auto qn = rangevar_to_qualified_name(node.relation);
        auto create_index = logical_plan::make_node_create_index(resource_,
                                                                 std::move(qn.dbname),
                                                                 std::move(qn.relname),
                                                                 std::string(node.idxname),
                                                                 logical_plan::index_type::single,
                                                                 std::move(qn.schemaname),
                                                                 std::move(qn.uuid));
        for (auto key : node.indexParams->lst) {
            create_index->keys().emplace_back(resource_, pg_ptr_cast<IndexElem>(key.data)->name);
        }
        return create_index;
    }

} // namespace components::sql::transform
