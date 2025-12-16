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

        auto create_index =
            logical_plan::make_node_create_index(resource_, rangevar_to_collection(node.relation), node.idxname);
        for (auto key : node.indexParams->lst) {
            create_index->keys().emplace_back(resource_, pg_ptr_cast<IndexElem>(key.data)->name);
        }
        return create_index;
    }

} // namespace components::sql::transform
