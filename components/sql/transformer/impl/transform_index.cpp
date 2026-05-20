#include <components/logical_plan/node_create_index.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {

    namespace {
        logical_plan::index_type detect_index_type(const char* method) {
            if (method != nullptr && std::strcmp(method, "hash") == 0) {
                return logical_plan::index_type::hashed;
            }
            return logical_plan::index_type::single;
        }
    } // namespace

    logical_plan::node_ptr transformer::transform_create_index(IndexStmt& node) {
        if (!(node.relation && node.relation->relname && node.relation->catalogname && node.idxname)) {
            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                   std::pmr::string{"incorrect create index arguments", resource_});
            return nullptr;
        }

        auto qn = rangevar_to_qualified_name(node.relation);
        const std::string dbname_for_resolve = qn.dbname;
        const std::string relname_for_resolve = qn.relname;
        auto create_index = logical_plan::make_node_create_index(resource_,
                                                                 core::indexname_t{std::string(node.idxname)},
                                                                 detect_index_type(node.accessMethod));
        for (auto key : node.indexParams->lst) {
            create_index->keys().emplace_back(resource_, pg_ptr_cast<IndexElem>(key.data)->name);
        }
        // Wrap with catalog_resolve so Pass 1 stamps ns_oid + table_oid +
        // columns; enrich_logical_plan reads from the plan-tree idx.
        return maybe_wrap_with_catalog_resolve_table(resource_,
                                                     dbname_for_resolve,
                                                     relname_for_resolve,
                                                     std::move(create_index));
    }

} // namespace components::sql::transform
