#include <components/logical_plan/node_create_index.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

#include <cstring>

using namespace components::expressions;

namespace components::sql::transform {

    namespace {
        logical_plan::index_type index_type_of(const char* method) {
            if (std::strcmp(method, "hash") == 0) {
                return logical_plan::index_type::hashed;
            }

            if (std::strcmp(method, "btree") == 0) {
                return logical_plan::index_type::single;
            }
            return logical_plan::index_type::no_valid;
        }
    } // namespace

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_create_index(IndexStmt& node) {
        if (!(node.relation && node.relation->relname && node.relation->catalogname && node.idxname &&
              node.accessMethod)) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"incorrect create index arguments", resource_});
        }
        // FOUR more clauses the grammar fills (gram.y, IndexStmt: `CREATE opt_unique
        // INDEX ... opt_reloptions OptTableSpace where_clause`). node_create_index
        // carries a name, a method and a column list — nothing else — so read by none
        // of them each clause is silently DROPPED on the floor while the statement
        // reports success. The worst is `unique`: CREATE UNIQUE INDEX then builds an
        // ordinary index that admits duplicates, a declared constraint that never acts.
        // Rule 6: refuse and name the clause; no index is created.
        if (node.unique) {
            return core::error_t(core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"CREATE UNIQUE INDEX is not implemented: the index built here "
                                                  "would not enforce the declared uniqueness — declare a UNIQUE "
                                                  "constraint on the table instead; no index was created",
                                                  resource_});
        }
        if (node.whereClause != nullptr) {
            return core::error_t(core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"CREATE INDEX ... WHERE (a partial index) is not implemented: "
                                                  "the predicate would have been dropped and a full index built — "
                                                  "no index was created",
                                                  resource_});
        }
        if (node.options != nullptr && !node.options->lst.empty()) {
            return core::error_t(core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"CREATE INDEX ... WITH (options) is not implemented: the options "
                                                  "would have been dropped — no index was created",
                                                  resource_});
        }
        if (node.tableSpace != nullptr) {
            std::pmr::string msg{"CREATE INDEX ... TABLESPACE ", resource_};
            msg += node.tableSpace;
            msg += " is not implemented: the tablespace would have been ignored — no index was created";
            return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
        }
        const logical_plan::index_type type = index_type_of(node.accessMethod);
        if (type == logical_plan::index_type::no_valid) {
            std::pmr::string msg{"CREATE INDEX ... USING ", resource_};
            msg += node.accessMethod;
            msg += " is not implemented; supported access methods are btree and hash — no index was created";
            return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
        }

        auto qn = rangevar_to_qualified_name(node.relation);
        const std::string dbname_for_resolve = qn.dbname;
        const std::string relname_for_resolve = qn.relname;
        auto create_index = logical_plan::make_node_create_index(resource_, core::indexname_t{std::string(node.idxname)}, type);
        for (auto key : node.indexParams->lst) {
            auto* elem = pg_ptr_cast<IndexElem>(key.data);
            // An expression element — CREATE INDEX ... ((expr)) — has no name
            // (elem->expr instead); only plain column elements are supported.
            if (elem->name == nullptr) {
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{"expression indexes are not supported; "
                                                      "CREATE INDEX accepts plain column names only",
                                                      resource_});
            }
            create_index->keys().emplace_back(resource_, elem->name);
        }
        // The indexed table's identity stays ON the node: enrich binds it to a
        // resolved entry by name and stamps ns_oid + table_oid + columns from there.
        create_index->set_dbname(dbname_for_resolve);
        create_index->set_relname(relname_for_resolve);
        // TWO demands, exactly as DROP INDEX registers them: the indexed table AND
        // the index's own name. The second probes pg_class for a relation already
        // answering to the new name — enrich stamps name_conflict_oid from it and
        // the planner refuses a taken name. A miss on this demand is the NORMAL
        // case (the name is free) and refuses nothing.
        std::vector<std::pair<std::string, std::string>> targets;
        targets.emplace_back(dbname_for_resolve, relname_for_resolve);
        targets.emplace_back(dbname_for_resolve, std::string(node.idxname));
        register_catalog_resolve_tables(resource_, &catalog_resolves_, targets);
        return create_index;
    }

} // namespace components::sql::transform
