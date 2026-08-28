#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/types/user_type_walk.hpp>

#include <set>

using namespace components::types;

namespace components::sql::transform {
    // It is guaranteed to be a table ref, but in form of a list of strings
    enum table_name
    {
        table = 1,
        database_table = 2,
        database_schema_table = 3,
        uuid_database_schema_table = 4
    };

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_create_table(CreateStmt& node) {
        auto coldefs = reinterpret_cast<List*>(node.tableElts);

        VALUE_OR_RETURN(auto col_defs, get_column_definitions(resource_, *coldefs));

        auto qn = rangevar_to_qualified_name(node.relation);
        const std::string dbname = qn.dbname;

        logical_plan::node_ptr created;
        if (col_defs.empty()) {
            created =
                logical_plan::make_node_create_collection(resource_, core::relname_t{qn.relname}, node.if_not_exists);
        }

        VALUE_OR_RETURN(auto constraints, extract_table_constraints(resource_, *coldefs, raw_sql_));

        // Parse WITH (storage = 'disk') clause
        bool disk_storage = false;
        if (node.options) {
            for (auto data : node.options->lst) {
                auto def = pg_ptr_cast<DefElem>(data.data);
                if (!def->defname)
                    continue;
                std::string opt_name(def->defname);
                if (opt_name == "storage" && def->arg) {
                    std::string val(strVal(def->arg));
                    if (val == "disk") {
                        disk_storage = true;
                    }
                }
            }
        }

        created = logical_plan::make_node_create_collection(resource_,
                                                            core::relname_t{qn.relname},
                                                            std::move(col_defs),
                                                            std::move(constraints),
                                                            disk_storage,
                                                            node.if_not_exists);
        // Collect every UDT type_name referenced by the column defs
        // (including nested STRUCT children) so Pass 1's resolve_type
        // operator can stamp pg_type metadata into the plan-tree idx.
        std::set<std::string> udt_names;
        // Re-read col_defs from the constructed node (we moved it above).
        if (auto* cn = dynamic_cast<logical_plan::node_create_collection_t*>(created.get())) {
            for (const auto& col : cn->column_definitions()) {
                components::types::walk_user_type_refs(col.type(), [&](std::string_view nm) { udt_names.emplace(nm); });
            }
        }
        // The target namespace stays ON the node: enrich binds it to a resolved
        // namespace entry by name and stamps namespace_oid() from there.
        if (auto* cn = dynamic_cast<logical_plan::node_create_collection_t*>(created.get())) {
            cn->set_dbname(dbname);
        }
        register_catalog_resolve_namespace(resource_, &catalog_resolves_, dbname);
        // Probe the "public" namespace by default (resolve_one_type's first hit).
        // pg_catalog builtins are not in udt_names since walk_user_type_refs only
        // emits STRUCT/ENUM/UNKNOWN; pg_catalog scalars resolve via resolve_builtin
        // earlier.
        register_catalog_resolve_types(resource_,
                                       &catalog_resolves_,
                                       std::vector<std::string>(udt_names.begin(), udt_names.end()));
        return created;
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_drop(DropStmt& node) {
        auto wrap_one = [&](const std::string& db, const std::string& rel, logical_plan::node_ptr n) {
            auto* drop = static_cast<logical_plan::node_drop_t*>(n.get());
            drop->set_dbname(db);
            drop->set_relname(rel);
            register_catalog_resolve_table(resource_, &catalog_resolves_, db, rel);
            return n;
        };
        switch (node.removeType) {
            case OBJECT_TABLE: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                // TODO: this might have broke behavior that relied on all 4 qualifiers
                switch (static_cast<table_name>(drop_name.size())) {
                    case table: {
                        std::string collection = strVal(drop_name.front().data);
                        auto n = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::collection);
                        return wrap_one(std::string{}, collection, std::move(n));
                    }
                    case database_table: {
                        auto it = drop_name.begin();
                        std::string database = strVal(it++->data);
                        std::string collection = strVal(it->data);
                        auto n = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::collection);
                        return wrap_one(database, collection, std::move(n));
                    }
                    case database_schema_table: {
                        auto it = drop_name.begin();
                        std::string database = strVal(it++->data);
                        std::string /*schema*/ _ = strVal(it++->data);
                        std::string collection = strVal(it->data);
                        (void) _;
                        auto n = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::collection);
                        return wrap_one(database, collection, std::move(n));
                    }
                    case uuid_database_schema_table: {
                        auto it = drop_name.begin();
                        std::string /*uuid*/ _u = strVal(it++->data);
                        std::string database = strVal(it++->data);
                        std::string /*schema*/ _s = strVal(it++->data);
                        std::string collection = strVal(it->data);
                        (void) _u;
                        (void) _s;
                        auto n = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::collection);
                        return wrap_one(database, collection, std::move(n));
                    }
                    default:
                        return core::error_t(core::error_code_t::sql_parse_error,
                                             std::pmr::string{"incorrect drop: arguments size", resource_});
                }
            }
            case OBJECT_INDEX: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                if (drop_name.empty()) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"incorrect drop: arguments size", resource_});
                }
                // DROP INDEX names two pg_class rows — the parent table and the index itself
                auto wrap_index = [&](const std::string& db,
                                      const std::string& rel,
                                      const std::string& index_name,
                                      logical_plan::node_ptr n) {
                    auto* drop = static_cast<logical_plan::node_drop_t*>(n.get());
                    drop->set_dbname(db);
                    drop->set_relname(rel);
                    drop->set_index_name(index_name);
                    std::vector<std::pair<std::string, std::string>> targets;
                    targets.emplace_back(db, rel);
                    targets.emplace_back(db, index_name);
                    register_catalog_resolve_tables(resource_, &catalog_resolves_, targets);
                    return n;
                };
                //when casting to enum -1 is used to account for obligated index name
                switch (static_cast<table_name>(drop_name.size() - 1)) {
                    case database_table: {
                        auto it = drop_name.begin();
                        std::string database = strVal(it++->data);
                        std::string collection = strVal(it++->data);
                        std::string name = strVal(it->data);
                        auto n = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::index);
                        return wrap_index(database, collection, name, std::move(n));
                    }
                    case database_schema_table: {
                        auto it = drop_name.begin();
                        std::string database = strVal(it++->data);
                        std::string /*schema*/ _ = strVal(it++->data);
                        std::string collection = strVal(it++->data);
                        std::string name = strVal(it->data);
                        (void) _;
                        auto n = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::index);
                        return wrap_index(database, collection, name, std::move(n));
                    }
                    case uuid_database_schema_table: {
                        auto it = drop_name.begin();
                        std::string /*uuid*/ _u = strVal(it++->data);
                        std::string database = strVal(it++->data);
                        std::string /*schema*/ _s = strVal(it++->data);
                        std::string collection = strVal(it++->data);
                        std::string name = strVal(it->data);
                        (void) _u;
                        (void) _s;
                        auto n = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::index);
                        return wrap_index(database, collection, name, std::move(n));
                    }
                    default:
                        return core::error_t(core::error_code_t::sql_parse_error,
                                             std::pmr::string{"incorrect drop: arguments size", resource_});
                }
            }
            case OBJECT_TYPE: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                if (drop_name.empty()) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"incorrect drop: arguments size", resource_});
                }
                std::string type_name = strVal(drop_name.back().data);
                auto n = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::type);
                // The dropped type's name stays ON the node (in relname_, the
                // node's single target-name slot) so enrich binds it to the
                // resolved type entry and stamps type_oid from there.
                n->set_dbname("public");
                n->set_relname(type_name);
                register_catalog_resolve_namespace(resource_, &catalog_resolves_, "public");
                register_catalog_resolve_types(resource_, &catalog_resolves_, {type_name});
                return n;
            }
            case OBJECT_SEQUENCE: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                switch (static_cast<table_name>(drop_name.size())) {
                    case table: {
                        std::string seq_name = strVal(drop_name.front().data);
                        auto n = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::sequence);
                        return wrap_one(std::string{}, seq_name, std::move(n));
                    }
                    case database_table: {
                        auto it = drop_name.begin();
                        std::string database = strVal(it++->data);
                        std::string seq_name = strVal(it->data);
                        auto n = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::sequence);
                        return wrap_one(database, seq_name, std::move(n));
                    }
                    default:
                        return core::error_t(core::error_code_t::sql_parse_error,
                                             std::pmr::string{"incorrect drop: arguments size", resource_});
                }
            }
            case OBJECT_VIEW: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                switch (static_cast<table_name>(drop_name.size())) {
                    case table: {
                        std::string view_name = strVal(drop_name.front().data);
                        auto n = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::view);
                        return wrap_one(std::string{}, view_name, std::move(n));
                    }
                    case database_table: {
                        auto it = drop_name.begin();
                        std::string database = strVal(it++->data);
                        std::string view_name = strVal(it->data);
                        auto n = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::view);
                        return wrap_one(database, view_name, std::move(n));
                    }
                    default:
                        return core::error_t(core::error_code_t::sql_parse_error,
                                             std::pmr::string{"incorrect drop: arguments size", resource_});
                }
            }
            case OBJECT_FUNCTION: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                switch (static_cast<table_name>(drop_name.size())) {
                    case table: {
                        std::string macro_name = strVal(drop_name.front().data);
                        auto n = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::macro);
                        return wrap_one(std::string{}, macro_name, std::move(n));
                    }
                    case database_table: {
                        auto it = drop_name.begin();
                        std::string database = strVal(it++->data);
                        std::string macro_name = strVal(it->data);
                        auto n = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::macro);
                        return wrap_one(database, macro_name, std::move(n));
                    }
                    default:
                        return core::error_t(core::error_code_t::sql_parse_error,
                                             std::pmr::string{"incorrect drop: arguments size", resource_});
                }
            }
            default:
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{"Unsupported removeType", resource_});
        }
    }

} // namespace components::sql::transform
