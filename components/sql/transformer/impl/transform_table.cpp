#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/types/user_type_walk.hpp>

#include <algorithm>
#include <span>
#include <vector>

using namespace components::types;

namespace components::sql::transform {
    namespace {
        logical_plan::constraint_kind constraint_kind_of(components::table::table_constraint_type type) {
            using components::table::table_constraint_type;
            switch (type) {
                case table_constraint_type::PRIMARY_KEY:
                    return logical_plan::constraint_kind::primary_key;
                case table_constraint_type::UNIQUE:
                    return logical_plan::constraint_kind::unique;
                case table_constraint_type::FOREIGN_KEY:
                    return logical_plan::constraint_kind::foreign_key;
                case table_constraint_type::CHECK:
                    break;
            }
            return logical_plan::constraint_kind::check;
        }
    } // namespace

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_create_table(CreateStmt& node) {
        if (node.inhRelations && !node.inhRelations->lst.empty()) {
            return core::error_t(core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"CREATE TABLE ... INHERITS is not supported; no table was created",
                                                  resource_});
        }
        if (node.ofTypename) {
            return core::error_t(core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"CREATE TABLE ... OF type is not supported: list the columns; no "
                                                  "table was created",
                                                  resource_});
        }
        auto coldefs = reinterpret_cast<List*>(node.tableElts);

        VALUE_OR_RETURN(auto col_defs, get_column_definitions(resource_, *coldefs));
        register_referenced_tables(&catalog_resolves_, *coldefs);

        auto qn = rangevar_to_qualified_name(node.relation);
        const std::string& dbname = database_for(qn, namespace_policy::default_public).t;

        // Column-level (`code bigint UNIQUE`) and table-level (`UNIQUE (code)`) constraints
        // land in one list, column-level first in declaration order; downstream treats them identically.
        VALUE_OR_RETURN(auto constraints, extract_column_constraints(resource_, *coldefs, raw_sql_));
        {
            VALUE_OR_RETURN(auto table_level, extract_table_constraints(resource_, *coldefs, raw_sql_));
            constraints.insert(constraints.end(),
                               std::make_move_iterator(table_level.begin()),
                               std::make_move_iterator(table_level.end()));
        }

        // No WITH (...) option is implemented; refuse all of them rather than silently no-op
        // an unrecognized/misspelt name. `storage` is checked across the whole list first so
        // it gets its own explanatory error regardless of option order.
        if (node.options) {
            for (auto data : node.options->lst) {
                auto def = pg_ptr_cast<DefElem>(data.data);
                if (def->defname && std::string_view(def->defname) == "storage") {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"the WITH (storage = ...) option has been removed: "
                                                          "tables are always disk-backed",
                                                          resource_});
                }
            }
            for (auto data : node.options->lst) {
                auto def = pg_ptr_cast<DefElem>(data.data);
                const std::string_view name = def->defname ? std::string_view(def->defname) : std::string_view{};
                std::pmr::string msg{"CREATE TABLE ... WITH (", resource_};
                msg.append(name.empty() ? std::string_view{"<unnamed option>"} : name);
                msg.append(" = ...) is not implemented; no table was created");
                return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
            }
        }

        logical_plan::node_ptr created = logical_plan::make_node_create_collection(resource_,
                                                                                   qn.collection,
                                                                                   std::move(col_defs),
                                                                                   std::move(constraints),
                                                                                   node.if_not_exists);
        // Each declared constraint becomes the same node ALTER TABLE ADD CONSTRAINT produces,
        // hung off the create node as a child since the table has no catalog identity yet.
        auto* cn = static_cast<logical_plan::node_create_collection_t*>(created.get());
        {
            for (const auto& tc : cn->constraints()) {
                const auto kind = constraint_kind_of(tc.type);
                // A CHECK whose expression did not survive deparsing would enforce nothing.
                if (kind == logical_plan::constraint_kind::check && tc.check_expression.empty()) {
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"CHECK constraint expression contains unsupported constructs; "
                                         "allowed: comparisons, AND/OR/NOT, IS NULL/IS NOT NULL, "
                                         "column references, and constants",
                                         resource_});
                }
                const std::string ref_db = tc.ref_database.empty() ? dbname : tc.ref_database;
                auto cstr = logical_plan::make_node_create_constraint(
                    resource_,
                    qualified_name_t{core::dbname_t{dbname}, qn.collection},
                    core::constraint_name_t{tc.name},
                    kind,
                    qualified_name_t{core::dbname_t{ref_db}, core::relname_t{tc.ref_collection}});
                cstr->set_inline_with_table(true);
                cstr->set_local_col_names(tc.columns);
                if (kind == logical_plan::constraint_kind::check) {
                    cstr->set_check_expression_sql(tc.check_expression);
                }
                if (kind == logical_plan::constraint_kind::foreign_key) {
                    cstr->set_ref_col_names(tc.ref_columns);
                    cstr->set_match_type(tc.fk_matchtype);
                    cstr->set_del_action(tc.fk_del_action);
                    cstr->set_upd_action(tc.fk_upd_action);
                    // A key pointing back at the table being created has nothing to look up
                    // yet (both oids are minted by the same rewrite) — a lookup would read
                    // as "referenced relation does not exist".
                    const bool self_ref =
                        !tc.ref_collection.empty() && tc.ref_collection == qn.collection.t && ref_db == dbname;
                    cstr->set_self_reference(self_ref);
                    if (!self_ref && !tc.ref_collection.empty()) {
                        // Omitted column list binds to the parent's PRIMARY KEY (its
                        // pg_constraint rows) — same constraint gather as the ALTER path.
                        register_table(ref_db,
                                       tc.ref_collection,
                                       tc.ref_columns.empty() ? constraint_resolve_kind::outgoing
                                                              : constraint_resolve_kind::none);
                    }
                }
                created->append_child(logical_plan::node_ptr{std::move(cstr)});
            }
        }
        // Collect every UDT type_name referenced by the column defs
        // (including nested STRUCT children) so Pass 1's resolve_type
        // operator can stamp pg_type metadata into the plan-tree idx.
        std::vector<std::string> udt_names;
        // Re-read col_defs from the constructed node (we moved it above).
        for (const auto& col : cn->column_definitions()) {
            components::types::walk_user_type_refs(col.type(),
                                                   [&](std::string_view nm) { udt_names.emplace_back(nm); });
        }
        std::sort(udt_names.begin(), udt_names.end());
        udt_names.erase(std::unique(udt_names.begin(), udt_names.end()), udt_names.end());
        // The target namespace stays ON the node: enrich binds it to a resolved
        // namespace entry by name and stamps namespace_oid() from there.
        register_namespace(set_target(*cn, qn, target_slots::relation));
        // Probe the "public" namespace by default (resolve_one_type's first hit).
        // pg_catalog builtins are not in udt_names since walk_user_type_refs only
        // emits STRUCT/ENUM/UNKNOWN; pg_catalog scalars resolve via resolve_builtin
        // earlier.
        register_types(udt_names);
        return created;
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_drop(DropStmt& node,
                                                                               logical_plan::execution_plan_t* plan) {
        // Every arm below reads only `node.objects->lst.front()`; `DROP TABLE a, b, c` would otherwise
        // silently drop just `a` and report success. One node_drop_t names one object, so refuse
        // instead and name the objects that would have been skipped.
        if (!node.objects || node.objects->lst.empty()) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"DROP names no object", resource_});
        }
        // Checked once here for every object/part: the six arms below strVal() these
        // cells, and strVal on a non-T_String node reads the wrong union member.
        for (const auto& object : node.objects->lst) {
            if (!object.data || nodeTag(object.data) != T_List || pg_ptr_cast<List>(object.data)->lst.empty()) {
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{"incorrect drop: malformed object name", resource_});
            }
            for (const auto& part : pg_ptr_cast<List>(object.data)->lst) {
                if (!part.data || nodeTag(part.data) != T_String) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"incorrect drop: malformed object name", resource_});
                }
            }
        }
        if (node.objects->lst.size() > 1) {
            std::pmr::string msg{"DROP names ", resource_};
            msg += std::to_string(node.objects->lst.size());
            msg += " objects in one statement (";
            bool first = true;
            for (const auto& object : node.objects->lst) {
                if (!first) {
                    msg += ", ";
                }
                first = false;
                bool first_part = true;
                for (const auto& part : pg_ptr_cast<List>(object.data)->lst) {
                    if (!first_part) {
                        msg += '.';
                    }
                    first_part = false;
                    msg += strVal(part.data);
                }
            }
            msg += "); only one object per DROP is supported — nothing was dropped";
            return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
        }
        plan->if_exists = node.missing_ok;
        auto wrap_one = [&](const qualified_name_t& written, logical_plan::node_ptr n) {
            auto* drop = static_cast<logical_plan::node_drop_t*>(n.get());
            set_target(*drop, written, target_slots::relation);
            // One drop_behavior_of choke-point for all six DROP arms (bare = restrict_, PostgreSQL parity).
            drop->set_behavior(drop_behavior_of(node.behavior));
            register_table(written.database.t, written.collection.t, constraint_resolve_kind::none);
            return n;
        };
        // The catalog holds a relname and a relnamespace, so the arms below plan at most those two (and
        // an index name). A table spelled with uid or schema is recorded for enrich to refuse, the same
        // way a CREATE is — see set_target.
        switch (node.removeType) {
            case OBJECT_INDEX: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                if (drop_name.empty()) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"incorrect drop: arguments size", resource_});
                }
                // DROP INDEX names two pg_class rows — the parent table and the index itself
                auto wrap_index = [&](const qualified_name_t& written,
                                      const std::string& index_name,
                                      logical_plan::node_ptr n) {
                    auto* drop = static_cast<logical_plan::node_drop_t*>(n.get());
                    set_target(*drop, written, target_slots::relation);
                    drop->set_index_name(core::indexname_t{index_name});
                    // Not read yet (rewrite_drop_index builds its own delete sequence, not the
                    // dynamic cascade), but this is the only place it could be set.
                    drop->set_behavior(drop_behavior_of(node.behavior));
                    register_table(written.database.t, written.collection.t, constraint_resolve_kind::none);
                    register_table(written.database.t, index_name, constraint_resolve_kind::none);
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
                        return wrap_index(qualified_name_t{core::dbname_t{database}, core::relname_t{collection}},
                                          name,
                                          std::move(n));
                    }
                    case database_schema_table: {
                        auto it = drop_name.begin();
                        std::string database = strVal(it++->data);
                        std::string schema = strVal(it++->data);
                        std::string collection = strVal(it++->data);
                        std::string name = strVal(it->data);
                        auto n = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::index);
                        return wrap_index(qualified_name_t{core::dbname_t{database},
                                                           core::schema_t{schema},
                                                           core::relname_t{collection}},
                                          name,
                                          std::move(n));
                    }
                    case uuid_database_schema_table: {
                        auto it = drop_name.begin();
                        std::string uuid = strVal(it++->data);
                        std::string database = strVal(it++->data);
                        std::string schema = strVal(it++->data);
                        std::string collection = strVal(it++->data);
                        std::string name = strVal(it->data);
                        auto n = logical_plan::make_node_drop(resource_, logical_plan::drop_target_kind::index);
                        return wrap_index(qualified_name_t{core::uid_t{uuid},
                                                           core::dbname_t{database},
                                                           core::schema_t{schema},
                                                           core::relname_t{collection}},
                                          name,
                                          std::move(n));
                    }
                    default:
                        return core::error_t(core::error_code_t::sql_parse_error,
                                             std::pmr::string{"incorrect drop: arguments size", resource_});
                }
            }
            default:
                break;
        }
        auto kind = logical_plan::drop_target_kind::collection;
        switch (node.removeType) {
            case OBJECT_TABLE:
                break;
            case OBJECT_TYPE:
                kind = logical_plan::drop_target_kind::type;
                break;
            case OBJECT_SEQUENCE:
                kind = logical_plan::drop_target_kind::sequence;
                break;
            case OBJECT_VIEW:
                kind = logical_plan::drop_target_kind::view;
                break;
            case OBJECT_MATVIEW:
                kind = logical_plan::drop_target_kind::materialized_view;
                break;
            case OBJECT_FUNCTION:
                kind = logical_plan::drop_target_kind::macro;
                break;
            default:
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{"Unsupported removeType", resource_});
        }
        VALUE_OR_RETURN(auto written,
                        qualified_name_of(resource_, *reinterpret_cast<List*>(node.objects->lst.front().data)));
        auto n = logical_plan::make_node_drop(resource_, kind);
        if (kind != logical_plan::drop_target_kind::type) {
            return wrap_one(written, std::move(n));
        }
        // The dropped type's name stays ON the node (in the target's relname slot)
        // so enrich binds it to the resolved type entry and stamps type_oid from there.
        const std::string type_db = set_target(*n, written, target_slots::relation);
        // Unlike DROP INDEX, this arm does reach the dynamic cascade
        // (planner's rewrite_drop routes drop_target_kind::type there).
        n->set_behavior(drop_behavior_of(node.behavior));
        register_namespace(type_db);
        register_types(std::span<const std::string>{&written.collection.t, 1});
        return n;
    }
} // namespace components::sql::transform
