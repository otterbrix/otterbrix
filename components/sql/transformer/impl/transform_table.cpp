#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
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
        auto coldefs = reinterpret_cast<List*>(node.tableElts);

        VALUE_OR_RETURN(auto col_defs, get_column_definitions(resource_, *coldefs));

        auto qn = rangevar_to_qualified_name(node.relation);
        const std::string dbname = qn.dbname;

        // Both syntaxes land in ONE list. A constraint written on a column
        // (`code bigint UNIQUE`) and one written as its own element (`UNIQUE (code)`)
        // differ only in where the constrained column name is spelled; everything
        // downstream — the node, the enrich guards, the pg_constraint row — is the same.
        // Column-level first, in declaration order, then the table-level ones.
        VALUE_OR_RETURN(auto constraints, extract_column_constraints(resource_, *coldefs));
        {
            VALUE_OR_RETURN(auto table_level, extract_table_constraints(resource_, *coldefs));
            constraints.insert(constraints.end(),
                               std::make_move_iterator(table_level.begin()),
                               std::make_move_iterator(table_level.end()));
        }

        // B1a: every table is disk-backed; the WITH (storage = ...) option is gone.
        // A user writing it believes it still selects a storage mode, so refuse it
        // loudly (rule 6) instead of silently handing back a disk table.
        if (node.options) {
            for (auto data : node.options->lst) {
                auto def = pg_ptr_cast<DefElem>(data.data);
                if (!def->defname)
                    continue;
                if (std::string_view(def->defname) == "storage") {
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"the WITH (storage = ...) option has been removed: "
                                         "tables are always disk-backed",
                                         resource_});
                }
            }
        }

        logical_plan::node_ptr created = logical_plan::make_node_create_collection(resource_,
                                                                                   core::relname_t{qn.relname},
                                                                                   std::move(col_defs),
                                                                                   std::move(constraints),
                                                                                   node.if_not_exists);
        // Every declared constraint becomes the SAME node ALTER TABLE ADD CONSTRAINT
        // produces — hung off the create node as a child, because the table it
        // constrains is this statement's own product and has no catalog identity yet.
        // enrich runs the guards through the parent (which owns the declared column
        // list); rewrite_create_table mints the attoids and writes the pg_constraint
        // rows into the same catalog-write sequence as pg_class / pg_attribute.
        auto* cn = static_cast<logical_plan::node_create_collection_t*>(created.get());
        {
            for (const auto& tc : cn->constraints()) {
                const auto kind = constraint_kind_of(tc.type);
                // Rule 6: a CHECK whose expression did not survive deparsing would be a
                // constraint that enforces nothing. Refuse the CREATE TABLE instead —
                // the same refusal ALTER TABLE ADD CONSTRAINT makes.
                if (kind == logical_plan::constraint_kind::check && tc.check_expression.empty()) {
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"CHECK constraint expression contains unsupported constructs; "
                                         "allowed: comparisons, AND/OR/NOT, IS NULL/IS NOT NULL, "
                                         "column references, and constants",
                                         resource_});
                }
                const std::string ref_db = tc.ref_database.empty() ? dbname : tc.ref_database;
                auto cstr = logical_plan::make_node_create_constraint(resource_,
                                                                      dbname,
                                                                      qn.relname,
                                                                      core::constraint_name_t{tc.name},
                                                                      kind,
                                                                      ref_db);
                cstr->set_inline_with_table(true);
                cstr->set_local_col_names(tc.columns);
                if (kind == logical_plan::constraint_kind::check) {
                    cstr->set_check_expr(tc.check_expression);
                }
                if (kind == logical_plan::constraint_kind::foreign_key) {
                    cstr->set_ref_relname(tc.ref_collection);
                    cstr->set_ref_col_names(tc.ref_columns);
                    cstr->set_match_type(tc.fk_matchtype);
                    cstr->set_del_action(tc.fk_del_action);
                    cstr->set_upd_action(tc.fk_upd_action);
                    // A key pointing back at the table being created has nothing to look
                    // up: both column lists are in this declaration, and both oids are
                    // minted by the same rewrite. Registering a lookup for it would
                    // resolve to nothing and read as "referenced relation does not exist".
                    const bool self_ref = !tc.ref_collection.empty() && tc.ref_collection == qn.relname &&
                                          ref_db == dbname;
                    cstr->set_self_reference(self_ref);
                    if (!self_ref && !tc.ref_collection.empty()) {
                        register_catalog_resolve_table(resource_, &catalog_resolves_, ref_db, tc.ref_collection);
                        // `REFERENCES parent` with the column list omitted binds to the
                        // parent's PRIMARY KEY, which lives in that table's pg_constraint
                        // rows — ask for its constraint gather, exactly as the ALTER path does.
                        if (tc.ref_columns.empty()) {
                            register_catalog_resolve_table(resource_,
                                                           &catalog_resolves_,
                                                           ref_db,
                                                           tc.ref_collection,
                                                           constraint_resolve_kind::outgoing);
                        }
                    }
                }
                created->append_child(logical_plan::node_ptr{std::move(cstr)});
            }
        }
        // Collect every UDT type_name referenced by the column defs
        // (including nested STRUCT children) so Pass 1's resolve_type
        // operator can stamp pg_type metadata into the plan-tree idx.
        std::set<std::string> udt_names;
        // Re-read col_defs from the constructed node (we moved it above).
        for (const auto& col : cn->column_definitions()) {
            components::types::walk_user_type_refs(col.type(), [&](std::string_view nm) { udt_names.emplace(nm); });
        }
        // The target namespace stays ON the node: enrich binds it to a resolved
        // namespace entry by name and stamps namespace_oid() from there.
        cn->set_dbname(dbname);
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
        // THE defect. Every arm below reads `node.objects->lst.front()` and never looks
        // at the rest, so `DROP TABLE a, b, c` planned one drop of `a`, executed
        // cleanly, reported SUCCESS — and left `b` and `c` exactly where they were,
        // with nothing in the answer to say so. `any_name_list` (gram.y) accepts the
        // comma list for every drop_type, so this reaches all six arms.
        //
        // One node_drop_t names one object, and execution_plan_t::sub_queries is a
        // sub-query chain feeding parameters into a single consumer — not a statement
        // list — so there is no channel here that could carry N independent drops.
        // Rule 6: refuse, and name the objects that would have been skipped.
        if (!node.objects || node.objects->lst.empty()) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"DROP names no object", resource_});
        }
        // `any_name_list` is a List of `any_name`, and every `any_name` is itself a
        // non-empty List of T_String cells (gram.y: any_name / attrs both build through
        // makeString). Checked once, here, for EVERY object and EVERY name part —
        // because the six arms below reinterpret_cast the front cell to List* and then
        // strVal() its parts, and strVal on a node that is not a T_String reads the
        // integer half of the Value union AS A char*. This guard therefore covers the
        // widest read in the function, not just its own.
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
        auto wrap_one = [&](const std::string& db, const std::string& rel, logical_plan::node_ptr n) {
            auto* drop = static_cast<logical_plan::node_drop_t*>(n.get());
            drop->set_dbname(db);
            drop->set_relname(rel);
            // `IF EXISTS`. The grammar has set DropStmt.missing_ok since the rule was
            // written; discarding it here left node_drop_t at its loud default, so the
            // one no-op success PostgreSQL grants the IF EXISTS form was unreachable
            // from SQL (the CREATE side has honoured IF NOT EXISTS all along).
            drop->set_missing_ok(node.missing_ok);
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
                    // `IF EXISTS` — same wiring as wrap_one; rewrite_drop_index already
                    // reads this flag when the index name does not resolve.
                    drop->set_missing_ok(node.missing_ok);
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
                // `IF EXISTS` — the one arm that does not build through wrap_one.
                n->set_missing_ok(node.missing_ok);
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
