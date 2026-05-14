#include <components/logical_plan/node_catalog_resolve_namespace.hpp>
#include <components/logical_plan/node_catalog_resolve_type.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/logical_plan/node_drop_macro.hpp>
#include <components/logical_plan/node_drop_sequence.hpp>
#include <components/logical_plan/node_drop_type.hpp>
#include <components/logical_plan/node_drop_view.hpp>
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

    logical_plan::node_ptr transformer::transform_create_table(CreateStmt& node) {
        auto coldefs = reinterpret_cast<List*>(node.tableElts);

        std::vector<components::table::column_definition_t> col_defs;
        fill_column_definitions(col_defs, resource_, *coldefs);

        auto qn = rangevar_to_qualified_name(node.relation);
        const std::string dbname = qn.dbname;

        logical_plan::node_ptr created;
        if (col_defs.empty()) {
            created = logical_plan::make_node_create_collection(resource_,
                                                                std::move(qn.dbname),
                                                                std::move(qn.relname),
                                                                std::move(qn.schemaname),
                                                                std::move(qn.uuid));
        } else {
            auto constraints = extract_table_constraints(*coldefs);

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
                                                                std::move(qn.dbname),
                                                                std::move(qn.relname),
                                                                std::move(col_defs),
                                                                std::move(constraints),
                                                                disk_storage,
                                                                std::move(qn.schemaname),
                                                                std::move(qn.uuid));
        }
        // Collect every UDT type_name referenced by the column defs
        // (including nested STRUCT children) so Pass 1's resolve_type
        // operator can stamp pg_type metadata into the plan-tree idx.
        std::set<std::string> udt_names;
        // Re-read col_defs from the constructed node (we moved it above).
        if (auto* cn = dynamic_cast<logical_plan::node_create_collection_t*>(created.get())) {
            for (const auto& col : cn->column_definitions()) {
                components::types::walk_user_type_refs(
                    col.type(),
                    [&](std::string_view nm) { udt_names.emplace(nm); });
            }
        }
        if (udt_names.empty()) {
            // No UDTs — wrap target namespace only.
            return maybe_wrap_with_catalog_resolve_namespace(resource_, dbname, std::move(created));
        }
        // Build sequence_t(resolve_ns, resolve_type per UDT…, create).
        // Pass 1 stamps each resolve_type node's type_md_by_qname entry;
        // resolve_one_type + dispatcher's existence checks read from idx.
        auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(resource_));
        if (!dbname.empty()) {
            seq->append_child(
                logical_plan::make_node_catalog_resolve_namespace(resource_, dbname));
        }
        for (const auto& nm : udt_names) {
            // Probe "public" namespace by default (resolve_one_type's first
            // hit). pg_catalog builtins are not in udt_names since
            // walk_user_type_refs only emits STRUCT/ENUM/UNKNOWN; pg_catalog
            // scalars resolve via resolve_builtin earlier.
            seq->append_child(logical_plan::make_node_catalog_resolve_type(
                resource_, std::string{"public"}, nm));
        }
        seq->append_child(std::move(created));
        return seq;
    }

    logical_plan::node_ptr transformer::transform_drop(DropStmt& node) {
        auto wrap_one = [&](const std::string& db, const std::string& rel,
                            logical_plan::node_ptr n) {
            return maybe_wrap_with_catalog_resolve_table(resource_, db, rel, std::move(n));
        };
        switch (node.removeType) {
            case OBJECT_TABLE: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                switch (static_cast<table_name>(drop_name.size())) {
                    case table: {
                        std::string collection = strVal(drop_name.front().data);
                        auto n = logical_plan::make_node_drop_collection(resource_, std::string{}, collection);
                        return wrap_one(std::string{}, collection, std::move(n));
                    }
                    case database_table: {
                        auto it = drop_name.begin();
                        std::string database = strVal(it++->data);
                        std::string collection = strVal(it->data);
                        auto n = logical_plan::make_node_drop_collection(resource_, database, collection);
                        return wrap_one(database, collection, std::move(n));
                    }
                    case database_schema_table: {
                        auto it = drop_name.begin();
                        std::string database = strVal(it++->data);
                        std::string schema = strVal(it++->data);
                        std::string collection = strVal(it->data);
                        auto n = logical_plan::make_node_drop_collection(resource_, database, collection, schema);
                        return wrap_one(database, collection, std::move(n));
                    }
                    case uuid_database_schema_table: {
                        auto it = drop_name.begin();
                        std::string uuid = strVal(it++->data);
                        std::string database = strVal(it++->data);
                        std::string schema = strVal(it++->data);
                        std::string collection = strVal(it->data);
                        auto n = logical_plan::make_node_drop_collection(resource_, database, collection, schema, uuid);
                        return wrap_one(database, collection, std::move(n));
                    }
                    default:
                        throw parser_exception_t{"incorrect drop: arguments size", ""};
                        return logical_plan::make_node_drop_collection(resource_, std::string{}, std::string{});
                }
            }
            case OBJECT_INDEX: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                if (drop_name.empty()) {
                    throw parser_exception_t{"incorrect drop: arguments size", ""};
                }
                auto wrap_index = [&](const std::string& db, const std::string& rel,
                                       const std::string& index_name,
                                       logical_plan::node_ptr n) {
                    std::vector<std::pair<std::string, std::string>> targets;
                    targets.emplace_back(db, rel);
                    targets.emplace_back(db, index_name);
                    return maybe_wrap_with_catalog_resolve_tables(
                        resource_, std::move(targets), std::move(n));
                };
                //when casting to enum -1 is used to account for obligated index name
                switch (static_cast<table_name>(drop_name.size() - 1)) {
                    case database_table: {
                        auto it = drop_name.begin();
                        std::string database = strVal(it++->data);
                        std::string collection = strVal(it++->data);
                        std::string name = strVal(it->data);
                        auto n = logical_plan::make_node_drop_index(resource_, database, collection, name);
                        return wrap_index(database, collection, name, std::move(n));
                    }
                    case database_schema_table: {
                        auto it = drop_name.begin();
                        std::string database = strVal(it++->data);
                        std::string schema = strVal(it++->data);
                        std::string collection = strVal(it++->data);
                        std::string name = strVal(it->data);
                        auto n = logical_plan::make_node_drop_index(resource_, database, collection, name, schema);
                        return wrap_index(database, collection, name, std::move(n));
                    }
                    case uuid_database_schema_table: {
                        auto it = drop_name.begin();
                        std::string uuid = strVal(it++->data);
                        std::string database = strVal(it++->data);
                        std::string schema = strVal(it++->data);
                        std::string collection = strVal(it++->data);
                        std::string name = strVal(it->data);
                        auto n = logical_plan::make_node_drop_index(resource_, database, collection, name, schema, uuid);
                        return wrap_index(database, collection, name, std::move(n));
                    }
                    default:
                        throw parser_exception_t{"incorrect drop: arguments size", ""};
                        return logical_plan::make_node_drop_index(resource_, std::string{}, std::string{}, std::string{});
                }
            }
            case OBJECT_TYPE: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                if (drop_name.empty()) {
                    throw parser_exception_t{"incorrect drop: arguments size", ""};
                }
                std::string type_name = strVal(drop_name.back().data);
                auto n = logical_plan::make_node_drop_type(resource_, type_name);
                // M4.F: wrap with resolve_type so Pass 1 stamps type_oid +
                // resolved_type_metadata. enrich's drop_type_t branch reads
                // from plan-tree idx (resolve_type_t stamps it at Pass 1).
                // Phase 9.W: node_drop_type_t has no user-typed dbname, so we
                // default to public_namespace (matching enrich/dispatcher).
                auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(resource_));
                seq->append_child(logical_plan::make_node_catalog_resolve_namespace(
                    resource_, std::string{"public"}));
                seq->append_child(logical_plan::make_node_catalog_resolve_type(
                    resource_, std::string{"public"}, type_name));
                seq->append_child(std::move(n));
                return seq;
            }
            case OBJECT_SEQUENCE: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                switch (static_cast<table_name>(drop_name.size())) {
                    case table: {
                        std::string seq_name = strVal(drop_name.front().data);
                        auto n = logical_plan::make_node_drop_sequence(resource_, std::string{}, seq_name);
                        return wrap_one(std::string{}, seq_name, std::move(n));
                    }
                    case database_table: {
                        auto it = drop_name.begin();
                        std::string database = strVal(it++->data);
                        std::string seq_name = strVal(it->data);
                        auto n = logical_plan::make_node_drop_sequence(resource_, database, seq_name);
                        return wrap_one(database, seq_name, std::move(n));
                    }
                    default:
                        throw parser_exception_t{"incorrect drop: arguments size", ""};
                }
            }
            case OBJECT_VIEW: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                switch (static_cast<table_name>(drop_name.size())) {
                    case table: {
                        std::string view_name = strVal(drop_name.front().data);
                        auto n = logical_plan::make_node_drop_view(resource_, std::string{}, view_name);
                        return wrap_one(std::string{}, view_name, std::move(n));
                    }
                    case database_table: {
                        auto it = drop_name.begin();
                        std::string database = strVal(it++->data);
                        std::string view_name = strVal(it->data);
                        auto n = logical_plan::make_node_drop_view(resource_, database, view_name);
                        return wrap_one(database, view_name, std::move(n));
                    }
                    default:
                        throw parser_exception_t{"incorrect drop: arguments size", ""};
                }
            }
            case OBJECT_FUNCTION: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                switch (static_cast<table_name>(drop_name.size())) {
                    case table: {
                        std::string macro_name = strVal(drop_name.front().data);
                        auto n = logical_plan::make_node_drop_macro(resource_, std::string{}, macro_name);
                        return wrap_one(std::string{}, macro_name, std::move(n));
                    }
                    case database_table: {
                        auto it = drop_name.begin();
                        std::string database = strVal(it++->data);
                        std::string macro_name = strVal(it->data);
                        auto n = logical_plan::make_node_drop_macro(resource_, database, macro_name);
                        return wrap_one(database, macro_name, std::move(n));
                    }
                    default:
                        throw parser_exception_t{"incorrect drop: arguments size", ""};
                }
            }
            default:
                throw std::runtime_error("Unsupported removeType");
        }
    }

} // namespace components::sql::transform
