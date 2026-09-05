// Logical plan enrichment.
//
// Runs after SQL parsing and before physical plan generation. Reads the
// plan-tree resolve idx (populated by operator_resolve_*_t) to annotate DML
// nodes with the data they need at execution time:
//   INSERT  — not_null_cols, outgoing FK references, CHECK expressions
//   UPDATE  — not_null_cols, outgoing FK references, CHECK expressions
//   DELETE  — referencing FKs (for CASCADE / SET NULL / SET DEFAULT)
//   CREATE  — namespace_oid (for catalog registration)
//
// No disk I/O of its own — all catalog metadata comes from the resolve idx
// materialized in-plan by the resolve operators.

#include "enrich_logical_plan.hpp"

#include "resolve_type.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/cursor/cursor.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_alter_column.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_matview.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_extension.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_having.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_refresh_matview.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <services/index/manager_index.hpp>

#include <algorithm>
#include <limits>
#include <queue>
#include <string>
#include <unordered_map>

namespace services::dispatcher { namespace {

    using components::logical_plan::catalog_resolves_t;

    void fill_not_null(const components::logical_plan::resolved_table_metadata_t& md,
                       std::vector<std::string>& out,
                       bool include_with_defaults) {
        for (const auto& col : md.columns) {
            if (col.attnotnull && (include_with_defaults || !col.atthasdefault)) {
                out.push_back(col.attname);
            }
        }
    }

    // PRIMARY KEY implies NOT NULL, but pg_attribute.attnotnull is only written for
    // column-level constraints at CREATE TABLE — ALTER TABLE ADD PRIMARY KEY / a
    // table-level PK never back-fills it. Merge the resolved PK columns into the DML
    // node's NOT-NULL list. include_with_defaults mirrors fill_not_null's policy:
    // INSERT skips DEFAULT-backed columns (the disk agent fills them non-NULL);
    // UPDATE keeps them (the write-set carries every column, an explicit NULL must
    // still fail).
    void merge_pk_not_null(const components::logical_plan::resolved_table_metadata_t* md,
                           const std::vector<std::string>& pk_columns,
                           std::vector<std::string>& not_null,
                           bool include_with_defaults) {
        for (const auto& col : pk_columns) {
            if (!include_with_defaults && md != nullptr) {
                bool has_default = false;
                for (const auto& c : md->columns) {
                    if (c.attname == col) {
                        has_default = c.atthasdefault;
                        break;
                    }
                }
                if (has_default) {
                    continue;
                }
            }
            if (std::find(not_null.begin(), not_null.end(), col) == not_null.end()) {
                not_null.push_back(col);
            }
        }
    }

    // Decoded column DEFAULT values for the constraint operators: an INSERT omitting
    // a defaulted column stores the default (filled agent-side at storage_append), so
    // CHECK / UNIQUE must evaluate the ABSENT column AS its default.
    std::vector<std::pair<std::string, components::types::logical_value_t>>
    decode_column_defaults(std::pmr::memory_resource* resource,
                           const components::logical_plan::resolved_table_metadata_t& md) {
        std::vector<std::pair<std::string, components::types::logical_value_t>> defaults;
        for (const auto& col : md.columns) {
            if (!col.atthasdefault || col.attdefspec.empty()) {
                continue;
            }
            if (auto v = components::catalog::decode_default_spec(resource, col.attdefspec)) {
                defaults.emplace_back(col.attname, std::move(*v));
            }
        }
        return defaults;
    }

    void enrich_insert_sync(components::logical_plan::node_insert_t* node) {
        // bind_catalog_data already pasted the target's metadata onto the node.
        const auto* md = node->table_metadata();
        if (!md)
            return;
        std::vector<std::string> nn;
        fill_not_null(*md, nn, /*include_with_defaults=*/false);
        node->set_not_null_cols(std::move(nn));

        // A too-short value for a fixed ARRAY reconciles by padding NULL, which a NOT NULL column
        // cannot accept, so such values must error before the append (see
        // node_insert_t::array_size_reqs). A DEFAULT does not exempt the column: a default fills an
        // ABSENT column, never the missing tail of a value that was supplied.
        std::vector<std::pair<std::string, uint64_t>> array_reqs;
        for (const auto& col : md->columns) {
            if (col.type.type() == components::types::logical_type::ARRAY && col.attnotnull) {
                const auto size =
                    static_cast<const components::types::array_logical_type_extension*>(col.type.extension())->size();
                array_reqs.emplace_back(col.attname, size);
            }
        }
        node->set_array_size_reqs(std::move(array_reqs));
    }

    void enrich_update_sync(components::logical_plan::node_update_t* node) {
        const auto* md = node->table_metadata();
        if (!md)
            return;
        std::vector<std::string> nn;
        fill_not_null(*md, nn, /*include_with_defaults=*/true);
        node->set_not_null_cols(std::move(nn));
    }

}} // namespace services::dispatcher::

// Helpers shared between the dispatcher and executor pipelines.
namespace services::catalog_resolve {

    using components::logical_plan::catalog_resolves_t;
    using components::logical_plan::resolve_entry_t;

    namespace {

        // A nullable view over one resolved entry, in the accessor shape the
        // per-consumer stamping cases below read.
        struct entry_view_t {
            const resolve_entry_t* entry{nullptr};

            explicit operator bool() const noexcept { return entry != nullptr; }
            const entry_view_t* operator->() const noexcept { return this; }
            components::catalog::oid_t namespace_oid() const noexcept { return entry->namespace_oid; }
            components::catalog::oid_t type_oid() const noexcept { return entry->type_oid; }
            components::catalog::oid_t table_oid() const noexcept {
                return entry->table_md.has_value() ? entry->table_md->table_oid : components::catalog::INVALID_OID;
            }
            const std::string& relname() const noexcept { return entry->relname; }
            const std::optional<components::logical_plan::resolved_table_metadata_t>&
            resolved_metadata() const noexcept {
                return entry->table_md;
            }
        };

        struct target_names_t {
            std::string_view dbname{};
            std::string_view relname{};
            std::string_view secondary_relname{};
            std::string_view namespace_dbname{};
            std::string_view type_name{};
        };

        target_names_t target_names_of(const components::logical_plan::node_t* node) {
            using namespace components::logical_plan;
            switch (node->type()) {
                case node_type::aggregate_t: {
                    const auto* d = static_cast<const node_aggregate_t*>(node);
                    return {d->dbname().t, d->relname().t, {}};
                }
                case node_type::match_t: {
                    const auto* d = static_cast<const node_match_t*>(node);
                    return {d->dbname(), d->relname(), {}};
                }
                case node_type::group_t: {
                    const auto* d = static_cast<const node_group_t*>(node);
                    return {d->dbname(), d->relname(), {}};
                }
                case node_type::sort_t: {
                    const auto* d = static_cast<const node_sort_t*>(node);
                    return {d->dbname(), d->relname(), {}};
                }
                case node_type::join_t: {
                    const auto* d = static_cast<const node_join_t*>(node);
                    return {d->dbname(), d->relname(), {}};
                }
                case node_type::limit_t: {
                    const auto* d = static_cast<const node_limit_t*>(node);
                    return {d->dbname(), d->relname(), {}};
                }
                case node_type::having_t: {
                    const auto* d = static_cast<const node_having_t*>(node);
                    return {d->dbname(), d->relname(), {}};
                }
                case node_type::extension_t: {
                    const auto* d = static_cast<const node_extension_t*>(node);
                    return {d->dbname(), d->relname(), {}};
                }
                case node_type::insert_t: {
                    const auto* d = static_cast<const node_insert_t*>(node);
                    return {d->dbname(), d->relname(), {}};
                }
                case node_type::update_t: {
                    const auto* d = static_cast<const node_update_t*>(node);
                    return {d->dbname(), d->relname(), {}};
                }
                case node_type::delete_t: {
                    const auto* d = static_cast<const node_delete_t*>(node);
                    return {d->dbname(), d->relname(), {}};
                }
                case node_type::drop_t: {
                    const auto* d = static_cast<const node_drop_t*>(node);
                    if (d->kind() == drop_target_kind::type) {
                        return {d->dbname(), {}, {}, {}, d->relname()};
                    }
                    return {d->dbname(), d->relname(), d->index_name()};
                }
                case node_type::create_database_t: {
                    const auto* d = static_cast<const node_create_database_t*>(node);
                    return {d->dbname(), {}, {}};
                }
                case node_type::create_collection_t: {
                    const auto* d = static_cast<const node_create_collection_t*>(node);
                    return {d->dbname(), {}, {}};
                }
                case node_type::create_sequence_t: {
                    const auto* d = static_cast<const node_create_sequence_t*>(node);
                    return {d->dbname(), {}, {}};
                }
                case node_type::create_view_t: {
                    const auto* d = static_cast<const node_create_view_t*>(node);
                    return {d->dbname(), {}, {}};
                }
                case node_type::create_macro_t: {
                    const auto* d = static_cast<const node_create_macro_t*>(node);
                    return {d->dbname(), {}, {}};
                }
                case node_type::create_type_t: {
                    const auto* d = static_cast<const node_create_type_t*>(node);
                    return {d->dbname(), {}, {}};
                }
                case node_type::create_index_t: {
                    const auto* d = static_cast<const node_create_index_t*>(node);
                    return {d->dbname(), d->relname(), {}};
                }
                case node_type::alter_table_t: {
                    const auto* d = static_cast<const node_alter_table_t*>(node);
                    return {d->dbname(), d->relname(), {}};
                }
                case node_type::create_constraint_t: {
                    const auto* d = static_cast<const node_create_constraint_t*>(node);
                    return {d->dbname(), d->relname(), d->ref_relname()};
                }
                case node_type::create_matview_t: {
                    // Binds against its SOURCE table (whose columns the planner needs)
                    // while living in its own namespace.
                    const auto* d = static_cast<const node_create_matview_t*>(node);
                    return {d->source_dbname(), d->source_relname(), {}, d->dbname()};
                }
                case node_type::refresh_matview_t: {
                    const auto* d = static_cast<const node_refresh_matview_t*>(node);
                    return {d->dbname(), d->matviewname(), {}};
                }
                default:
                    return {};
            }
        }

    } // namespace

    // Derive a materialized view's output schema from its body plan + the
    // source table's resolved_metadata. Supports only single-table FROM with
    // scalar_type::get_field expressions (plain column references). Returns
    // empty on unsupported shapes — the planner surfaces this as an error
    // (no fallback).
    static std::vector<components::table::column_definition_t>
    derive_matview_output_schema(const components::logical_plan::node_t* body_plan,
                                 const components::logical_plan::resolved_table_metadata_t* source_md) {
        using namespace components::logical_plan;
        std::vector<components::table::column_definition_t> out;
        if (!body_plan || !source_md) {
            return out;
        }
        if (body_plan->type() != node_type::aggregate_t) {
            return out;
        }
        // Find the node holding the SELECT-list expressions. The transformer routes the whole
        // target list to the GROUP node and leaves the select EMPTY, so the group is where the
        // output columns live; the select still carries them for shapes that never grow a group.
        const node_t* select_node = nullptr;
        const node_t* group_node = nullptr;
        for (const auto& c : body_plan->children()) {
            if (!c) {
                continue;
            }
            if (c->type() == node_type::select_t) {
                select_node = c.get();
            } else if (c->type() == node_type::group_t) {
                group_node = c.get();
            }
        }
        const node_t* target_list =
            select_node != nullptr && !select_node->expressions().empty() ? select_node : group_node;
        if (target_list == nullptr) {
            return out;
        }
        const auto& exprs = target_list->expressions();
        out.reserve(exprs.size());
        for (const auto& expr : exprs) {
            if (!expr) {
                return {};
            }
            // A grouping key is not an output column of its own — the target list names it
            // separately where it is projected.
            if (auto* key_expr = dynamic_cast<components::expressions::scalar_expression_t*>(expr.get());
                key_expr != nullptr && key_expr->type() == components::expressions::scalar_type::group_field) {
                continue;
            }
            auto* sc = dynamic_cast<components::expressions::scalar_expression_t*>(expr.get());
            if (!sc) {
                return {}; // non-scalar (function/aggregate): out of scope
            }
            if (sc->type() != components::expressions::scalar_type::get_field) {
                return {}; // arithmetic/case_expr/coalesce/...: out of scope
            }
            const auto& key_storage = sc->key().storage();
            if (key_storage.empty()) {
                return {};
            }
            // Use the last path component as the column name (handles
            // single-table FROM where path is just [col]).
            const std::string col_name(key_storage.back().c_str(), key_storage.back().size());
            // Look up the column in the source's stamped pg_attribute.
            bool found = false;
            for (const auto& src_col : source_md->columns) {
                if (src_col.attname == col_name) {
                    components::table::column_definition_t def(col_name, src_col.type);
                    def.set_atttypid(static_cast<std::uint32_t>(src_col.atttypid));
                    out.emplace_back(std::move(def));
                    found = true;
                    break;
                }
            }
            if (!found) {
                return {};
            }
        }
        return out;
    }

    // Mark every node whose target table is `table_oid` with whether that table has any index.
    // Walks the whole tree by oid rather than trusting position: a statement can carry several
    // tables, and the DML target is not necessarily the last one resolved.
    void stamp_table_has_indexes(components::logical_plan::node_t* root,
                                 components::catalog::oid_t table_oid,
                                 bool has_indexes) {
        using namespace components::logical_plan;
        if (root == nullptr || table_oid == components::catalog::INVALID_OID) {
            return;
        }
        std::queue<node_t*> q;
        q.push(root);
        while (!q.empty()) {
            auto* n = q.front();
            q.pop();
            if (n->table_oid() == table_oid) {
                n->set_table_has_indexes(has_indexes);
            }
            for (const auto& child : n->children()) {
                if (child) {
                    q.push(child.get());
                }
            }
        }
    }

    // Copy resolved catalog data onto the consumer nodes that asked for it.
    void bind_catalog_data(components::logical_plan::node_t* root, const catalog_resolves_t& resolves) {
        using namespace components::logical_plan;
        if (!root)
            return;
        std::queue<node_t*> q;
        q.push(root);
        while (!q.empty()) {
            auto* n = q.front();
            q.pop();
            {
                // The names this node targets. Empty for nodes that target nothing,
                // which then bind nothing below.
                const auto names = target_names_of(n);
                const entry_view_t rn{
                    resolves.namespace_entry(names.namespace_dbname.empty() ? names.dbname : names.namespace_dbname)};
                const entry_view_t rt{resolves.table_entry(names.dbname, names.relname)};
                const entry_view_t rt_index{resolves.table_entry(names.dbname, names.secondary_relname)};
                const entry_view_t ry{resolves.type_entry(names.dbname, names.type_name)};
                // The table this node targets, pasted whole so validation reads
                // columns / relkind / flags straight off the node.
                if (rt) {
                    if (rt.table_oid() != components::catalog::INVALID_OID) {
                        n->set_table_oid(rt.table_oid());
                    }
                    if (rt.resolved_metadata().has_value()) {
                        n->set_table_metadata(&rt.resolved_metadata().value());
                    }
                }
                {
                    switch (n->type()) {
                        case node_type::drop_t: {
                            auto* d = static_cast<node_drop_t*>(n);
                            switch (d->kind()) {
                                case drop_target_kind::database: {
                                    if (rn && rn->namespace_oid() != components::catalog::INVALID_OID) {
                                        d->set_namespace_oid(rn->namespace_oid());
                                    }
                                    break;
                                }
                                case drop_target_kind::collection: {
                                    if (rn && rn->namespace_oid() != components::catalog::INVALID_OID) {
                                        d->set_namespace_oid(rn->namespace_oid());
                                    } else if (rt && rt->namespace_oid() != components::catalog::INVALID_OID) {
                                        d->set_namespace_oid(rt->namespace_oid());
                                    }
                                    if (rt && rt->table_oid() != components::catalog::INVALID_OID) {
                                        d->set_table_oid(rt->table_oid());
                                    }
                                    break;
                                }
                                case drop_target_kind::view:
                                case drop_target_kind::sequence:
                                case drop_target_kind::macro: {
                                    if (rt && rt->table_oid() != components::catalog::INVALID_OID) {
                                        d->set_table_oid(rt->table_oid());
                                    }
                                    break;
                                }
                                case drop_target_kind::index: {
                                    if (rn && rn->namespace_oid() != components::catalog::INVALID_OID) {
                                        d->set_namespace_oid(rn->namespace_oid());
                                    } else if (rt && rt->namespace_oid() != components::catalog::INVALID_OID) {
                                        d->set_namespace_oid(rt->namespace_oid());
                                    }
                                    if (rt && rt->table_oid() != components::catalog::INVALID_OID) {
                                        d->set_table_oid(rt->table_oid());
                                    }
                                    // Name → indexrelid resolution happens HERE, once, at the
                                    // planner boundary (rule 16): everything below carries only
                                    // the oid. rt_index resolves the index's pg_class entry, so
                                    // its table_oid slot holds the index relation's own oid.
                                    if (rt_index && rt_index->table_oid() != components::catalog::INVALID_OID) {
                                        d->set_index_oid(rt_index->table_oid());
                                    }
                                    break;
                                }
                                case drop_target_kind::type: {
                                    if (ry && ry->type_oid() != components::catalog::INVALID_OID) {
                                        d->set_type_oid(ry->type_oid());
                                    }
                                    break;
                                }
                            }
                            break;
                        }
                        case node_type::create_collection_t: {
                            auto* d = static_cast<node_create_collection_t*>(n);
                            if (rn && rn->namespace_oid() != components::catalog::INVALID_OID) {
                                d->set_namespace_oid(rn->namespace_oid());
                            }
                            break;
                        }
                        case node_type::create_sequence_t: {
                            auto* d = static_cast<node_create_sequence_t*>(n);
                            if (rn && rn->namespace_oid() != components::catalog::INVALID_OID) {
                                d->set_namespace_oid(rn->namespace_oid());
                            }
                            break;
                        }
                        case node_type::create_view_t: {
                            auto* d = static_cast<node_create_view_t*>(n);
                            if (rn && rn->namespace_oid() != components::catalog::INVALID_OID) {
                                d->set_namespace_oid(rn->namespace_oid());
                            }
                            break;
                        }
                        case node_type::create_macro_t: {
                            auto* d = static_cast<node_create_macro_t*>(n);
                            if (rn && rn->namespace_oid() != components::catalog::INVALID_OID) {
                                d->set_namespace_oid(rn->namespace_oid());
                            }
                            break;
                        }
                        case node_type::create_matview_t: {
                            // Stamp namespace + source oids from sibling resolves.
                            // derive_matview_output_schema walks body_plan +
                            // source's resolved_metadata.columns to produce
                            // the matview's column schema.
                            auto* d = static_cast<node_create_matview_t*>(n);
                            if (rn && rn->namespace_oid() != components::catalog::INVALID_OID) {
                                d->set_namespace_oid(rn->namespace_oid());
                            }
                            if (rt && rt->table_oid() != components::catalog::INVALID_OID) {
                                d->set_source_table_oid(rt->table_oid());
                            }
                            if (rt && rt->resolved_metadata() && d->body_plan()) {
                                auto cols = derive_matview_output_schema(d->body_plan().get(),
                                                                         &rt->resolved_metadata().value());
                                if (!cols.empty()) {
                                    d->set_inferred_columns(std::move(cols));
                                }
                            }
                            break;
                        }
                        case node_type::refresh_matview_t: {
                            // refresh: mv_oid comes from sibling rt's resolved_metadata
                            // (which also carries view_sql — operator_resolve_table
                            // reads pg_rewrite for relkind='m').
                            // No fields to stamp here — planner reads from rt directly.
                            break;
                        }
                        case node_type::create_index_t: {
                            auto* d = static_cast<node_create_index_t*>(n);
                            if (rt && rt->namespace_oid() != components::catalog::INVALID_OID) {
                                d->set_namespace_oid(rt->namespace_oid());
                            } else if (rn && rn->namespace_oid() != components::catalog::INVALID_OID) {
                                d->set_namespace_oid(rn->namespace_oid());
                            }
                            if (rt && rt->table_oid() != components::catalog::INVALID_OID) {
                                d->set_table_oid(rt->table_oid());
                            }
                            break;
                        }
                        case node_type::create_constraint_t: {
                            auto* d = static_cast<node_create_constraint_t*>(n);
                            if (rt_index && rt_index->table_oid() != components::catalog::INVALID_OID) {
                                d->set_ref_table_oid(rt_index->table_oid());
                            }
                            break;
                        }
                        // DML consumers carry only OIDs now; stamp table_oid from the
                        // sibling resolve_table inside the same sequence_t. The
                        // UPDATE FROM / DELETE USING source is a child sub-plan whose
                        // own scans self-resolve by name (this same enrich walk), so
                        // there is no from-side OID to stamp here.
                        case node_type::insert_t: {
                            auto* d = static_cast<node_insert_t*>(n);
                            if (rt && rt->table_oid() != components::catalog::INVALID_OID) {
                                d->set_table_oid(rt->table_oid());
                            }
                            break;
                        }
                        case node_type::update_t: {
                            auto* d = static_cast<node_update_t*>(n);
                            if (rt && rt->table_oid() != components::catalog::INVALID_OID) {
                                d->set_table_oid(rt->table_oid());
                            }
                            break;
                        }
                        case node_type::delete_t: {
                            auto* d = static_cast<node_delete_t*>(n);
                            if (rt && rt->table_oid() != components::catalog::INVALID_OID) {
                                d->set_table_oid(rt->table_oid());
                            }
                            break;
                        }
                        // alter_* nodes carry only OIDs now; stamp
                        // table_oid from the sibling resolve_table inside
                        // the wrapping sequence_t. The child-emitting
                        // planner cases (alter_column_*) keep their own
                        // table_oid set at construction time — re-stamping
                        // from a sibling resolve here is a no-op for them.
                        case node_type::alter_table_t:
                        case node_type::alter_column_t: {
                            if (rt && rt->table_oid() != components::catalog::INVALID_OID) {
                                n->set_table_oid(rt->table_oid());
                            }
                            break;
                        }
                        default:
                            break;
                    }
                }
            }
            for (const auto& c : n->children()) {
                if (c)
                    q.push(c.get());
            }
        }
    }

    // --- view expansion helpers ---

    const resolve_entry_t* find_view_entry(const catalog_resolves_t& resolves,
                                           const components::logical_plan::node_t* root) {
        using namespace components::logical_plan;
        if (!root || !resolves.tables) {
            return nullptr;
        }
        // Collect the table names THIS plan reads, so a view belonging to another
        // sub-query (the entries are plan-wide) is never expanded here.
        std::queue<const node_t*> q;
        q.push(root);
        while (!q.empty()) {
            const auto* n = q.front();
            q.pop();
            const auto names = target_names_of(n);
            if (!names.relname.empty()) {
                if (const auto* entry = resolves.table_entry(names.dbname, names.relname)) {
                    if (entry->table_md.has_value() && entry->table_md->relkind == components::catalog::relkind::view &&
                        !entry->table_md->view_sql.empty()) {
                        return entry;
                    }
                }
            }
            for (const auto& c : n->children()) {
                if (c) {
                    q.push(c.get());
                }
            }
        }
        return nullptr;
    }

    view_expansion_result_t expand_view_body(std::pmr::memory_resource* resource, const std::string& view_sql) {
        view_expansion_result_t out;
        std::pmr::monotonic_buffer_resource parser_arena(resource);
        void* parse_cell = nullptr;
        try {
            auto* parsed = raw_parser(&parser_arena, view_sql.c_str());
            if (!parsed) {
                out.error = components::cursor::make_cursor(
                    resource,
                    core::error_t(core::error_code_t::sql_parse_error,
                                  std::pmr::string{"view body re-parse returned null", resource}));
                return out;
            }
            parse_cell = linitial(parsed);
        } catch (const std::exception& ex) {
            out.error = components::cursor::make_cursor(
                resource,
                core::error_t(core::error_code_t::sql_parse_error, std::pmr::string{ex.what(), resource}));
            return out;
        }
        if (!parse_cell) {
            out.error =
                components::cursor::make_cursor(resource,
                                                core::error_t(core::error_code_t::sql_parse_error,
                                                              std::pmr::string{"empty view body parse", resource}));
            return out;
        }
        components::sql::transform::transformer local_transformer(resource, view_sql.c_str());
        auto tr = local_transformer.transform(components::sql::transform::pg_cell_to_node_cast(parse_cell)).finalize();
        if (tr.has_error()) {
            out.error = components::cursor::make_cursor(resource, tr.error());
            return out;
        }
        // Fresh plan with its own catalog lookups (the view body's FROM tables);
        // they still need a resolve round once merged into the outer plan's.
        out.had_expansion = true;
        out.expanded_plan = std::move(tr.value().sub_queries.back());
        out.expanded_resolves = std::move(tr.value().catalog_resolves);
        out.expanded_params = std::move(tr.value().parameters);
        return out;
    }

    void register_plan_targets(std::pmr::memory_resource* resource,
                               const components::logical_plan::node_t* root,
                               catalog_resolves_t* resolves) {
        using namespace components::logical_plan;
        if (!root || !resolves) {
            return;
        }
        std::queue<const node_t*> q;
        q.push(root);
        while (!q.empty()) {
            const auto* n = q.front();
            q.pop();
            const auto names = target_names_of(n);
            const auto namespace_dbname = names.namespace_dbname.empty() ? names.dbname : names.namespace_dbname;
            if (!namespace_dbname.empty()) {
                resolve_entry_t entry;
                entry.dbname = namespace_dbname;
                resolves->ensure(resource, resolve_kind::namespace_).add(std::move(entry));
            }
            for (const auto& relname : {names.relname, names.secondary_relname}) {
                if (relname.empty()) {
                    continue;
                }
                resolve_entry_t entry;
                entry.dbname = names.dbname;
                entry.relname = relname;
                resolves->ensure(resource, resolve_kind::table).add(std::move(entry));
            }
            if (!names.type_name.empty()) {
                resolve_entry_t entry;
                entry.dbname = names.dbname;
                entry.type_name = names.type_name;
                resolves->ensure(resource, resolve_kind::type).add(std::move(entry));
            }
            for (const auto& c : n->children()) {
                if (c) {
                    q.push(c.get());
                }
            }
        }
    }

    void merge_catalog_resolves(std::pmr::memory_resource* resource,
                                catalog_resolves_t& dest,
                                const catalog_resolves_t& src) {
        using components::logical_plan::resolve_kind;
        for (const auto& [kind, slot] : {std::pair{resolve_kind::database, &src.database},
                                         std::pair{resolve_kind::namespace_, &src.namespaces},
                                         std::pair{resolve_kind::table, &src.tables},
                                         std::pair{resolve_kind::type, &src.types},
                                         std::pair{resolve_kind::constraint, &src.constraints}}) {
            if (!*slot || (*slot)->empty()) {
                continue;
            }
            auto& target = dest.ensure(resource, kind);
            for (const auto& entry : (*slot)->entries()) {
                target.add(entry);
            }
        }
    }

    bool has_unresolved_entries(const catalog_resolves_t& resolves) {
        using namespace components::logical_plan;
        if (resolves.tables) {
            for (const auto& entry : resolves.tables->entries()) {
                if (!entry.table_md.has_value()) {
                    return true;
                }
            }
        }
        if (resolves.namespaces) {
            for (const auto& entry : resolves.namespaces->entries()) {
                if (entry.namespace_oid == components::catalog::INVALID_OID) {
                    return true;
                }
            }
        }
        if (resolves.database) {
            for (const auto& entry : resolves.database->entries()) {
                if (entry.database_oid == components::catalog::INVALID_OID) {
                    return true;
                }
            }
        }
        return false;
    }

    const components::logical_plan::resolved_type_metadata_t*
    probe_type_in_path(const catalog_resolves_t& resolves,
                       std::string_view name,
                       std::span<const std::string> search_dbnames) {
        for (const auto& db : search_dbnames) {
            if (const auto* md = resolves.type_md(db, name)) {
                return md;
            }
        }
        return nullptr;
    }

    // A cast SPELLED in the query carries only the NAME of its target type -- the transformer has
    // no catalog, so `CAST(x AS oddness_t)` arrives as UNKNOWN("oddness_t"). Resolve it HERE, where
    // the plan-tree index is at hand, so validation and the cast registry only ever see concrete
    // types (the registry could never match an UNKNOWN against its ENUM family entry).
    // A name that resolves to nothing is LEFT ALONE: validation reports the unknown type with the
    // context to say which expression it came from.
    std::vector<std::string> build_type_search_path_str(std::string_view target_dbname) {
        std::vector<std::string> path;
        if (!target_dbname.empty() && target_dbname != "public" && target_dbname != "pg_catalog") {
            path.emplace_back(target_dbname);
        }
        path.emplace_back("public");
        path.emplace_back("pg_catalog");
        return path;
    }

} // namespace services::catalog_resolve

namespace services::dispatcher { namespace {

    // Per-node enrichment worker, recursing through children. Threads a
    // core::error_t through the recursion (no-error state on success).
    // bind_catalog_data has already run over the whole tree, so every node's
    // table_oid() and table_metadata() are stamped by the time we get here;
    // `resolves` supplies only the constraint gathers, which are keyed by oid.
    [[nodiscard]] actor_zeta::unique_future<core::error_t> enrich_node(std::pmr::memory_resource* resource,
                                                                       components::logical_plan::node_ptr root,
                                                                       components::execution_context_t ctx,
                                                                       const catalog_resolves_t* resolves) {
        using namespace components::logical_plan;
        if (!root)
            co_return core::error_t::no_error();
        switch (root->type()) {
            case node_type::insert_t: {
                auto* node = static_cast<node_insert_t*>(root.get());
                enrich_insert_sync(node);
                // FK + CHECK + UNIQUE/PK gathered by operator_resolve_constraint_t
                // (direction=outgoing). No catalog probe here — a pure entry read.
                const auto* md = node->table_metadata();
                const auto* constraints =
                    resolves ? resolves->constraints_for(node->table_oid(), resolve_direction::outgoing) : nullptr;
                if (constraints) {
                    auto fks = constraints->fks;
                    // Resolve child column names → positions in the INSERT chunk.
                    const auto& kt = node->key_translation();
                    for (auto& fk : fks) {
                        for (const auto& col_name : fk.child_col_names) {
                            std::size_t pos = std::numeric_limits<std::size_t>::max();
                            for (std::size_t i = 0; i < kt.size(); ++i) {
                                if (kt[i].as_string() == col_name) {
                                    pos = i;
                                    break;
                                }
                            }
                            fk.child_col_indices.push_back(pos);
                        }
                    }
                    node->set_outgoing_fks(std::move(fks));
                    node->set_check_exprs(constraints->check_exprs);
                    node->set_unique_groups(constraints->unique_constraints);
                    if (!constraints->pk_columns.empty()) {
                        auto nn = node->not_null_cols();
                        merge_pk_not_null(md, constraints->pk_columns, nn, /*include_with_defaults=*/false);
                        node->set_not_null_cols(std::move(nn));
                    }
                }
                if (md != nullptr) {
                    node->set_column_defaults(decode_column_defaults(node->resource(), *md));
                }
                break;
            }
            case node_type::update_t: {
                auto* node = static_cast<node_update_t*>(root.get());
                enrich_update_sync(node);
                const auto* md = node->table_metadata();
                const auto* constraints =
                    resolves ? resolves->constraints_for(node->table_oid(), resolve_direction::outgoing) : nullptr;
                if (constraints) {
                    auto fks = constraints->fks;
                    // Resolve the child column NAMES to their positions, the same way the INSERT
                    // branch does through key_translation(). Handing the node unresolved foreign
                    // keys left child_col_indices empty, and operator_fk_check reads that as "no
                    // key column to address" and skips the row — so every row was skipped, the
                    // qualifying count stayed zero, and zero is its success path.
                    //
                    // An UPDATE is fed the scanned base row, so a child column is at its storage
                    // chunk_position rather than at a position in an INSERT tuple.
                    if (md) {
                        for (auto& fk : fks) {
                            fk.child_col_indices.clear();
                            for (const auto& col_name : fk.child_col_names) {
                                std::size_t pos = std::numeric_limits<std::size_t>::max();
                                for (const auto& col : md->columns) {
                                    if (col.attname == col_name && col.chunk_position >= 0) {
                                        pos = static_cast<std::size_t>(col.chunk_position);
                                        break;
                                    }
                                }
                                fk.child_col_indices.push_back(pos);
                            }
                        }
                    }
                    node->set_outgoing_fks(std::move(fks));
                    node->set_check_exprs(constraints->check_exprs);
                    node->set_unique_groups(constraints->unique_constraints);
                    if (!constraints->pk_columns.empty()) {
                        auto nn = node->not_null_cols();
                        merge_pk_not_null(md, constraints->pk_columns, nn, /*include_with_defaults=*/true);
                        node->set_not_null_cols(std::move(nn));
                    }
                }
                if (md != nullptr) {
                    node->set_column_defaults(decode_column_defaults(node->resource(), *md));
                }
                break;
            }
            case node_type::delete_t: {
                auto* node = static_cast<node_delete_t*>(root.get());
                // Parent table metadata + referencing FK rows are both stamped
                // by operator_resolve_table_t + operator_resolve_constraint_t
                // (direction=referencing). Descendant child column positions
                // and defspecs are pre-populated by the resolve_constraint
                // operator itself — see operator_resolve_constraint.cpp.
                const auto* tbl = node->table_metadata();
                if (tbl) {
                    const auto* constraints =
                        resolves ? resolves->constraints_for(tbl->table_oid, resolve_direction::referencing) : nullptr;
                    if (constraints) {
                        auto fks = constraints->fks;
                        // Resolve parent column positions in the parent table's
                        // attnum-ordered columns (used by operator_fk_cascade
                        // SET NULL / SET DEFAULT to locate FK cols in a fetched
                        // parent row).
                        for (auto& fk : fks) {
                            for (const auto& col_name : fk.parent_col_names) {
                                std::size_t pos = std::numeric_limits<std::size_t>::max();
                                for (std::size_t i = 0; i < tbl->columns.size(); ++i) {
                                    if (tbl->columns[i].attname == col_name) {
                                        pos = i;
                                        break;
                                    }
                                }
                                fk.parent_col_indices.push_back(pos);
                            }
                        }
                        node->set_referencing_fks(std::move(fks));
                    }
                }
                break;
            }
            case node_type::create_collection_t: {
                auto* node = static_cast<node_create_collection_t*>(root.get());
                // Replace the UNKNOWNs a CREATE TABLE spells by name (UDT columns,
                // STRUCT fields, ARRAY/LIST element types) with concrete types, so
                // validation only ever sees resolved ones.
                resolve_column_definitions(node->column_definitions(), resolves);
                break;
            }
            case node_type::create_sequence_t:
            case node_type::create_view_t:
            case node_type::create_macro_t: {
                // namespace_oid pasted by bind_catalog_data; nothing else to do.
                break;
            }
            case node_type::create_index_t: {
                // namespace_oid + table_oid + metadata pasted by bind_catalog_data.
                // Column attoids + indkey still need deriving from the column list.
                auto* node = static_cast<node_create_index_t*>(root.get());
                const auto* tbl = node->table_metadata();
                if (!tbl)
                    break;

                std::vector<components::catalog::oid_t> col_attoids;
                std::string indkey;
                col_attoids.reserve(node->keys().size());
                for (std::size_t i = 0; i < node->keys().size(); ++i) {
                    const std::string cn = node->keys()[i].as_string();
                    components::catalog::oid_t attoid = components::catalog::INVALID_OID;
                    for (const auto& ci : tbl->columns) {
                        if (ci.attname == cn) {
                            attoid = ci.attoid;
                            break;
                        }
                    }
                    col_attoids.push_back(attoid);
                    if (i)
                        indkey += ",";
                    indkey += std::to_string(attoid);
                }
                node->set_column_attoids(std::move(col_attoids));
                node->set_indkey(std::move(indkey));
                break;
            }
            case node_type::create_constraint_t: {
                auto* node = static_cast<node_create_constraint_t*>(root.get());
                const auto* tbl = node->table_metadata();
                if (!tbl)
                    break;

                // Resolve local (child) column names → attoids.
                std::vector<components::catalog::oid_t> fk_attoids;
                for (const auto& col_name : node->local_col_names()) {
                    for (const auto& ci : tbl->columns) {
                        if (ci.attname == col_name) {
                            fk_attoids.push_back(ci.attoid);
                            break;
                        }
                    }
                }
                node->set_fk_col_attoids(std::move(fk_attoids));

                // FK only — resolve referenced table + parent column attoids.
                // ref_table_oid was pasted by bind_catalog_data from the entry
                // naming (ref_dbname, ref_relname).
                if (node->kind() == constraint_kind::foreign_key &&
                    node->ref_table_oid() != components::catalog::INVALID_OID) {
                    const auto* rrt = resolves ? resolves->table_md(node->ref_table_oid()) : nullptr;
                    if (rrt) {
                        std::vector<components::catalog::oid_t> ref_attoids;
                        for (const auto& col_name : node->ref_col_names()) {
                            for (const auto& ci : rrt->columns) {
                                if (ci.attname == col_name) {
                                    ref_attoids.push_back(ci.attoid);
                                    break;
                                }
                            }
                        }
                        node->set_ref_col_attoids(std::move(ref_attoids));
                    }
                }
                break;
            }
            case node_type::alter_table_t: {
                // table_oid + metadata pasted by bind_catalog_data; the planner
                // rewrite only needs relkind (computed-vs-regular routing).
                auto* node = static_cast<node_alter_table_t*>(root.get());
                if (const auto* tbl = node->table_metadata()) {
                    node->set_relkind(tbl->relkind);
                }
                break;
            }
            case node_type::drop_t: {
                // All DROP kinds (incl. index): every OID is pasted by
                // bind_catalog_data; no per-node work in this pass.
                break;
            }
            default:
                break;
        }
        // Recurse into ALL children after the per-type enrichment, regardless
        // of which case ran. DML cases (insert/update/delete) own a match_t /
        // data_t child that itself carries (db, rel) and needs its own
        // table_oid stamp for create_plan_match / scan operators to route to
        // the right storage. The previous pattern (each case's `break;` exits
        // the function without descending) left those sub-nodes at
        // INVALID_OID — DELETE WHERE was then a no-op.
        for (auto& child : root->children()) {
            if (!child)
                continue;
            auto child_err = co_await enrich_node(resource, child, ctx, resolves);
            if (child_err.contains_error()) {
                co_return child_err;
            }
        }
        co_return core::error_t::no_error();
    }

}} // namespace services::dispatcher::

namespace services::dispatcher {

    actor_zeta::unique_future<core::error_t> enrich_plan(std::pmr::memory_resource* resource,
                                                         components::logical_plan::node_ptr root,
                                                         components::execution_context_t ctx,
                                                         const components::logical_plan::catalog_resolves_t* resolves,
                                                         actor_zeta::address_t index_address,
                                                         services::context_storage_t* collections_ctx) {
        if (!root)
            co_return core::error_t::no_error();
        // Paste the resolved OIDs and table metadata onto every node that named a
        // target, so the per-node cases below (and all of validation after them)
        // read the plan rather than the resolve entries.
        if (resolves) {
            bind_catalog_data(root.get(), *resolves);
        }
        auto err = co_await enrich_node(resource, root, ctx, resolves);
        if (err.contains_error()) {
            co_return err;
        }

        if (collections_ctx && index_address != actor_zeta::address_t::empty_address()) {
            // Two-phase: per-table get_indexed_keys + get_indexed_descriptions
            // are independent across tables, so send both queries for every
            // table first, then await and consume. Future i belongs to
            // queried_oids[i] (both pushed by the same loop iteration), so the
            // consume loops below walk the same index sequence the send loop
            // produced.
            std::pmr::vector<actor_zeta::unique_future<std::pmr::vector<components::index::keys_base_storage_t>>>
                keys_futures(resource);
            std::pmr::vector<actor_zeta::unique_future<std::pmr::vector<components::index::index_description_t>>>
                desc_futures(resource);
            std::pmr::vector<components::catalog::oid_t> queried_oids(resource);
            for (auto tbl_oid : root->table_oid_dependencies()) {
                if (tbl_oid == components::catalog::INVALID_OID) {
                    continue;
                }
                queried_oids.push_back(tbl_oid);
                auto [_ik, ikf] =
                    actor_zeta::send(index_address, &index::manager_index_t::get_indexed_keys, ctx.session, tbl_oid);
                keys_futures.push_back(std::move(ikf));
                auto [_id, idf] = actor_zeta::send(index_address,
                                                   &index::manager_index_t::get_indexed_descriptions,
                                                   ctx.session,
                                                   tbl_oid);
                desc_futures.push_back(std::move(idf));
            }
            // Consume PER OID: file each table's key set / descriptions under its
            // own collections_ctx->table_indexes entry — the planner's index
            // accessors (has_index_on / preferred_index_type_for_compare) are
            // oid-keyed, so every scan of a multi-table statement is judged by
            // ITS table's indexes, never by another table's. Also stamp "does
            // this table have an index" onto every node targeting that table:
            // the stamp is what the DML operators (insert/update/delete) read at
            // execution time, where context_storage is out of reach.
            for (std::size_t i = 0; i < keys_futures.size(); ++i) {
                auto keys = co_await std::move(keys_futures[i]);
                catalog_resolve::stamp_table_has_indexes(root.get(), queried_oids[i], !keys.empty());
                collections_ctx->index_info_slot(queried_oids[i]).keys = std::move(keys);
            }
            for (std::size_t i = 0; i < desc_futures.size(); ++i) {
                auto descriptions = co_await std::move(desc_futures[i]);
                collections_ctx->index_info_slot(queried_oids[i]).descriptions = std::move(descriptions);
            }
        }
        co_return core::error_t::no_error();
    }

} // namespace services::dispatcher
