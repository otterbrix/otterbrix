// Logical plan enrichment.
//
// Runs after SQL parsing and before physical plan generation. Reads the
// plan-tree resolve idx (populated by operator_resolve_*_t) to annotate DML
// nodes with the data they need at execution time:
//   INSERT  — not_null_cols, outgoing FK references, CHECK expressions
//   UPDATE  — not_null_cols, outgoing FK references
//   DELETE  — referencing FKs (for CASCADE / SET NULL / SET DEFAULT)
//   CREATE  — namespace_oid (for catalog registration)
//
// No disk I/O of its own — all catalog metadata comes from the resolve idx
// materialized in-plan by the resolve operators.

#include "enrich_logical_plan.hpp"

#include "plan_resolve_index.hpp"
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
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_matview.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
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

    using services::catalog_resolve::plan_resolve_index_t;
    using services::catalog_resolve::tbl_md_for;
    using services::catalog_resolve::tbl_md_for_oid;

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

    void enrich_insert_sync(components::logical_plan::node_insert_t* node, const plan_resolve_index_t* idx) {
        // Insert node carries only its table_oid (stamped by
        // stamp_drop_oids_from_resolves from the sibling resolve_table);
        // look up table metadata by OID rather than (db, rel) strings.
        if (node->table_oid() == components::catalog::INVALID_OID)
            return;
        const auto* md = tbl_md_for_oid(idx, node->table_oid());
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

    void enrich_update_sync(components::logical_plan::node_update_t* node, const plan_resolve_index_t* idx) {
        // Lookup by table_oid stamped from the sibling resolve_table.
        if (node->table_oid() == components::catalog::INVALID_OID)
            return;
        const auto* md = tbl_md_for_oid(idx, node->table_oid());
        if (!md)
            return;
        std::vector<std::string> nn;
        fill_not_null(*md, nn, /*include_with_defaults=*/true);
        node->set_not_null_cols(std::move(nn));
    }

    void enrich_create_collection_sync(components::logical_plan::node_create_collection_t* node,
                                       const plan_resolve_index_t* /*idx*/) {
        // namespace_oid stamped by stamp_drop_oids_from_resolves from the
        // sibling catalog_resolve_namespace_t; no per-node work here.
        (void) node;
    }

    // Name→OID lookup via the plan-tree index. Returns INVALID_OID on miss;
    // the caller decides whether a miss is fatal. Reads the same metadata
    // pointers gather_plan_resolve_index harvested, keyed by (db, rel).
    components::catalog::oid_t
    lookup_table_oid(const plan_resolve_index_t* idx, std::string_view db, std::string_view rel) {
        const auto* md = tbl_md_for(idx, db, rel);
        return md ? md->table_oid : components::catalog::INVALID_OID;
    }

}} // namespace services::dispatcher::

// Helpers shared between the dispatcher and executor pipelines.
namespace services::catalog_resolve {

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

    // Propagate OIDs from sibling catalog_resolve_* nodes onto their
    // consumer nodes (drop/create/DML/alter) inside each sequence_t.
    // After the resolve operators stamp OIDs on resolve_* nodes via
    // back-pointer, this walker copies them onto the consumers whose name
    // fields are gone. Idempotent — INVALID_OID guards make repeat calls
    // no-ops. Called by the dispatcher (after resolve, before validate) and
    // again defensively by enrich_plan (second call is no-op).
    void stamp_oids_from_resolves(components::logical_plan::node_t* root) {
        using namespace components::logical_plan;
        if (!root)
            return;
        std::queue<node_t*> q;
        q.push(root);
        while (!q.empty()) {
            auto* n = q.front();
            q.pop();
            if (n->type() == node_type::sequence_t) {
                node_catalog_resolve_t* rn = nullptr;
                node_catalog_resolve_t* rt = nullptr;
                node_catalog_resolve_t* rt_index = nullptr;
                node_catalog_resolve_t* ry = nullptr;
                for (const auto& c : n->children()) {
                    if (!c)
                        continue;
                    if (c->type() != node_type::catalog_resolve_t)
                        continue;
                    auto* cr = static_cast<node_catalog_resolve_t*>(c.get());
                    switch (cr->kind()) {
                        case resolve_kind::namespace_:
                            rn = cr;
                            break;
                        case resolve_kind::table:
                            // For DROP INDEX the transformer emits two resolve_table
                            // siblings — the first is the parent table, the second
                            // is the index entry (also a pg_class row).
                            if (!rt) {
                                rt = cr;
                            } else if (!rt_index) {
                                rt_index = cr;
                            }
                            break;
                        case resolve_kind::type:
                            ry = cr;
                            break;
                        default:
                            break;
                    }
                }
                for (const auto& c : n->children()) {
                    if (!c)
                        continue;
                    switch (c->type()) {
                        case node_type::drop_t: {
                            auto* d = static_cast<node_drop_t*>(c.get());
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
                                    if (rt_index && rt_index->table_oid() != components::catalog::INVALID_OID) {
                                        d->set_index_oid(rt_index->table_oid());
                                    }
                                    // Stamp the runtime name used by manager_index_t::drop_index
                                    // (the index actor keys engine entries by (table_oid, name)).
                                    if (rt_index) {
                                        d->set_runtime_index_name(rt_index->relname());
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
                            auto* d = static_cast<node_create_collection_t*>(c.get());
                            if (rn && rn->namespace_oid() != components::catalog::INVALID_OID) {
                                d->set_namespace_oid(rn->namespace_oid());
                            }
                            break;
                        }
                        case node_type::create_sequence_t: {
                            auto* d = static_cast<node_create_sequence_t*>(c.get());
                            if (rn && rn->namespace_oid() != components::catalog::INVALID_OID) {
                                d->set_namespace_oid(rn->namespace_oid());
                            }
                            break;
                        }
                        case node_type::create_view_t: {
                            auto* d = static_cast<node_create_view_t*>(c.get());
                            if (rn && rn->namespace_oid() != components::catalog::INVALID_OID) {
                                d->set_namespace_oid(rn->namespace_oid());
                            }
                            break;
                        }
                        case node_type::create_macro_t: {
                            auto* d = static_cast<node_create_macro_t*>(c.get());
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
                            auto* d = static_cast<node_create_matview_t*>(c.get());
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
                            (void) c;
                            break;
                        }
                        case node_type::create_index_t: {
                            auto* d = static_cast<node_create_index_t*>(c.get());
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
                            auto* d = static_cast<node_create_constraint_t*>(c.get());
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
                            auto* d = static_cast<node_insert_t*>(c.get());
                            if (rt && rt->table_oid() != components::catalog::INVALID_OID) {
                                d->set_table_oid(rt->table_oid());
                            }
                            break;
                        }
                        case node_type::update_t: {
                            auto* d = static_cast<node_update_t*>(c.get());
                            if (rt && rt->table_oid() != components::catalog::INVALID_OID) {
                                d->set_table_oid(rt->table_oid());
                            }
                            break;
                        }
                        case node_type::delete_t: {
                            auto* d = static_cast<node_delete_t*>(c.get());
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
                                c->set_table_oid(rt->table_oid());
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

    // --- SELECT-time view expansion helpers ---

    components::logical_plan::node_catalog_resolve_t* find_first_view_resolve(components::logical_plan::node_t* root) {
        using namespace components::logical_plan;
        if (!root || root->type() != node_type::sequence_t) {
            return nullptr;
        }
        for (auto& c : root->children()) {
            if (!c || c->type() != node_type::catalog_resolve_t) {
                continue;
            }
            auto* rt = static_cast<node_catalog_resolve_t*>(c.get());
            if (rt->kind() != resolve_kind::table) {
                continue;
            }
            const auto& md = rt->resolved_metadata();
            if (md && md->relkind == components::catalog::relkind::view && !md->view_sql.empty()) {
                return rt;
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
        // Fresh plan with its own resolve wrap, typically
        // sequence_t(catalog_resolve_namespace, catalog_resolve_table(t),
        //            aggregate(t, ...)); its resolves still need a resolve round.
        out.had_expansion = true;
        out.expanded_plan = std::move(tr.value().sub_queries.back());
        out.expanded_params = std::move(tr.value().parameters);
        return out;
    }

    std::vector<components::logical_plan::node_ptr>
    extract_unresolved_resolves(components::logical_plan::node_t* root) {
        using namespace components::logical_plan;
        std::vector<node_ptr> out;
        if (!root || root->type() != node_type::sequence_t) {
            return out;
        }
        for (auto& c : root->children()) {
            if (!c)
                continue;
            if (c->type() != node_type::catalog_resolve_t)
                continue;
            auto* r = static_cast<node_catalog_resolve_t*>(c.get());
            switch (r->kind()) {
                case resolve_kind::table:
                    if (r->resolved_metadata().has_value()) {
                        continue; // already resolved (outer plan's resolve)
                    }
                    break;
                case resolve_kind::namespace_:
                    if (r->namespace_oid() != components::catalog::INVALID_OID) {
                        continue; // already resolved
                    }
                    break;
                case resolve_kind::database:
                    if (r->database_oid() != components::catalog::INVALID_OID) {
                        continue; // already resolved
                    }
                    break;
                default:
                    break;
            }
            out.push_back(c);
        }
        return out;
    }

    // === Plan-routing helpers ===

    std::pair<std::string, std::string>
    drop_target_names_from_resolves(const components::logical_plan::node_t* plan_root) {
        using namespace components::logical_plan;
        if (!plan_root || plan_root->type() != node_type::sequence_t) {
            return {};
        }
        std::string db;
        std::string rel;
        for (const auto& c : plan_root->children()) {
            if (!c)
                continue;
            if (c->type() != node_type::catalog_resolve_t)
                continue;
            auto* r = static_cast<const node_catalog_resolve_t*>(c.get());
            if (r->kind() == resolve_kind::namespace_) {
                if (db.empty())
                    db = r->dbname();
            } else if (r->kind() == resolve_kind::table) {
                if (db.empty())
                    db = r->dbname();
                if (rel.empty())
                    rel = r->relname();
            }
        }
        return {std::move(db), std::move(rel)};
    }

    const components::logical_plan::resolved_type_metadata_t*
    probe_type_in_path(const plan_resolve_index_t* idx,
                       std::string_view name,
                       std::span<const std::string> search_dbnames) {
        for (const auto& db : search_dbnames) {
            if (const auto* md = type_md_for(idx, db, name))
                return md;
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
    // core::error_t through the recursion (no-error state on success). idx is
    // the executor-built plan-tree resolve index; never null here (enrich_plan
    // is the only caller and it requires a non-null idx).
    [[nodiscard]] actor_zeta::unique_future<core::error_t>
    enrich_node(std::pmr::memory_resource* resource,
                components::logical_plan::node_ptr root,
                components::execution_context_t ctx,
                const services::catalog_resolve::plan_resolve_index_t* idx) {
        using namespace components::logical_plan;
        if (!root)
            co_return core::error_t::no_error();
        // Stamp table_oid for any SELECT-side consumer that still carries
        // (db, rel) on the node body (aggregate/match/group/sort/join/limit/
        // having). DML consumers (insert/update/delete) have already been
        // stamped by stamp_drop_oids_from_resolves from their sibling
        // resolve_table inside the wrapping sequence_t.
        {
            std::string_view db;
            std::string_view rel;
            switch (root->type()) {
                case node_type::aggregate_t: {
                    auto* d = static_cast<node_aggregate_t*>(root.get());
                    db = d->dbname().t;
                    rel = d->relname().t;
                    break;
                }
                case node_type::match_t: {
                    auto* d = static_cast<node_match_t*>(root.get());
                    db = d->dbname();
                    rel = d->relname();
                    break;
                }
                case node_type::group_t: {
                    auto* d = static_cast<node_group_t*>(root.get());
                    db = d->dbname();
                    rel = d->relname();
                    break;
                }
                case node_type::sort_t: {
                    auto* d = static_cast<node_sort_t*>(root.get());
                    db = d->dbname();
                    rel = d->relname();
                    break;
                }
                case node_type::join_t: {
                    auto* d = static_cast<node_join_t*>(root.get());
                    db = d->dbname();
                    rel = d->relname();
                    break;
                }
                case node_type::limit_t: {
                    auto* d = static_cast<node_limit_t*>(root.get());
                    db = d->dbname();
                    rel = d->relname();
                    break;
                }
                case node_type::having_t: {
                    auto* d = static_cast<node_having_t*>(root.get());
                    db = d->dbname();
                    rel = d->relname();
                    break;
                }
                // Host-extension source: stamp table_oid from the resolved (db, rel)
                // so the catalog identity is available to physgen / execution.
                case node_type::extension_t: {
                    auto* d = static_cast<node_extension_t*>(root.get());
                    db = d->dbname();
                    rel = d->relname();
                    break;
                }
                default:
                    break;
            }
            if (!db.empty() && !rel.empty()) {
                auto resolved_oid = lookup_table_oid(idx, db, rel);
                if (resolved_oid != components::catalog::INVALID_OID) {
                    root->set_table_oid(resolved_oid);
                }
            }
        }
        switch (root->type()) {
            case node_type::insert_t: {
                auto* node = static_cast<node_insert_t*>(root.get());
                enrich_insert_sync(node, idx);
                const auto tbl_oid = node->table_oid();
                // FK + CHECK populated by operator_resolve_constraint_t
                // (direction=outgoing) and gathered into idx. No catalog
                // probe here — pure plan-tree read.
                if (tbl_oid != components::catalog::INVALID_OID && idx) {
                    if (auto it = idx->outgoing_fks_by_oid.find(tbl_oid); it != idx->outgoing_fks_by_oid.end()) {
                        auto fks = it->second;
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
                    }
                    if (auto it = idx->check_exprs_by_oid.find(tbl_oid); it != idx->check_exprs_by_oid.end()) {
                        node->set_check_exprs(it->second);
                    }
                    if (auto it = idx->unique_constraints_by_oid.find(tbl_oid);
                        it != idx->unique_constraints_by_oid.end()) {
                        node->set_unique_groups(it->second);
                    }
                    const auto* md = tbl_md_for_oid(idx, tbl_oid);
                    if (auto it = idx->pk_columns_by_oid.find(tbl_oid);
                        it != idx->pk_columns_by_oid.end() && !it->second.empty()) {
                        auto nn = node->not_null_cols();
                        merge_pk_not_null(md, it->second, nn, /*include_with_defaults=*/false);
                        node->set_not_null_cols(std::move(nn));
                    }
                    if (md != nullptr) {
                        node->set_column_defaults(decode_column_defaults(node->resource(), *md));
                    }
                }
                break;
            }
            case node_type::update_t: {
                auto* node = static_cast<node_update_t*>(root.get());
                enrich_update_sync(node, idx);
                const auto tbl_oid = node->table_oid();
                if (tbl_oid != components::catalog::INVALID_OID && idx) {
                    const auto* md = tbl_md_for_oid(idx, tbl_oid);
                    if (auto it = idx->outgoing_fks_by_oid.find(tbl_oid); it != idx->outgoing_fks_by_oid.end()) {
                        auto fks = it->second;
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
                    }
                    if (auto it = idx->unique_constraints_by_oid.find(tbl_oid);
                        it != idx->unique_constraints_by_oid.end()) {
                        node->set_unique_groups(it->second);
                    }
                    if (auto it = idx->pk_columns_by_oid.find(tbl_oid);
                        it != idx->pk_columns_by_oid.end() && !it->second.empty()) {
                        auto nn = node->not_null_cols();
                        merge_pk_not_null(md, it->second, nn, /*include_with_defaults=*/true);
                        node->set_not_null_cols(std::move(nn));
                    }
                    if (md != nullptr) {
                        node->set_column_defaults(decode_column_defaults(node->resource(), *md));
                    }
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
                const auto* tbl = (node->table_oid() != components::catalog::INVALID_OID)
                                      ? tbl_md_for_oid(idx, node->table_oid())
                                      : nullptr;
                if (tbl && idx) {
                    const auto tbl_oid = tbl->table_oid;
                    if (auto it = idx->referencing_fks_by_oid.find(tbl_oid); it != idx->referencing_fks_by_oid.end()) {
                        auto fks = it->second;
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
                enrich_create_collection_sync(node, idx);
                // resolve_column_definitions takes an explicit plan-tree idx.
                // Build a sub-tree-local plan_resolve_index_t from this node so
                // UDT columns get resolved without thread_local state. (The
                // executor-built `idx` covers the whole plan; gathering from
                // `root` here keeps the original sub-tree scoping.)
                impl::plan_resolve_index_t local_plan_idx;
                impl::gather_plan_resolve_index(root.get(), &local_plan_idx);
                resolve_column_definitions(node->column_definitions(), &local_plan_idx);
                break;
            }
            case node_type::create_sequence_t:
            case node_type::create_view_t:
            case node_type::create_macro_t: {
                // OIDs stamped by stamp_drop_oids_from_resolves from sibling resolve nodes.
                break;
            }
            case node_type::create_index_t: {
                // namespace_oid + table_oid are stamped by stamp_drop_oids_from_resolves
                // from the sibling catalog_resolve_table_t. We still resolve column
                // attoids + indkey here since they need the table's column list.
                auto* node = static_cast<node_create_index_t*>(root.get());
                if (node->table_oid() == components::catalog::INVALID_OID)
                    break;
                const auto* tbl = tbl_md_for_oid(idx, node->table_oid());
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
                // idx provides ns/table metadata for the target. The FK
                // reference table (when constraint is FK) needs a separate
                // resolve_table emitted by the transformer.
                auto* node = static_cast<node_create_constraint_t*>(root.get());
                const std::string& ns_name = node->dbname();
                const auto* tbl = tbl_md_for(idx, ns_name, node->relname());
                if (!tbl)
                    break;
                node->set_table_oid(tbl->table_oid);

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
                // ref_table_oid was stamped by stamp_drop_oids_from_resolves from
                // the 2nd resolve_table sibling (transformer emits FK ref table).
                if (node->kind() == constraint_kind::foreign_key &&
                    node->ref_table_oid() != components::catalog::INVALID_OID) {
                    const auto* rrt = tbl_md_for_oid(idx, node->ref_table_oid());
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
                // table_oid stamped by stamp_drop_oids_from_resolves from
                // the sibling resolve_table; we only need to look up relkind for
                // the planner rewrite (computed-vs-regular routing).
                auto* node = static_cast<node_alter_table_t*>(root.get());
                if (node->table_oid() != components::catalog::INVALID_OID) {
                    const auto* tbl = tbl_md_for_oid(idx, node->table_oid());
                    if (tbl) {
                        node->set_relkind(tbl->relkind);
                    }
                }
                break;
            }
            case node_type::drop_t: {
                // All DROP kinds (incl. index): OIDs are stamped by
                // stamp_drop_oids_from_resolves at the top of enrich_plan from
                // the sibling resolve nodes; no per-node work in this pass.
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
            auto child_err = co_await enrich_node(resource, child, ctx, idx);
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
                                                         actor_zeta::address_t disk_address,
                                                         components::execution_context_t ctx,
                                                         const services::catalog_resolve::plan_resolve_index_t* idx,
                                                         actor_zeta::address_t index_address,
                                                         services::context_storage_t* collections_ctx) {
        (void) disk_address;
        if (!root)
            co_return core::error_t::no_error();
        // drop_* nodes no longer carry user-typed names; copy OIDs from their
        // sibling catalog_resolve_* nodes inside each sequence_t before the
        // per-node enrich cases run. (The executor already stamps before
        // building `idx`; this is a defensive no-op for already-stamped trees.)
        stamp_oids_from_resolves(root.get());
        auto err = co_await enrich_node(resource, root, ctx, idx);
        if (err.contains_error()) {
            co_return err;
        }

        if (collections_ctx && index_address != actor_zeta::address_t::empty_address()) {
            // Two-phase: per-table get_indexed_keys + get_indexed_descriptions
            // are independent across tables, so send both queries for every
            // table first, then await and consume. collections_ctx fields are
            // overwritten per table (last table wins, as before), so the
            // await order must match the send order; awaiting in the same loop
            // index sequence preserves that.
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
            // Stamp "does this table have an index" onto every node targeting that table, by
            // OID — not from collections_ctx->indexed_keys, which is overwritten per table
            // (last table wins). A multi-table statement would otherwise judge its DML target
            // by another table's index set.
            std::size_t oid_pos = 0;
            for (auto& ikf : keys_futures) {
                auto keys = co_await std::move(ikf);
                const bool has_indexes = !keys.empty();
                if (oid_pos < queried_oids.size()) {
                    catalog_resolve::stamp_table_has_indexes(root.get(), queried_oids[oid_pos], has_indexes);
                }
                ++oid_pos;
                collections_ctx->indexed_keys = std::move(keys);
            }
            for (auto& idf : desc_futures) {
                collections_ctx->indexed_descriptions = co_await std::move(idf);
            }
        }
        co_return core::error_t::no_error();
    }

} // namespace services::dispatcher
