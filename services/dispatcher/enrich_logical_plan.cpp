// Runs after SQL parsing and before physical plan generation, from the resolve idx operator_resolve_*_t populated.

#include "enrich_logical_plan.hpp"

#include "resolve_type.hpp"

#include <core/executor.hpp>

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

    // DEFAULT-backed NOT NULL columns included too: both write paths hand the operator a row with every column.
    void fill_not_null(const components::logical_plan::resolved_table_metadata_t& md, std::vector<std::string>& out) {
        for (const auto& col : md.columns) {
            if (col.attnotnull) {
                out.push_back(col.attname);
            }
        }
    }

    // ALTER TABLE ADD PRIMARY KEY never back-fills pg_attribute.attnotnull, so PK columns are merged in separately.
    void merge_pk_not_null(const std::vector<std::string>& pk_columns, std::vector<std::string>& not_null) {
        for (const auto& col : pk_columns) {
            if (std::find(not_null.begin(), not_null.end(), col) == not_null.end()) {
                not_null.push_back(col);
            }
        }
    }

    // DEFAULT is expanded here, once — the only reader of pg_attribute.attdefspec. An empty
    // column_bindings list means relkind='g' (dynamic schema): nothing is stamped.
    core::error_t build_insert_fill_list(components::logical_plan::node_insert_t* node,
                                         const components::logical_plan::resolved_table_metadata_t& md) {
        auto* resource = node->resource();
        components::logical_plan::insert_fill_list_t fill(resource);
        if (node->column_bindings().empty()) {
            node->set_fill_list(std::move(fill));
            return core::error_t::no_error();
        }
        for (std::size_t i = 0; i < md.columns.size(); ++i) {
            const auto& col = md.columns[i];
            bool written = false;
            for (const auto& binding : node->column_bindings()) {
                if (binding.target_index == i) {
                    written = true;
                    break;
                }
            }
            if (written) {
                continue;
            }
            std::optional<components::types::logical_value_t> decoded;
            if (col.atthasdefault) {
                // Fails the statement, not "no default" — that would put NULL into an already-cleared column.
                if (auto ec = components::catalog::decode_default_spec(resource, col.type, col.attdefspec, decoded);
                    ec.contains_error()) {
                    return ec;
                }
            }
            fill.push_back(components::logical_plan::insert_fill_column_t{
                std::pmr::string{col.attname.c_str(), resource},
                col.type,
                decoded.has_value()
                    ? std::move(*decoded)
                    : components::types::logical_value_t(
                          resource,
                          components::types::complex_logical_type{components::types::logical_type::NA})});
        }
        node->set_fill_list(std::move(fill));
        return core::error_t::no_error();
    }

    // Order operator_insert hands its constraint operators: statement columns, then DEFAULT
    // fill-list columns. FK columns resolve against this list — an unresolved position silently
    // qualifies 0 rows in operator_fk_check's success path, so `pid bigint DEFAULT 42` inserts unchecked.
    std::vector<std::string> insert_chunk_column_names(const components::logical_plan::node_insert_t* node) {
        std::vector<std::string> names;
        const auto& bindings = node->column_bindings();
        if (!bindings.empty()) {
            names.reserve(bindings.size() + node->fill_list().size());
            for (const auto& binding : bindings) {
                names.emplace_back(binding.target_name.c_str());
            }
        } else {
            names.reserve(node->key_translation().size() + node->fill_list().size());
            for (const auto& key : node->key_translation()) {
                names.emplace_back(key.as_string());
            }
        }
        for (const auto& column : node->fill_list()) {
            names.emplace_back(column.name.c_str());
        }
        return names;
    }

    // A too-short fixed-ARRAY value pads NULL, which NOT NULL can't accept, so it must error before the append.
    std::vector<std::pair<std::string, uint64_t>>
    collect_array_size_reqs(const components::logical_plan::resolved_table_metadata_t& md) {
        std::vector<std::pair<std::string, uint64_t>> array_reqs;
        for (const auto& col : md.columns) {
            if (col.type.type() == components::types::logical_type::ARRAY && col.attnotnull) {
                const auto size =
                    static_cast<const components::types::array_logical_type_extension*>(col.type.extension())->size();
                array_reqs.emplace_back(col.attname, size);
            }
        }
        return array_reqs;
    }

    // CHECK expressions are recorded in the catalog as plain SQL, to avoid explicit expression (de)serialization.
    [[nodiscard]] core::error_t
    parse_check_predicates(std::pmr::memory_resource* resource,
                           const std::vector<std::pair<std::string, std::string>>& stored,
                           std::vector<std::pair<std::string, components::expressions::expression_ptr>>* predicates,
                           components::logical_plan::parameter_node_ptr* params) {
        predicates->clear();
        if (stored.empty()) {
            return core::error_t::no_error();
        }
        *params = components::logical_plan::make_parameter_node(resource);
        components::sql::transform::transformer local(resource);
        for (const auto& [name, text] : stored) {
            auto parsed = local.parse_where_expr(text, *params);
            if (parsed.has_error()) {
                return core::error_t{
                    core::error_code_t::invalid_constraint,
                    std::pmr::string{"stored CHECK constraint \"" + name + "\" no longer parses: " + text, resource}};
            }
            predicates->emplace_back(name, std::move(parsed.value().expr));
        }
        return core::error_t::no_error();
    }

    // Re-reads bare fractional literals at the column's own scale: the transformer, with no
    // catalog at parse time, could only park `0.1` as a double with no scale to honor.
    core::error_t spend_literal_digits(components::logical_plan::node_insert_t* node) {
        using components::types::logical_type;
        const auto& digits = node->literal_digits();
        if (digits.empty() || node->column_bindings().empty() || node->children().empty()) {
            return core::error_t::no_error();
        }
        auto* source = node->children().front().get();
        if (source->type() != components::logical_plan::node_type::data_t) {
            return core::error_t::no_error();
        }
        auto* resource = node->resource();
        auto& chunks = static_cast<components::logical_plan::node_data_t*>(source)->chunks();
        std::vector<uint64_t> chunk_start(chunks.size() + 1, 0);
        for (std::size_t i = 0; i < chunks.size(); ++i) {
            chunk_start[i + 1] = chunk_start[i] + chunks[i].size();
        }
        const uint64_t total_rows = chunk_start.back();

        // Anything outside the chunk batch means the batch is no longer the one the transformer filled.
        std::unordered_map<uint64_t, std::vector<const std::pmr::string*>> by_column;
        for (const auto& record : digits) {
            if (record.row >= total_rows || record.column >= node->column_bindings().size()) {
                return core::error_t::no_error();
            }
            auto& rows = by_column[record.column];
            if (rows.empty()) {
                rows.assign(total_rows, nullptr);
            }
            rows[record.row] = &record.text;
        }

        for (const auto& [column, rows] : by_column) {
            const auto& target_type = node->column_bindings()[column].target_type;
            if (target_type.type() != logical_type::DECIMAL || target_type.extension() == nullptr) {
                continue;
            }
            bool applicable = true;
            for (const auto& chunk : chunks) {
                if (column >= chunk.data.size() || chunk.data[column].type().type() != logical_type::DOUBLE) {
                    applicable = false;
                    break;
                }
            }
            for (std::size_t ci = 0; applicable && ci < chunks.size(); ++ci) {
                const auto& stored = chunks[ci].data[column];
                for (uint64_t row = 0; row < chunks[ci].size(); ++row) {
                    if (!stored.is_null(row) && rows[chunk_start[ci] + row] == nullptr) {
                        applicable = false;
                        break;
                    }
                }
            }
            if (!applicable) {
                continue;
            }
            const auto* decimal =
                static_cast<const components::types::decimal_logical_type_extension*>(target_type.extension());
            auto rebuilt_type = target_type;
            rebuilt_type.set_alias(std::string(chunks.front().data[column].type().alias()));
            const bool narrow = rebuilt_type.to_physical_type() == components::types::physical_type::INT64;
            for (std::size_t ci = 0; ci < chunks.size(); ++ci) {
                auto& chunk = chunks[ci];
                components::vector::vector_t rebuilt(resource, rebuilt_type, chunk.capacity());
                for (uint64_t row = 0; row < chunk.size(); ++row) {
                    if (chunk.data[column].is_null(row)) {
                        rebuilt.set_null(row, true);
                        continue;
                    }
                    auto scaled = components::sql::transform::parse_exact_decimal(resource,
                                                                                  *rows[chunk_start[ci] + row],
                                                                                  decimal->width(),
                                                                                  decimal->scale());
                    if (scaled.has_error()) {
                        return scaled.error();
                    }
                    rebuilt.set_value(row,
                                      narrow ? components::types::logical_value_t::create_decimal(
                                                   resource,
                                                   rebuilt_type,
                                                   static_cast<int64_t>(scaled.value()))
                                             : components::types::logical_value_t::create_decimal(resource,
                                                                                                  rebuilt_type,
                                                                                                  scaled.value()));
                }
                chunk.data[column] = std::move(rebuilt);
            }
            // The column is now the stored type, so the assignment cast would run a second, lossy conversion.
            node->column_bindings()[column].cast = {};
            if (source->has_output_types()) {
                auto declared = source->output_types();
                if (column < declared.size()) {
                    declared[column] = rebuilt_type;
                    source->set_output_types(std::move(declared));
                }
            }
        }
        return core::error_t::no_error();
    }

    void enrich_insert_sync(components::logical_plan::node_insert_t* node) {
        // bind_catalog_data already pasted the target's metadata onto the node.
        const auto* md = node->table_metadata();
        if (!md)
            return;
        std::vector<std::string> nn;
        fill_not_null(*md, nn);
        node->set_not_null_cols(std::move(nn));

        node->set_array_size_reqs(collect_array_size_reqs(*md));
    }

    void enrich_update_sync(components::logical_plan::node_update_t* node) {
        const auto* md = node->table_metadata();
        if (!md)
            return;
        std::vector<std::string> nn;
        fill_not_null(*md, nn);
        node->set_not_null_cols(std::move(nn));
        node->set_array_size_reqs(collect_array_size_reqs(*md));
    }

}} // namespace services::dispatcher::

namespace services::catalog_resolve {

    using components::logical_plan::catalog_resolves_t;
    using components::logical_plan::resolve_entry_t;

    namespace {

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
            // Database the secondary relation lives in; empty means same as dbname.
            std::string_view secondary_dbname{};
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
                    // The name being created is itself a target, or a second CREATE TABLE writes an ambiguous row.
                    return {d->dbname(), d->relname(), {}};
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
                    return {d->dbname(), d->relname(), d->name()};
                }
                case node_type::alter_table_t: {
                    const auto* d = static_cast<const node_alter_table_t*>(node);
                    return {d->dbname(), d->relname(), {}};
                }
                case node_type::create_constraint_t: {
                    const auto* d = static_cast<const node_create_constraint_t*>(node);
                    return {d->dbname(), d->relname(), d->ref_relname(), {}, {}, d->ref_dbname()};
                }
                case node_type::create_matview_t: {
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

    // Supports only single-table FROM with scalar_type::get_field expressions; returns empty on
    // any other shape, which the planner surfaces as an error — not a fallback.
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
        // The transformer routes the target list to GROUP and leaves select empty, so group is where columns live.
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
            // A grouping key is not an output column of its own — the target list names it separately.
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
            const std::string col_name(key_storage.back().c_str(), key_storage.back().size());
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

    // Walks the whole tree by oid rather than position: the DML target isn't necessarily the last one resolved.
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
                const auto names = target_names_of(n);
                const entry_view_t rn{
                    resolves.namespace_entry(names.namespace_dbname.empty() ? names.dbname : names.namespace_dbname)};
                const entry_view_t rt{resolves.table_entry(names.dbname, names.relname)};
                const entry_view_t rt_index{resolves.table_entry(
                    names.secondary_dbname.empty() ? names.dbname : names.secondary_dbname,
                    names.secondary_relname)};
                const entry_view_t ry{resolves.type_entry(names.dbname, names.type_name)};
                // Pasted whole, except relkind='v': a view's oid would make create_plan_match_ scan the empty heap.
                const bool targets_a_view =
                    rt && rt.resolved_metadata().has_value() &&
                    rt.resolved_metadata().value().relkind == components::catalog::relkind::view;
                if (rt && !targets_a_view) {
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
                                    // Name -> indexrelid resolved only here; rt_index's table_oid is the index's oid.
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
                            // mv_oid + view_sql already ride on rt's resolved_metadata (relkind='m').
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
                            // A hit means the name is taken (by an index or table); a miss means it's free.
                            if (rt_index && rt_index->table_oid() != components::catalog::INVALID_OID) {
                                d->set_name_conflict_oid(rt_index->table_oid());
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
                        // A FROM/USING source is a child sub-plan that self-resolves via this same walk.
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
                        // alter_column_* cases already set their own table_oid; re-stamping is a no-op for them.
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
            const auto secondary_dbname = names.secondary_dbname.empty() ? names.dbname : names.secondary_dbname;
            for (const auto& [db, relname] : {std::pair{names.dbname, names.relname},
                                              std::pair{secondary_dbname, names.secondary_relname}}) {
                if (relname.empty()) {
                    continue;
                }
                resolve_entry_t entry;
                entry.dbname = db;
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

    // A cast spelled in the query carries only its target type's name; unresolved is left for validation to report.
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

    struct constraint_column_view_t {
        std::string_view name;
        components::catalog::oid_t attoid{components::catalog::INVALID_OID};
    };

    struct constraint_table_view_t {
        std::string_view name;
        std::vector<constraint_column_view_t> columns;
        std::vector<std::string> pk_columns;
        // False for a table created by this same statement: rewrite_create_table mints attoids later.
        bool attoids_minted{true};
    };

    constraint_table_view_t table_view_of(const components::logical_plan::resolved_table_metadata_t& md) {
        constraint_table_view_t out;
        out.name = md.name;
        out.columns.reserve(md.columns.size());
        for (const auto& ci : md.columns) {
            out.columns.push_back(constraint_column_view_t{ci.attname, ci.attoid});
        }
        return out;
    }

    // An unresolvable constraint is refused, never trimmed — a shorter list would silently enforce a different one.
    [[nodiscard]] core::error_t resolve_constraint_columns(std::pmr::memory_resource* resource,
                                                           components::logical_plan::node_create_constraint_t* node,
                                                           const constraint_table_view_t* local,
                                                           const constraint_table_view_t* referenced) {
        using components::logical_plan::constraint_kind;
        auto describe_constraint = [&]() {
            std::string out;
            if (!node->name().empty()) {
                out = "constraint \"";
                out += node->name();
                out += "\"";
                return out;
            }
            switch (node->kind()) {
                case constraint_kind::primary_key:
                    return std::string{"PRIMARY KEY constraint"};
                case constraint_kind::unique:
                    return std::string{"UNIQUE constraint"};
                case constraint_kind::foreign_key:
                    return std::string{"FOREIGN KEY constraint"};
                case constraint_kind::check:
                    return std::string{"CHECK constraint"};
                default:
                    return std::string{"constraint"};
            }
        };
        if (local == nullptr) {
            // Unreachable through SQL, but the last line of defence against a pg_constraint row nailed to no table.
            std::string msg = describe_constraint();
            msg += ": table \"";
            msg += node->relname();
            msg += "\" carries no resolved metadata";
            return core::error_t(core::error_code_t::invalid_constraint, std::pmr::string{std::move(msg), resource});
        }

        // Every declared column name must resolve — a miss refuses, since conkey is read positionally from here on.
        std::vector<components::catalog::oid_t> fk_attoids;
        fk_attoids.reserve(node->local_col_names().size());
        for (const auto& col_name : node->local_col_names()) {
            bool found = false;
            for (const auto& ci : local->columns) {
                if (ci.name == col_name) {
                    fk_attoids.push_back(ci.attoid);
                    found = true;
                    break;
                }
            }
            if (!found) {
                std::string msg = describe_constraint();
                msg += ": column \"";
                msg += col_name;
                msg += "\" does not exist in table \"";
                msg.append(local->name);
                msg += "\"";
                return core::error_t(core::error_code_t::invalid_constraint,
                                     std::pmr::string{std::move(msg), resource});
            }
        }
        if (local->attoids_minted) {
            node->set_fk_col_attoids(std::move(fk_attoids));
        }

        if (node->kind() != constraint_kind::foreign_key) {
            return core::error_t::no_error();
        }

        // An unresolved referenced table refuses here: PostgreSQL answers "relation does not exist", and so does this.
        if (referenced == nullptr) {
            std::string msg = describe_constraint();
            msg += ": referenced relation \"";
            if (!node->ref_dbname().empty()) {
                msg += node->ref_dbname();
                msg += ".";
            }
            msg += node->ref_relname();
            msg += "\" does not exist";
            return core::error_t(core::error_code_t::invalid_constraint, std::pmr::string{std::move(msg), resource});
        }
        // Omitted column list binds to the parent's PRIMARY KEY; left empty, orphans go in and
        // ON DELETE RESTRICT lets the parent go too.
        if (node->ref_col_names().empty()) {
            if (referenced->pk_columns.empty()) {
                return core::error_t(core::error_code_t::invalid_constraint,
                                     std::pmr::string{describe_constraint() +
                                                          ": there is no primary key for referenced table \"" +
                                                          std::string(referenced->name) + "\"",
                                                      resource});
            }
            if (node->local_col_names().size() != referenced->pk_columns.size()) {
                return core::error_t(
                    core::error_code_t::invalid_constraint,
                    std::pmr::string{describe_constraint() + ": foreign key column count mismatch — " +
                                         std::to_string(node->local_col_names().size()) +
                                         " referencing column(s) vs " +
                                         std::to_string(referenced->pk_columns.size()) +
                                         " column(s) in the primary key of referenced table \"" +
                                         std::string(referenced->name) + "\"",
                                     resource});
            }
            node->set_ref_col_names(referenced->pk_columns);
        }
        // Both lists written and disagreeing: without this, every INSERT/DELETE on the pair is refused at DML time.
        if (node->local_col_names().size() != node->ref_col_names().size()) {
            return core::error_t(
                core::error_code_t::invalid_constraint,
                std::pmr::string{describe_constraint() + ": foreign key column count mismatch — " +
                                     std::to_string(node->local_col_names().size()) +
                                     " referencing column(s) vs " + std::to_string(node->ref_col_names().size()) +
                                     " referenced column(s) in table \"" + std::string(referenced->name) + "\"",
                                 resource});
        }
        std::vector<components::catalog::oid_t> ref_attoids;
        ref_attoids.reserve(node->ref_col_names().size());
        for (const auto& col_name : node->ref_col_names()) {
            bool found = false;
            for (const auto& ci : referenced->columns) {
                if (ci.name == col_name) {
                    ref_attoids.push_back(ci.attoid);
                    found = true;
                    break;
                }
            }
            if (!found) {
                std::string msg = describe_constraint();
                msg += ": column \"";
                msg += col_name;
                msg += "\" does not exist in referenced table \"";
                msg.append(referenced->name);
                msg += "\"";
                return core::error_t(core::error_code_t::invalid_constraint,
                                     std::pmr::string{std::move(msg), resource});
            }
        }
        if (referenced->attoids_minted) {
            node->set_ref_col_attoids(std::move(ref_attoids));
        }
        return core::error_t::no_error();
    }

    // bind_catalog_data has already stamped table_oid()/table_metadata() on every node by now.
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
                // Before the fill list: an exact literal may still overflow its column, and
                // that refusal must land before anything else is stamped onto the node.
                if (auto ec = spend_literal_digits(node); ec.contains_error()) {
                    co_return ec;
                }
                const auto* md = node->table_metadata();
                // The DEFAULT fill list comes first: the FK positions below are chunk positions, which include it.
                if (md != nullptr) {
                    if (auto ec = build_insert_fill_list(node, *md); ec.contains_error()) {
                        co_return ec;
                    }
                }
                const auto* constraints =
                    resolves ? resolves->constraints_for(node->table_oid(), resolve_direction::outgoing) : nullptr;
                if (constraints) {
                    auto fks = constraints->fks;
                    const auto chunk_columns = insert_chunk_column_names(node);
                    for (auto& fk : fks) {
                        for (const auto& col_name : fk.child_col_names) {
                            std::size_t pos = std::numeric_limits<std::size_t>::max();
                            for (std::size_t i = 0; i < chunk_columns.size(); ++i) {
                                if (chunk_columns[i] == col_name) {
                                    pos = i;
                                    break;
                                }
                            }
                            fk.child_col_indices.push_back(pos);
                        }
                    }
                    node->set_outgoing_fks(std::move(fks));
                    node->set_check_exprs(constraints->check_exprs);
                    {
                        std::vector<std::pair<std::string, components::expressions::expression_ptr>> predicates;
                        components::logical_plan::parameter_node_ptr check_params;
                        if (auto error =
                                parse_check_predicates(resource, constraints->check_exprs, &predicates, &check_params);
                            error.contains_error()) {
                            co_return error;
                        }
                        node->set_check_predicates(std::move(predicates));
                        node->set_check_params(std::move(check_params));
                    }
                    node->set_unique_groups(constraints->unique_constraints);
                    if (!constraints->pk_columns.empty()) {
                        auto nn = node->not_null_cols();
                        merge_pk_not_null(constraints->pk_columns, nn);
                        node->set_not_null_cols(std::move(nn));
                    }
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
                    // An UPDATE is fed the scanned base row, so a child column sits at chunk_position, not a tuple pos.
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
                    {
                        std::vector<std::pair<std::string, components::expressions::expression_ptr>> predicates;
                        components::logical_plan::parameter_node_ptr check_params;
                        if (auto error =
                                parse_check_predicates(resource, constraints->check_exprs, &predicates, &check_params);
                            error.contains_error()) {
                            co_return error;
                        }
                        node->set_check_predicates(std::move(predicates));
                        node->set_check_params(std::move(check_params));
                    }
                    node->set_unique_groups(constraints->unique_constraints);
                    if (!constraints->pk_columns.empty()) {
                        auto nn = node->not_null_cols();
                        merge_pk_not_null(constraints->pk_columns, nn);
                        node->set_not_null_cols(std::move(nn));
                    }
                }
                break;
            }
            case node_type::delete_t: {
                auto* node = static_cast<node_delete_t*>(root.get());
                const auto* tbl = node->table_metadata();
                if (tbl) {
                    const auto* constraints =
                        resolves ? resolves->constraints_for(tbl->table_oid, resolve_direction::referencing) : nullptr;
                    if (constraints) {
                        auto fks = constraints->fks;
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
                // Replaces the UNKNOWNs a CREATE TABLE spells by name with concrete types before validation.
                resolve_column_definitions(node->column_definitions(), resolves);

                // Targets the statement's own not-yet-cataloged table: attoids stay unstamped for rewrite_create_table.
                bool has_inline_constraints = false;
                for (const auto& child : root->children()) {
                    if (child && child->type() == node_type::create_constraint_t) {
                        has_inline_constraints = true;
                        break;
                    }
                }
                if (!has_inline_constraints) {
                    break;
                }
                if (node->column_definitions().empty()) {
                    // relkind='g' (dynamic schema): no pg_attribute rows, so a conkey attoid could never be matched.
                    co_return core::error_t(
                        core::error_code_t::schema_error,
                        std::pmr::string{"constraints are not supported on dynamic-schema (relkind='g') tables: "
                                         "CREATE TABLE declared a constraint but no columns. Constraint "
                                         "enforcement requires stable column attoids.",
                                         resource});
                }
                constraint_table_view_t local;
                local.name = node->relname();
                local.attoids_minted = false;
                local.columns.reserve(node->column_definitions().size());
                for (const auto& col : node->column_definitions()) {
                    local.columns.push_back(constraint_column_view_t{col.name(), components::catalog::INVALID_OID});
                }
                // A self-referencing FK that omitted its column list binds to the PK declared in this same statement.
                for (const auto& child : root->children()) {
                    if (!child || child->type() != node_type::create_constraint_t) {
                        continue;
                    }
                    const auto* cstr = static_cast<const node_create_constraint_t*>(child.get());
                    if (cstr->kind() == constraint_kind::primary_key) {
                        local.pk_columns = cstr->local_col_names();
                        break;
                    }
                }
                for (const auto& child : root->children()) {
                    if (!child || child->type() != node_type::create_constraint_t) {
                        continue;
                    }
                    auto* cstr = static_cast<node_create_constraint_t*>(child.get());
                    constraint_table_view_t referenced;
                    const constraint_table_view_t* referenced_ptr = nullptr;
                    if (cstr->kind() == constraint_kind::foreign_key) {
                        if (cstr->self_reference()) {
                            referenced_ptr = &local;
                        } else {
                            const auto* rrt =
                                (cstr->ref_table_oid() != components::catalog::INVALID_OID && resolves)
                                    ? resolves->table_md(cstr->ref_table_oid())
                                    : nullptr;
                            if (rrt) {
                                if (rrt->relkind == 'g') {
                                    co_return core::error_t(
                                        core::error_code_t::schema_error,
                                        std::pmr::string{
                                            "Foreign key constraints are not supported when the referencing or "
                                            "referenced table is dynamic-schema (relkind='g'). FK enforcement "
                                            "requires stable column attoids; dynamic-schema columns may evolve. "
                                            "Convert involved tables to static schema first.",
                                            resource});
                                }
                                referenced = table_view_of(*rrt);
                                const auto* parent_constraints =
                                    resolves->constraints_for(cstr->ref_table_oid(), resolve_direction::outgoing);
                                if (parent_constraints) {
                                    referenced.pk_columns = parent_constraints->pk_columns;
                                }
                                referenced_ptr = &referenced;
                            }
                        }
                    }
                    if (auto ec = resolve_constraint_columns(resource, cstr, &local, referenced_ptr);
                        ec.contains_error()) {
                        co_return ec;
                    }
                }
                break;
            }
            case node_type::create_sequence_t:
            case node_type::create_view_t:
            case node_type::create_macro_t: {
                break;
            }
            case node_type::create_index_t: {
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
                if (node->inline_with_table()) {
                    break;
                }
                const auto* tbl = node->table_metadata();
                constraint_table_view_t local;
                if (tbl) {
                    local = table_view_of(*tbl);
                }
                constraint_table_view_t referenced;
                const constraint_table_view_t* referenced_ptr = nullptr;
                if (node->kind() == constraint_kind::foreign_key) {
                    const auto* rrt = (node->ref_table_oid() != components::catalog::INVALID_OID && resolves)
                                          ? resolves->table_md(node->ref_table_oid())
                                          : nullptr;
                    if (rrt) {
                        referenced = table_view_of(*rrt);
                        const auto* parent_constraints =
                            resolves->constraints_for(node->ref_table_oid(), resolve_direction::outgoing);
                        if (parent_constraints) {
                            referenced.pk_columns = parent_constraints->pk_columns;
                        }
                        referenced_ptr = &referenced;
                    }
                }
                if (auto ec = resolve_constraint_columns(resource,
                                                         node,
                                                         tbl ? &local : nullptr,
                                                         referenced_ptr);
                    ec.contains_error()) {
                    co_return ec;
                }
                break;
            }
            case node_type::alter_table_t: {
                auto* node = static_cast<node_alter_table_t*>(root.get());
                if (const auto* tbl = node->table_metadata()) {
                    node->set_relkind(tbl->relkind);
                }
                // DROP CONSTRAINT: a missing name refuses, except under IF EXISTS (stays INVALID_OID, skipped below).
                if (node->table_oid() != components::catalog::INVALID_OID) {
                    for (auto& sub : node->subcommands()) {
                        if (sub.kind != components::logical_plan::alter_table_kind::drop_constraint) {
                            continue;
                        }
                        const auto* names = resolves ? resolves->constraint_names_for(node->table_oid()) : nullptr;
                        auto found = components::catalog::INVALID_OID;
                        if (names) {
                            for (const auto& [cname, coid] : names->constraint_oids) {
                                if (cname == sub.constraint_name) {
                                    found = coid;
                                    break;
                                }
                            }
                        }
                        if (found == components::catalog::INVALID_OID && !sub.missing_ok) {
                            std::pmr::string msg{resource};
                            msg.append("constraint \"");
                            msg.append(sub.constraint_name.data(), sub.constraint_name.size());
                            msg.append("\" of relation \"");
                            msg.append(node->relname().data(), node->relname().size());
                            msg.append("\" does not exist");
                            co_return core::error_t(core::error_code_t::invalid_constraint, std::move(msg));
                        }
                        sub.constraint_oid = found;
                    }
                }
                break;
            }
            case node_type::drop_t: {
                break;
            }
            default:
                break;
        }
        // Recurses regardless of case: skip this and a DML child's table_oid stays unstamped,
        // so DELETE WHERE silently becomes a no-op.
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
        if (resolves) {
            bind_catalog_data(root.get(), *resolves);
        }
        auto err = co_await enrich_node(resource, root, ctx, resolves);
        if (err.contains_error()) {
            co_return err;
        }

        if (collections_ctx && index_address != actor_zeta::address_t::empty_address()) {
            // Two-phase: both queries sent for every table first, then awaited; future i belongs to queried_oids[i].
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
                auto [_ik, ikf] = actor_zeta::otterbrix::send(index_address,
                                                              &index::manager_index_t::get_indexed_keys,
                                                              ctx.session,
                                                              tbl_oid);
                keys_futures.push_back(std::move(ikf));
                auto [_id, idf] = actor_zeta::otterbrix::send(index_address,
                                                              &index::manager_index_t::get_indexed_descriptions,
                                                              ctx.session,
                                                              tbl_oid);
                desc_futures.push_back(std::move(idf));
            }
            // Also stamps "has an index" per node targeting that table, since DML operators can't
            // reach context_storage at execution time; a stamp gone stale against a concurrent
            // CREATE INDEX is corrected after the append via index_contract::unmirrored_ranges.
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
