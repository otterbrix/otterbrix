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
#include <components/logical_plan/node_create_schema.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_having.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_refresh_matview.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/planner/view_expansion.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/vector/vector_builders.hpp>
#include <components/vector/vector_operations.hpp>
#include <services/index/manager_index.hpp>

#include <algorithm>
#include <cassert>
#include <limits>
#include <optional>
#include <queue>
#include <string>
#include <unordered_map>

namespace services::dispatcher { namespace {
    using components::logical_plan::catalog_resolves_t;

    // DEFAULT is expanded here, once — the only reader of pg_attribute.attdefspec.
    core::error_t build_insert_fill_list(components::logical_plan::node_insert_t* node,
                                         const components::logical_plan::resolved_table_metadata_t& md) {
        auto* resource = node->resource();
        components::logical_plan::insert_fill_list_t fill(resource);
        std::pmr::vector<uint64_t> slots(resource);
        const auto& bindings = node->column_bindings();
        // A dynamic-schema table's bindings name source positions, not its columns; the storage
        // fills what such a payload lacks by name (data_table_t::widen_by_name).
        if (bindings.empty() || md.relkind == 'g') {
            node->set_fill_list(std::move(fill));
            node->set_column_slots(std::move(slots));
            return core::error_t::no_error();
        }
        slots.reserve(md.columns.size());
        for (std::size_t i = 0; i < md.columns.size(); ++i) {
            const auto& col = md.columns[i];
            auto written = std::find_if(bindings.begin(), bindings.end(), [i](const auto& binding) {
                return binding.target_index == i;
            });
            if (written != bindings.end()) {
                slots.push_back(static_cast<uint64_t>(written - bindings.begin()));
                continue;
            }
            slots.push_back(bindings.size() + fill.size());
            std::optional<components::vector::vector_t> decoded;
            if (col.atthasdefault) {
                if (auto ec = components::catalog::decode_default_spec(resource, col.type, col.attdefspec, decoded);
                    ec.contains_error()) {
                    return ec;
                }
            }
            fill.push_back(components::logical_plan::insert_fill_column_t{
                i,
                std::pmr::string{col.attname.c_str(), resource},
                col.type,
                decoded.has_value() ? std::move(*decoded) : components::vector::make_null(resource, col.type)});
        }
        node->set_fill_list(std::move(fill));
        node->set_column_slots(std::move(slots));
        return core::error_t::no_error();
    }

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
                    core::error_code_t::internal_error,
                    std::pmr::string{"stored CHECK constraint \"" + name + "\" no longer parses: " + text, resource}};
            }
            predicates->emplace_back(name, std::move(parsed.value().expr));
        }
        return core::error_t::no_error();
    }

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
            const auto storage = rebuilt_type.to_physical_type();
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
                    switch (storage) {
                        case components::types::physical_type::INT16:
                            rebuilt.set_value(row, static_cast<int16_t>(scaled.value()));
                            break;
                        case components::types::physical_type::INT32:
                            rebuilt.set_value(row, static_cast<int32_t>(scaled.value()));
                            break;
                        case components::types::physical_type::INT64:
                            rebuilt.set_value(row, static_cast<int64_t>(scaled.value()));
                            break;
                        default:
                            rebuilt.set_value(row, scaled.value());
                            break;
                    }
                }
                chunk.data[column] = std::move(rebuilt);
            }
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

    // A cell that did not share its column's type is cast to the TABLE column's type here (validate stamped the
    // cast), as PostgreSQL's transformInsertRow coerces every row. The column is rebuilt in that type for every
    // chunk, so the binding's cast, resolved against the typed rows, no longer applies.
    core::error_t spend_deferred_cells(components::logical_plan::node_insert_t* node,
                                       core::date::timezone_offset_t session_tz) {
        const auto& cells = node->deferred_cells();
        if (cells.empty()) {
            return core::error_t::no_error();
        }
        auto* resource = node->resource();
        auto* source = node->children().empty() ? nullptr : node->children().front().get();
        if (node->column_bindings().empty() || source == nullptr ||
            source->type() != components::logical_plan::node_type::data_t) {
            return core::error_t(core::error_code_t::internal_error,
                                 std::pmr::string{"INSERT: VALUES cells of differing types cannot be resolved "
                                                  "against the target columns",
                                                  resource});
        }
        auto& chunks = static_cast<components::logical_plan::node_data_t*>(source)->chunks();
        std::pmr::vector<uint64_t> chunk_start(chunks.size() + 1, 0, resource);
        for (std::size_t i = 0; i < chunks.size(); ++i) {
            chunk_start[i + 1] = chunk_start[i] + chunks[i].size();
        }
        const uint64_t total_rows = chunk_start.back();

        using cell_ptrs = std::pmr::vector<const components::logical_plan::insert_deferred_cell_t*>;
        std::pmr::unordered_map<uint64_t, cell_ptrs> by_column(resource);
        for (const auto& cell : cells) {
            if (cell.row >= total_rows || cell.column >= node->column_bindings().size()) {
                return core::error_t(
                    core::error_code_t::internal_error,
                    std::pmr::string{"INSERT: a deferred VALUES cell points outside the VALUES list", resource});
            }
            auto found = by_column.find(cell.column);
            if (found == by_column.end()) {
                found = by_column.emplace(cell.column, cell_ptrs(resource)).first;
            }
            found->second.push_back(&cell);
        }

        const components::graph_execution_context settings{.timezone_offset = session_tz};
        for (const auto& [column, column_cells] : by_column) {
            auto& binding = node->column_bindings()[column];
            auto target_type = binding.target_type;
            const auto first_chunk_type = chunks.front().data[column].type();
            target_type.set_alias(std::string(first_chunk_type.alias()));
            cell_ptrs by_row(total_rows, nullptr, resource);
            for (const auto* cell : column_cells) {
                by_row[cell->row] = cell;
            }
            for (std::size_t ci = 0; ci < chunks.size(); ++ci) {
                auto& chunk = chunks[ci];
                const auto& stored = chunk.data[column];
                const uint64_t count = chunk.size();
                if (stored.type() != first_chunk_type) {
                    return core::error_t(core::error_code_t::internal_error,
                                         std::pmr::string{"INSERT: a VALUES column of differing types spans several "
                                                          "chunks",
                                                          resource});
                }
                std::optional<components::vector::vector_t> casted;
                if (binding.cast) {
                    components::vector::vector_t converted(resource, target_type, count);
                    if (auto error =
                            binding.cast(components::casts::cast_kind::cast, stored, &converted, settings, count);
                        error.contains_error()) {
                        return error;
                    }
                    casted.emplace(std::move(converted));
                }
                components::vector::vector_t rebuilt(resource, target_type, chunk.capacity());
                for (uint64_t row = 0; row < count; ++row) {
                    const auto* cell = by_row[chunk_start[ci] + row];
                    if (cell != nullptr) {
                        if (cell->cast) {
                            components::vector::vector_t cell_value(resource, target_type, 1);
                            auto error = cell->cast(components::casts::cast_kind::cast,
                                                    components::vector::make_flat_copy(resource, cell->value, 1),
                                                    &cell_value,
                                                    settings,
                                                    1);
                            if (error.contains_error()) {
                                return error;
                            }
                            components::vector::vector_ops::copy(cell_value, rebuilt, 1, 0, row);
                        } else {
                            components::vector::vector_ops::copy(cell->value, rebuilt, 1, 0, row);
                        }
                    } else if (stored.is_null(row)) {
                        rebuilt.set_null(row, true);
                    } else {
                        components::vector::vector_ops::copy(casted ? *casted : stored, rebuilt, row + 1, row, row);
                    }
                }
                chunk.data[column] = std::move(rebuilt);
            }
            binding.cast = {};
            if (source->has_output_types()) {
                auto declared = source->output_types();
                if (column < declared.size()) {
                    declared[column] = target_type;
                    source->set_output_types(std::move(declared));
                }
            }
        }
        return core::error_t::no_error();
    }

    void enrich_insert_sync(components::logical_plan::node_insert_t* node) {
        const auto* md = node->table_metadata();
        if (!md)
            return;
        node->set_array_size_reqs(collect_array_size_reqs(*md));
    }

    void enrich_update_sync(components::logical_plan::node_update_t* node) {
        const auto* md = node->table_metadata();
        if (!md)
            return;
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
            components::catalog::oid_t database_oid() const noexcept { return entry->database_oid; }
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

        // What a node names: its table (or schema, or database) as its target spells it, the second relation it
        // names (DROP/CREATE INDEX: the index; a FOREIGN KEY: the referenced table) and a type.
        struct target_names_t {
            qualified_name_t table{};
            qualified_name_t secondary{};
            std::string_view type_name{};
        };

        target_names_t target_names_of(const components::logical_plan::node_t* node) {
            using namespace components::logical_plan;
            const auto& target = node->target();
            target_names_t names{.table = target};
            switch (node->type()) {
                case node_type::drop_t: {
                    const auto* d = static_cast<const node_drop_t*>(node);
                    if (d->kind() == drop_target_kind::type) {
                        names.type_name = target.collection.t;
                        names.table.collection = core::relname_t{};
                    } else if (!d->index_name().t.empty()) {
                        names.secondary = qualified_name_t{target.connection,
                                                           target.database,
                                                           target.schema,
                                                           core::relname_t{d->index_name().t}};
                    }
                    return names;
                }
                case node_type::create_type_t:
                    names.type_name = static_cast<const node_create_type_t*>(node)->type().type_name();
                    return names;
                case node_type::create_view_t: {
                    // CREATE OR REPLACE finds the view it replaces in the schema it creates in.
                    const auto* view = static_cast<const node_create_view_t*>(node);
                    if (view->replace()) {
                        names.secondary = qualified_name_t{target.connection,
                                                           target.database,
                                                           target.schema,
                                                           core::relname_t{view->viewname().t}};
                    }
                    return names;
                }
                case node_type::create_index_t:
                    names.secondary = qualified_name_t{
                        target.connection,
                        target.database,
                        target.schema,
                        core::relname_t{static_cast<const node_create_index_t*>(node)->indexname().t}};
                    return names;
                case node_type::create_constraint_t:
                    names.secondary = static_cast<const node_create_constraint_t*>(node)->ref();
                    return names;
                case node_type::aggregate_t:
                case node_type::match_t:
                case node_type::insert_t:
                case node_type::update_t:
                case node_type::delete_t:
                case node_type::create_database_t:
                case node_type::create_schema_t:
                case node_type::create_collection_t:
                case node_type::create_sequence_t:
                case node_type::create_macro_t:
                case node_type::alter_table_t:
                case node_type::refresh_matview_t:
                    return names;
                default:
                    return {};
            }
        }

        std::optional<components::logical_plan::resolve_direction>
        constraint_direction_of(components::logical_plan::node_type type) noexcept {
            using components::logical_plan::node_type;
            using components::logical_plan::resolve_direction;
            switch (type) {
                case node_type::insert_t:
                case node_type::update_t:
                    return resolve_direction::outgoing;
                case node_type::delete_t:
                    return resolve_direction::referencing;
                default:
                    return std::nullopt;
            }
        }

        // Breadth-first over the plan: every node once, a node before its children.
        template<typename Node, typename Visit>
        void for_each_node(Node* root, Visit&& visit) {
            std::queue<Node*> q;
            q.push(root);
            while (!q.empty()) {
                auto* n = q.front();
                q.pop();
                visit(n);
                for (const auto& child : n->children()) {
                    if (child) {
                        q.push(child.get());
                    }
                }
            }
        }
    } // namespace

    void stamp_table_has_indexes(components::logical_plan::node_t* root,
                                 components::catalog::oid_t table_oid,
                                 bool has_indexes) {
        using namespace components::logical_plan;
        if (root == nullptr || table_oid == components::catalog::INVALID_OID) {
            return;
        }
        for_each_node(root, [table_oid, has_indexes](node_t* n) {
            if (n->table_oid() == table_oid) {
                n->set_table_has_indexes(has_indexes);
            }
        });
    }

    // A DELETE's index mirror removes the old entries by value, so we have to read every key column.
    void stamp_index_key_columns(components::logical_plan::node_t* root,
                                 components::catalog::oid_t table_oid,
                                 const std::pmr::vector<components::index::keys_base_storage_t>& indexes) {
        using namespace components::logical_plan;
        if (root == nullptr || table_oid == components::catalog::INVALID_OID || indexes.empty()) {
            return;
        }
        for_each_node(root, [table_oid, &indexes](node_t* n) {
            if (n->type() != node_type::delete_t || n->table_oid() != table_oid || n->table_metadata() == nullptr) {
                return;
            }
            const auto& columns = n->table_metadata()->columns;
            std::vector<size_t> key_columns;
            for (const auto& keys : indexes) {
                for (const auto& key : keys) {
                    auto name = key.as_string();
                    auto found = std::find_if(columns.begin(), columns.end(), [&name](const auto& column) {
                        return column.attname == name;
                    });
                    if (found == columns.end()) {
                        key_columns.clear();
                        for (size_t column = 0; column < columns.size(); ++column) {
                            key_columns.push_back(column);
                        }
                        static_cast<node_delete_t*>(n)->set_index_key_columns(std::move(key_columns));
                        return;
                    }
                    key_columns.push_back(static_cast<size_t>(found - columns.begin()));
                }
            }
            static_cast<node_delete_t*>(n)->set_index_key_columns(std::move(key_columns));
        });
    }

    void bind_catalog_data(components::logical_plan::node_t* root, const catalog_resolves_t& resolves) {
        using namespace components::logical_plan;
        if (!root)
            return;
        for_each_node(root, [&resolves](node_t* n) {
            const auto names = target_names_of(n);
            {
                const entry_view_t rn{resolves.namespace_entry(names.table)};
                const entry_view_t rt{resolves.table_entry(names.table)};
                const entry_view_t rt_index{resolves.table_entry(names.secondary)};
                const entry_view_t ry{
                    resolves.type_entry(qualified_name_t{names.table.database,
                                                         names.table.schema,
                                                         core::relname_t{std::string{names.type_name}}})};
                // Pasted whole, except relkind='v': a view's oid would make create_plan_match_ scan the empty heap.
                const bool targets_a_view = rt && rt.resolved_metadata().has_value() &&
                                            rt.resolved_metadata()->relkind == components::catalog::relkind::view;
                if (rt && !targets_a_view) {
                    if (rt.table_oid() != components::catalog::INVALID_OID) {
                        n->set_table_oid(rt.table_oid());
                    }
                    if (rt.resolved_metadata().has_value()) {
                        n->set_table_metadata(&*rt.resolved_metadata());
                    }
                }
                {
                    switch (n->type()) {
                        case node_type::drop_t: {
                            auto* d = static_cast<node_drop_t*>(n);
                            switch (d->kind()) {
                                case drop_target_kind::database: {
                                    if (const auto* database = resolves.database_entry(names.table.connection.t,
                                                                                       names.table.database.t)) {
                                        d->set_database_oid(database->database_oid);
                                    }
                                    break;
                                }
                                case drop_target_kind::schema: {
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
                                case drop_target_kind::materialized_view:
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
                        case node_type::create_schema_t: {
                            if (rn) {
                                static_cast<node_create_schema_t*>(n)->set_database_oid(rn->database_oid());
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
        });
    }

    void register_plan_targets(std::pmr::memory_resource* resource,
                               const components::logical_plan::node_t* root,
                               catalog_resolves_t* resolves) {
        using namespace components::logical_plan;
        if (!root || !resolves) {
            return;
        }
        for_each_node(root, [resource, resolves](const node_t* n) {
            const auto names = target_names_of(n);
            components::sql::transform::register_catalog_resolve_namespace(resource, resolves, names.table);
            if (n->type() == node_type::create_database_t || n->type() == node_type::drop_t) {
                components::sql::transform::register_catalog_resolve_database(resource,
                                                                              resolves,
                                                                              names.table.connection.t,
                                                                              names.table.database.t);
            }
            std::size_t primary_index = resolve_entry_t::no_target;
            if (!names.table.collection.t.empty()) {
                resolve_entry_t entry;
                entry.connection = names.table.connection.t;
                entry.database = names.table.database.t;
                entry.schema = names.table.schema.t;
                entry.relname = names.table.collection.t;
                primary_index = resolves->ensure(resource, resolve_kind::table).add(std::move(entry));
                const auto direction = constraint_direction_of(n->type());
                if (direction.has_value()) {
                    resolve_entry_t constraint_entry;
                    constraint_entry.target = primary_index;
                    constraint_entry.direction = *direction;
                    resolves->ensure(resource, resolve_kind::constraint).add(std::move(constraint_entry));
                }
            }
            if (!names.secondary.collection.t.empty()) {
                resolve_entry_t entry;
                entry.connection = names.secondary.connection.t;
                entry.database = names.secondary.database.t;
                entry.schema = names.secondary.schema.t;
                entry.relname = names.secondary.collection.t;
                // An unqualified REFERENCES target lives where the table owning the key is found.
                if (n->type() == node_type::create_constraint_t && names.secondary.schema.t.empty()) {
                    entry.namespace_of = primary_index;
                }
                resolves->ensure(resource, resolve_kind::table).add(std::move(entry));
            }
            if (!names.type_name.empty()) {
                resolve_entry_t entry;
                entry.database = names.table.database.t;
                entry.schema = names.table.schema.t;
                entry.type_name = names.type_name;
                resolves->ensure(resource, resolve_kind::type).add(std::move(entry));
            }
        });
    }

    // PostgreSQL 18 (namespace.c, RangeVarGetRelidExtended) refuses a relation of another database before looking it
    // up; here only a FOREIGN KEY may not cross databases.
    core::error_t refuse_cross_database_references(std::pmr::memory_resource* resource,
                                                   const catalog_resolves_t& resolves) {
        // A database is named by its connection and name; a name without a database is in the place's.
        const auto database_of = [&resolves](const qualified_name_t& written) {
            return written.database.t.empty()
                       ? std::pair<std::string, std::string>{resolves.place.connection, resolves.place.database}
                       : std::pair<std::string, std::string>{written.connection.t, written.database.t};
        };
        for (const auto& [owner, target] : resolves.referenced_tables) {
            if (target.schema.t.empty() && target.connection.t.empty()) {
                continue;
            }
            if (database_of(target) == database_of(owner)) {
                continue;
            }
            std::pmr::string msg{"cross-database references are not implemented: \"", resource};
            msg += target.to_string();
            msg += '"';
            return core::error_t{core::error_code_t::unimplemented_yet, std::move(msg)};
        }
        return core::error_t::no_error();
    }

    core::error_t refuse_stale_pins(std::pmr::memory_resource* resource, const catalog_resolves_t& resolves) {
        if (!resolves.tables) {
            return core::error_t::no_error();
        }
        for (const auto& entry : resolves.tables->entries()) {
            if (entry.pin.kind == components::logical_plan::view_pin_t::kind_t::relation &&
                !entry.table_md.has_value()) {
                return components::planner::view_stale_error(resource,
                                                             entry.pin.view.t,
                                                             "the relation its body was bound to (\"" + entry.relname +
                                                                 "\", oid " + std::to_string(entry.pin.oid) +
                                                                 ") no longer exists");
            }
        }
        return core::error_t::no_error();
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

    [[nodiscard]] core::error_t resolve_constraint_columns(std::pmr::memory_resource* resource,
                                                           components::logical_plan::node_create_constraint_t* node,
                                                           const constraint_table_view_t* local,
                                                           const constraint_table_view_t* referenced) {
        using components::logical_plan::constraint_kind;
        auto describe_constraint = [&]() {
            std::string out;
            if (!node->name().t.empty()) {
                out = "constraint \"";
                out += node->name().t;
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
            std::string msg = describe_constraint();
            msg += ": table \"";
            msg += node->target().collection.t;
            msg += "\" carries no resolved metadata";
            return core::error_t(core::error_code_t::internal_error, std::pmr::string{std::move(msg), resource});
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
                return core::error_t(core::error_code_t::undefined_column, std::pmr::string{std::move(msg), resource});
            }
        }
        if (local->attoids_minted) {
            node->set_fk_col_attoids(std::move(fk_attoids));
        }

        if (node->kind() != constraint_kind::foreign_key) {
            return core::error_t::no_error();
        }

        if (referenced == nullptr) {
            std::string msg = describe_constraint();
            msg += ": referenced relation \"";
            msg += node->ref().to_string();
            msg += "\" does not exist";
            return core::error_t(core::error_code_t::table_not_exists, std::pmr::string{std::move(msg), resource});
        }
        if (node->ref_col_names().empty()) {
            if (referenced->pk_columns.empty()) {
                return core::error_t(core::error_code_t::undefined_object,
                                     std::pmr::string{describe_constraint() +
                                                          ": there is no primary key for referenced table \"" +
                                                          std::string(referenced->name) + "\"",
                                                      resource});
            }
            if (node->local_col_names().size() != referenced->pk_columns.size()) {
                return core::error_t(core::error_code_t::invalid_foreign_key,
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
                core::error_code_t::invalid_foreign_key,
                std::pmr::string{describe_constraint() + ": foreign key column count mismatch — " +
                                     std::to_string(node->local_col_names().size()) + " referencing column(s) vs " +
                                     std::to_string(node->ref_col_names().size()) +
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
                return core::error_t(core::error_code_t::undefined_column, std::pmr::string{std::move(msg), resource});
            }
        }
        if (referenced->attoids_minted) {
            node->set_ref_col_attoids(std::move(ref_attoids));
        }
        return core::error_t::no_error();
    }

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
                if (auto ec = spend_literal_digits(node); ec.contains_error()) {
                    co_return ec;
                }
                if (auto ec = spend_deferred_cells(node, ctx.session_tz); ec.contains_error()) {
                    co_return ec;
                }
                const auto* md = node->table_metadata();
                if (md != nullptr) {
                    if (auto ec = build_insert_fill_list(node, *md); ec.contains_error()) {
                        co_return ec;
                    }
                }
                const auto* constraints =
                    resolves ? resolves->constraints_for(node->table_oid(), resolve_direction::outgoing) : nullptr;
                if (constraints) {
                    node->set_outgoing_fks(constraints->fks);
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
                    node->set_unique_keys(constraints->unique_constraints);
                }
                break;
            }
            case node_type::update_t: {
                auto* node = static_cast<node_update_t*>(root.get());
                enrich_update_sync(node);
                const auto* constraints =
                    resolves ? resolves->constraints_for(node->table_oid(), resolve_direction::outgoing) : nullptr;
                if (constraints) {
                    node->set_outgoing_fks(constraints->fks);
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
                    node->set_unique_groups(components::catalog::unique_key_columns(constraints->unique_constraints));
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
                        node->set_referencing_fks(constraints->fks);
                    }
                }
                break;
            }
            case node_type::create_collection_t: {
                auto* node = static_cast<node_create_collection_t*>(root.get());
                resolve_column_definitions(node->column_definitions(), resolves);

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
                    co_return core::error_t(
                        core::error_code_t::unimplemented_yet,
                        std::pmr::string{"constraints are not supported on dynamic-schema (relkind='g') tables: "
                                         "CREATE TABLE declared a constraint but no columns. Constraint "
                                         "enforcement requires stable column attoids.",
                                         resource});
                }
                constraint_table_view_t local;
                local.name = node->target().collection.t;
                local.attoids_minted = false;
                local.columns.reserve(node->column_definitions().size());
                for (const auto& col : node->column_definitions()) {
                    local.columns.push_back(constraint_column_view_t{col.name(), components::catalog::INVALID_OID});
                }
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
                            const auto* rrt = (cstr->ref_table_oid() != components::catalog::INVALID_OID && resolves)
                                                  ? resolves->table_md(cstr->ref_table_oid())
                                                  : nullptr;
                            if (rrt) {
                                if (rrt->relkind == 'g') {
                                    co_return core::error_t(
                                        core::error_code_t::unimplemented_yet,
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
                if (auto ec = resolve_constraint_columns(resource, node, tbl ? &local : nullptr, referenced_ptr);
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
                break;
            }
            case node_type::drop_t: {
                break;
            }
            default:
                break;
        }
        // Recurses regardless of case: skipped, a DML child's table_oid stays unstamped and DELETE WHERE is a no-op.
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
            for (std::size_t i = 0; i < keys_futures.size(); ++i) {
                auto keys = co_await std::move(keys_futures[i]);
                catalog_resolve::stamp_table_has_indexes(root.get(), queried_oids[i], !keys.empty());
                catalog_resolve::stamp_index_key_columns(root.get(), queried_oids[i], keys);
                collections_ctx->indexes.index_info_slot(queried_oids[i]).keys = std::move(keys);
            }
            for (std::size_t i = 0; i < desc_futures.size(); ++i) {
                auto descriptions = co_await std::move(desc_futures[i]);
                collections_ctx->indexes.index_info_slot(queried_oids[i]).descriptions = std::move(descriptions);
            }
        }
        co_return core::error_t::no_error();
    }
} // namespace services::dispatcher
