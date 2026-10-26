#include <algorithm>
#include <atomic>

#include <components/catalog/type_display.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/jsonb_path.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_insert_on_conflict.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/vector/vector_builders.hpp>
#include <components/vector/vector_operations.hpp>
#include <sql/parser/pg_functions.h>

#include <cassert>
#include <optional>

using namespace components::expressions;

namespace {

    bool is_quoted_literal(Node* node) {
        using components::sql::transform::pg_ptr_cast;
        return nodeTag(node) == T_A_Const && pg_ptr_cast<A_Const>(node)->val.type == T_String;
    }

    core::error_t empty_array_untyped(std::pmr::memory_resource* resource) {
        return core::error_t(core::error_code_t::sql_parse_error,
                             std::pmr::string{"cannot determine type of empty array: "
                                              "explicitly cast it to the desired type, for example ARRAY[]::integer[]",
                                              resource});
    }

    bool array_shapes_differ(const components::types::complex_logical_type& a,
                             const components::types::complex_logical_type& b) {
        using LT = components::types::logical_type;
        const bool a_arrayish = a.type() == LT::ARRAY || a.type() == LT::LIST;
        const bool b_arrayish = b.type() == LT::ARRAY || b.type() == LT::LIST;
        return a_arrayish && b_arrayish && a != b;
    }

    core::result_wrapper_t<components::vector::vector_t>
    promote_array_to_list(std::pmr::memory_resource* resource,
                          const components::vector::vector_t& col,
                          size_t num_rows,
                          const components::types::complex_logical_type& elem_type,
                          uint64_t capacity) {
        auto list_type = components::types::complex_logical_type::create_list(elem_type);
        list_type.set_alias(std::string(col.type().alias()));
        components::vector::vector_t new_col(resource, list_type, capacity);

        const auto stride =
            static_cast<const components::types::array_logical_type_extension*>(col.type().extension())->size();

        const components::vector::vector_t& src_child = col.entry();
        std::optional<components::vector::vector_t> casted;
        const components::vector::vector_t* elems = &src_child;
        if (src_child.type().to_physical_type() != elem_type.to_physical_type()) {
            auto casted_result = components::vector::vector_ops::cast_vector(
                resource,
                components::vector::make_flat_copy(resource, src_child, num_rows * stride),
                elem_type,
                num_rows * stride);
            if (casted_result.has_error()) {
                return casted_result.error();
            }
            casted.emplace(std::move(casted_result.value()));
            elems = &casted.value();
        }

        auto* row_entries = new_col.data<components::types::list_entry_t>();
        new_col.set_list_size(0);
        uint64_t offset = 0;
        for (size_t row = 0; row < num_rows; ++row) {
            if (col.is_null(row)) {
                new_col.set_null(row, true);
                row_entries[row] = components::types::list_entry_t{offset, 0};
                continue;
            }
            new_col.append(*elems, row * stride + stride, row * stride);
            row_entries[row] = components::types::list_entry_t{offset, stride};
            offset += stride;
        }
        new_col.set_list_size(offset);
        return new_col;
    }

    // A numeric cast that overflows is reported the way PostgreSQL words an INSERT overflow.
    core::result_wrapper_t<components::vector::vector_t>
    cast_checked(std::pmr::memory_resource* resource,
                 const components::vector::vector_t& source,
                 const components::types::complex_logical_type& target,
                 uint64_t count) {
        auto casted =
            components::vector::vector_ops::cast_vector(resource,
                                                        components::vector::make_flat_copy(resource, source, count),
                                                        target,
                                                        count);
        if (casted.has_error() && components::types::is_arithmetic_numeric(source.type().type()) &&
            components::types::is_arithmetic_numeric(target.type())) {
            return core::error_t(
                casted.error().type,
                std::pmr::string{components::catalog::format_type_be(target, false) + " out of range", resource});
        }
        return casted;
    }

    core::result_wrapper_t<components::vector::vector_t>
    promote_column(std::pmr::memory_resource* resource,
                   const components::vector::vector_t& col,
                   size_t num_rows,
                   const components::types::complex_logical_type& promoted,
                   uint64_t capacity) {
        auto target = promoted;
        target.set_alias(std::string(col.type().alias()));
#ifdef DEV_MODE
        components::sql::transform::note_promoted_rows(num_rows);
#endif
        if (col.type().type() == components::types::logical_type::NA) {
            // A leading NULL column holds nothing to convert.
            components::vector::vector_t new_col(resource, target, capacity);
            for (size_t row = 0; row < num_rows; ++row) {
                new_col.set_null(row, true);
            }
            return new_col;
        }
        auto widened = cast_checked(resource, col, target, num_rows);
        if (widened.has_error()) {
            return widened.error();
        }
        widened.value().resize(num_rows, capacity);
        return std::move(widened.value());
    }

    // A chunk is always a prefix of the final schema (fill_row only appends/widens in place).
    core::error_t conform_param_chunks(std::pmr::memory_resource* resource,
                                       std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                       const std::pmr::vector<components::types::complex_logical_type>& schema) {
        for (auto& chunk : chunks) {
            for (size_t i = 0; i < schema.size(); ++i) {
                const auto& want = schema[i];
                if (i >= chunk.data.size()) {
                    components::vector::vector_t col(resource, want, chunk.capacity());
                    col.set_null(true);
                    chunk.data.emplace_back(std::move(col));
                } else if (chunk.data[i].type() != want) {
                    VALUE_OR_RETURN(auto promoted_col,
                                    promote_column(resource,
                                                   chunk.data[i],
                                                   chunk.size(),
                                                   components::types::complex_logical_type{want.type()},
                                                   chunk.capacity()));
                    chunk.data[i] = std::move(promoted_col);
                }
            }
        }
        return core::error_t::no_error();
    }

} // anonymous namespace

#ifdef DEV_MODE
namespace components::sql::transform {
    namespace {
        std::atomic<uint64_t> g_insert_promote_rows{0};
    } // namespace
    void note_promoted_rows(uint64_t rows) noexcept {
        g_insert_promote_rows.fetch_add(rows, std::memory_order_relaxed);
    }
    uint64_t insert_promote_rows() noexcept { return g_insert_promote_rows.load(std::memory_order_relaxed); }
    void reset_insert_promote_rows() noexcept { g_insert_promote_rows.store(0, std::memory_order_relaxed); }
} // namespace components::sql::transform
#endif

namespace components::sql::transform {
    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_insert(InsertStmt& node,
                                                                                 logical_plan::execution_plan_t* plan) {
        // A leading WITH must be registered before the inner SELECT so INSERT ... SELECT ... FROM cte works.
        RETURN_IF_ERROR(register_with_ctes(node.withClause));
        const auto& fields = pg_ptr_cast<List>(node.cols)->lst;
        std::pmr::vector<expressions::key_t> key_translation(resource_);
        for (const auto& field : fields) {
            auto target = pg_ptr_cast<ResTarget>(field.data);
            if (target->indirection->lst.empty()) {
                key_translation.emplace_back(resource_, target->name);
            } else {
                // Flatten the whole path (base name + every indirection element) so writes and reads agree.
                std::pmr::vector<std::pmr::string> segments(resource_);
                segments.emplace_back(std::pmr::string{target->name, resource_});
                for (const auto& part : target->indirection->lst) {
                    Node* p = pg_ptr_cast<Node>(part.data);
                    if (nodeTag(p) == T_A_Indices) {
                        VALUE_OR_RETURN(auto segment, indices_to_str(resource_, pg_ptr_cast<A_Indices>(p)));
                        segments.emplace_back(std::move(segment));
                    } else {
                        segments.emplace_back(pmrStrVal(p, resource_));
                    }
                }
                key_translation.emplace_back(resource_, jsonb_path::flatten(segments, resource_));
            }
        }
        std::pmr::vector<expressions::expression_ptr> returning(resource_);
        if (node.returningList) {
            name_collection_t rnames;
            rnames.left_name = rangevar_to_qualified_name(node.relation);
            rnames.left_alias = construct_alias(node.relation->alias);
            VALUE_OR_RETURN(returning, transform_returning(node.returningList, rnames, plan));
        }

        logical_plan::insert_on_conflict_t on_conflict(resource_);
        logical_plan::node_update_ptr conflict_update;
        if (node.onConflictClause) {
            const auto* clause = node.onConflictClause;
            on_conflict.action = clause->action == ONCONFLICT_UPDATE ? logical_plan::on_conflict_action_t::do_update
                                                                     : logical_plan::on_conflict_action_t::do_nothing;
            if (clause->action == ONCONFLICT_UPDATE) {
                auto target = rangevar_to_qualified_name(node.relation);
                name_collection_t names;
                names.left_name = target;
                names.left_alias = construct_alias(node.relation->alias);
                names.right_alias = "excluded";
                VALUE_OR_RETURN(auto updates, transform_set_list(clause->targetList, names, plan));
                expressions::expression_ptr where_expr = make_compare_expression(resource_, compare_type::all_true);
                if (clause->whereClause) {
                    VALUE_OR_RETURN(where_expr, transform_predicate(clause->whereClause, names, plan));
                }
                auto match = logical_plan::make_node_match(resource_, target, where_expr);
                VALUE_OR_RETURN(auto limit, build_dml_limit(nullptr, plan));
                conflict_update = logical_plan::make_node_update(resource_, match, limit, updates);
                conflict_update->set_excluded_alias(names.right_alias);
                set_target(*conflict_update, target, target_slots::relation);
                if (node.returningList) {
                    name_collection_t target_names;
                    target_names.left_name = target;
                    target_names.left_alias = names.left_alias;
                    VALUE_OR_RETURN(conflict_update->returning(),
                                    transform_returning(node.returningList, target_names, plan));
                }
            }
            if (const auto* infer = clause->infer) {
                if (infer->conname) {
                    on_conflict.constraint_name = std::pmr::string{infer->conname, resource_};
                }
                for (const auto& cell : pg_ptr_cast<List>(infer->indexElems)->lst) {
                    const auto* elem = pg_ptr_cast<IndexElem>(cell.data);
                    if (!elem->name || !pg_ptr_cast<List>(elem->collation)->lst.empty() ||
                        !pg_ptr_cast<List>(elem->opclass)->lst.empty() || elem->ordering != SORTBY_DEFAULT ||
                        elem->nulls_ordering != SORTBY_NULLS_DEFAULT) {
                        return core::error_t(
                            core::error_code_t::unimplemented_yet,
                            std::pmr::string{"ON CONFLICT target supports plain column names only", resource_});
                    }
                    on_conflict.target_columns.emplace_back(elem->name);
                }
                if (infer->whereClause) {
                    name_collection_t target_names;
                    target_names.left_name = rangevar_to_qualified_name(node.relation);
                    target_names.left_alias = construct_alias(node.relation->alias);
                    VALUE_OR_RETURN(on_conflict.target_where,
                                    transform_predicate(infer->whereClause, target_names, plan));
                }
            }
        }
        auto with_on_conflict = [&](logical_plan::node_insert_ptr insert) -> logical_plan::node_ptr {
            if (!node.onConflictClause) {
                return insert;
            }
            auto statement = logical_plan::node_insert_on_conflict_ptr{
                new logical_plan::node_insert_on_conflict_t(resource_, std::move(insert))};
            statement->on_conflict() = std::move(on_conflict);
            if (conflict_update) {
                statement->append_child(std::move(conflict_update));
            }
            return statement;
        };

        if (!node.selectStmt) {
            return core::error_t(core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"INSERT ... DEFAULT VALUES is not supported", resource_});
        }
        if (pg_ptr_cast<SelectStmt>(node.selectStmt)->valuesLists) {
            const auto& vals = pg_ptr_cast<List>(pg_ptr_cast<SelectStmt>(node.selectStmt)->valuesLists)->lst;

            // Parameterised INSERT binds by absolute index in transform_result, so it stays one chunk.
            bool has_params = false;
            for (auto row : vals) {
                for (auto value : pg_ptr_cast<List>(row.data)->lst) {
                    if (nodeTag(value.data) == T_ParamRef) {
                        has_params = true;
                        break;
                    }
                }
                if (has_params) {
                    break;
                }
            }

            // Without a column list the values fill the table's columns in order (PostgreSQL 18 checkInsertTargets);
            // here they are named by position, and validation binds them to the table's columns.
            std::pmr::vector<std::string> field_names(resource_);
            if (fields.empty()) {
                const auto width = pg_ptr_cast<List>(vals.front().data)->lst.size();
                field_names.reserve(width);
                for (std::size_t position = 1; position <= width; ++position) {
                    field_names.emplace_back("?column?" + std::to_string(position));
                }
            } else {
                field_names.reserve(key_translation.size());
                for (const auto& field : key_translation) {
                    field_names.emplace_back(field.as_string());
                }
            }

            logical_plan::insert_literal_digits_list_t literal_digits(resource_);
            logical_plan::insert_deferred_cells_t deferred_cells(resource_);

            auto fill_row = [&](vector::data_chunk_t& chunk,
                                size_t chunk_row,
                                size_t global_row,
                                List* values_list) -> core::error_t {
                const auto& values = values_list->lst;
                if (values.size() != field_names.size()) {
                    const char* what = fields.empty() ? "VALUES lists must all be the same length"
                                       : values.size() > field_names.size()
                                           ? "INSERT has more expressions than target columns"
                                           : "INSERT has more target columns than expressions";
                    return core::error_t(core::error_code_t::sql_parse_error, std::pmr::string{what, resource_});
                }

                const auto find_column = [&chunk](const std::string& name) {
                    return std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& column) {
                        return column.type().alias() == name;
                    });
                };

                std::size_t field_pos = 0;
                for (auto it_value = values.begin(); it_value != values.end(); ++it_value, ++field_pos) {
                    const std::string& field_name = field_names[field_pos];
                    if (nodeTag(it_value->data) == T_ParamRef) {
                        auto ref = pg_ptr_cast<ParamRef>(it_value->data);
                        auto loc = std::make_pair(global_row, field_name);

                        if (auto it = parameter_insert_map_.find(static_cast<size_t>(ref->number));
                            it != parameter_insert_map_.end()) {
                            it->second.emplace_back(std::move(loc));
                        } else {
                            std::pmr::vector<insert_location_t> par(resource_);
                            par.emplace_back(std::move(loc));
                            parameter_insert_map_.emplace(ref->number, std::move(par));
                        }
                        // Must exist now, at its written position: downstream pairs columns positionally.
                        auto it_column = find_column(field_name);
                        if (it_column == chunk.data.end()) {
                            vector::vector_t placeholder(
                                resource_,
                                types::complex_logical_type{types::logical_type::NA, field_name},
                                chunk.capacity());
                            placeholder.set_null(true);
                            chunk.data.emplace_back(std::move(placeholder));
                        }
                    } else {
                        auto* written_node = pg_ptr_cast<Node>(it_value->data);
                        auto it = find_column(field_name);
                        if (nodeTag(written_node) == T_A_Const) {
                            Value* literal = &pg_ptr_cast<A_Const>(written_node)->val;
                            const auto tag = nodeTag(literal);
                            if (tag == T_Integer || tag == T_Float || tag == T_String) {
                                numeric_literal_t number;
                                if (tag != T_String) {
                                    VALUE_OR_RETURN(number, parse_numeric_literal(resource_, literal));
                                }
                                const auto literal_type =
                                    tag == T_String ? types::logical_type::STRING_LITERAL : number.type;
                                if (it == chunk.data.end()) {
                                    types::complex_logical_type column_type{literal_type};
                                    column_type.set_alias(field_name);
                                    chunk.data.emplace_back(resource_, std::move(column_type), chunk.capacity());
                                    it = chunk.data.end() - 1;
                                }
                                if (it->type().type() == literal_type) {
                                    switch (literal_type) {
                                        case types::logical_type::BIGINT:
                                            it->set_value(chunk_row, int64_t{number.as_int64});
                                            break;
                                        case types::logical_type::HUGEINT:
                                            it->set_value(chunk_row, types::int128_t{number.as_int128});
                                            break;
                                        case types::logical_type::UHUGEINT:
                                            it->set_value(chunk_row, types::uint128_t{number.as_uint128});
                                            break;
                                        case types::logical_type::DOUBLE:
                                            it->set_value(chunk_row, double{number.as_double});
                                            break;
                                        case types::logical_type::STRING_LITERAL:
                                            it->set_value(chunk_row, std::string_view{strVal(literal)});
                                            break;
                                        default:
                                            assert(false);
                                            break;
                                    }
                                    if (auto digits = fractional_literal_text(written_node); !digits.empty()) {
                                        literal_digits.push_back(logical_plan::insert_literal_digits_t{
                                            global_row,
                                            static_cast<size_t>(it - chunk.data.begin()),
                                            std::pmr::string{digits.data(), digits.size(), resource_}});
                                    }
                                    continue;
                                }
                            }
                        }
                        // TODO: move column matching to validation/optimizer phase for complex path resolution
                        VALUE_OR_RETURN(auto value,
                                        nodeTag(it_value->data) == T_A_Expr
                                            ? evaluate_const_a_expr(resource_, pg_ptr_cast<A_Expr>(it_value->data))
                                            : get_value(resource_, pg_ptr_cast<Node>(it_value->data)));
                        if (it == chunk.data.end()) {
                            auto column_type = value.type();
                            column_type.set_alias(field_name);
                            chunk.data.emplace_back(resource_, std::move(column_type), chunk.capacity());
                            it = chunk.data.end() - 1;
                        }
                        const size_t column_index = static_cast<size_t>(it - chunk.data.begin());
                        auto& column = *it;
                        const auto col_type = column.type().type();
                        const auto val_type = value.type().type();
                        const bool quoted = is_quoted_literal(pg_ptr_cast<Node>(it_value->data));

                        if (value.is_null(0)) {
                            column.set_null(chunk_row, true);
                            continue;
                        }
                        // PostgreSQL coerces each row to the TABLE column's type; a cell that is not of the type
                        // the column took waits for it, and validate/enrich cast it there.
                        const auto defer = [&](vector::vector_t cell, bool was_quoted, uint64_t row) {
                            column.set_null(row - (global_row - chunk_row), true);
                            deferred_cells.push_back(logical_plan::insert_deferred_cell_t{row,
                                                                                          column_index,
                                                                                          was_quoted,
                                                                                          std::move(cell),
                                                                                          {}});
                        };
                        if (col_type == types::logical_type::NA) {
                            // Column came from a leading NULL literal; it takes the first value's whole type.
                            VALUE_OR_RETURN(
                                auto na_promoted,
                                promote_column(resource_, column, chunk_row, value.type(), chunk.capacity()));
                            column = std::move(na_promoted);
                        } else if (column.type() == value.type()) {
                            // Same type: stored in place.
                        } else if (col_type == types::logical_type::STRING_LITERAL) {
                            // The rows so far are text (quoted or cast to text); the column takes this value's type
                            // and the text rows wait for the table column's.
                            auto row_node =
                                std::next(vals.begin(), static_cast<std::ptrdiff_t>(global_row - chunk_row));
                            for (size_t row = 0; row < chunk_row; ++row, ++row_node) {
                                if (column.is_null(row)) {
                                    continue;
                                }
                                auto* written =
                                    pg_ptr_cast<Node>(std::next(pg_ptr_cast<List>(row_node->data)->lst.begin(),
                                                                static_cast<std::ptrdiff_t>(field_pos))
                                                          ->data);
                                if (nodeTag(written) == T_ParamRef) {
                                    // Bind writes this cell; its placeholder is not text to defer.
                                    continue;
                                }
                                const std::string_view text = column.get_value<std::string_view>(row);
                                defer(vector::make_scalar(resource_, text),
                                      is_quoted_literal(written),
                                      global_row - chunk_row + row);
                            }
                            auto retyped_type = value.type();
                            retyped_type.set_alias(std::string(column.type().alias()));
                            vector::vector_t retyped(resource_, std::move(retyped_type), chunk.capacity());
                            for (size_t row = 0; row < chunk_row; ++row) {
                                retyped.set_null(row, true);
                            }
                            column = std::move(retyped);
                        } else if (quoted) {
                            defer(std::move(value), true, global_row);
                            continue;
                        } else if (col_type == types::logical_type::DECIMAL ||
                                   val_type == types::logical_type::DECIMAL) {
                            // DECIMAL has no promotion path; refuse rather than store a wrongly-typed cell.
                            return core::error_t(core::error_code_t::sql_parse_error,
                                                 std::pmr::string{"INSERT cannot mix a DECIMAL value with values of a "
                                                                  "different type in one column of a VALUES list",
                                                                  resource_});
                        } else if (types::is_arithmetic_numeric(col_type) && types::is_arithmetic_numeric(val_type)) {
                            const types::complex_logical_type promoted{types::promote_type(col_type, val_type)};
                            if (promoted.type() != col_type) {
                                VALUE_OR_RETURN(
                                    auto promoted_col,
                                    promote_column(resource_, column, chunk_row, promoted, chunk.capacity()));
                                column = std::move(promoted_col);
                            }
                            if (promoted.type() != val_type) {
                                VALUE_OR_RETURN(auto widened, cast_checked(resource_, value, promoted, 1));
                                value = std::move(widened);
                            }
                        } else if (array_shapes_differ(column.type(), value.type())) {
                            // Rows carry differently shaped arrays; a fixed-ARRAY vector can't hold both.
                            const auto column_element = column.type().child_type();
                            const auto value_element = value.type().child_type();
                            if (column_element.type() == types::logical_type::NA ||
                                value_element.type() == types::logical_type::NA) {
                                return empty_array_untyped(resource_);
                            }
                            auto element = value_element;
                            if (column_element != value_element) {
                                if (!types::is_arithmetic_numeric(column_element.type()) ||
                                    !types::is_arithmetic_numeric(value_element.type())) {
                                    defer(std::move(value), false, global_row);
                                    continue;
                                }
                                element = types::complex_logical_type{
                                    types::promote_type(column_element.type(), value_element.type())};
                            }
                            if (col_type == types::logical_type::ARRAY) {
                                VALUE_OR_RETURN(
                                    auto list_col,
                                    promote_array_to_list(resource_, column, chunk_row, element, chunk.capacity()));
                                column = std::move(list_col);
                            } else if (column_element != element) {
                                defer(std::move(value), false, global_row);
                                continue;
                            }
                            value.set_type_alias(field_name);
                            VALUE_OR_RETURN(auto list_value, promote_array_to_list(resource_, value, 1, element, 1));
                            value = std::move(list_value);
                        } else {
                            defer(std::move(value), false, global_row);
                            continue;
                        }
                        vector::vector_ops::copy(value, column, 1, 0, chunk_row);
                        if (auto digits = fractional_literal_text(pg_ptr_cast<Node>(it_value->data)); !digits.empty()) {
                            literal_digits.push_back(logical_plan::insert_literal_digits_t{
                                global_row,
                                column_index,
                                std::pmr::string{digits.data(), digits.size(), resource_}});
                        }
                    }
                }
                return core::error_t::no_error();
            };

            auto qn = rangevar_to_qualified_name(node.relation);
            // Identity travels via the catalog-resolve wrap; the insert node carries only payload + table_oid().
            logical_plan::node_ptr ins;
            if (has_params) {
                const uint64_t cap = vector::DEFAULT_VECTOR_CAPACITY;
                const uint64_t total = vals.size();
                std::pmr::vector<types::complex_logical_type> schema(resource_);
                auto row_it = vals.begin();
                uint64_t global_row = 0;
                while (global_row < total) {
                    const uint64_t batch = std::min<uint64_t>(cap, total - global_row);
                    vector::data_chunk_t chunk(resource_, schema, batch);
                    chunk.set_cardinality(batch);
                    for (uint64_t chunk_row = 0; chunk_row < batch; ++chunk_row, ++row_it, ++global_row) {
                        RETURN_IF_ERROR(fill_row(chunk, chunk_row, global_row, pg_ptr_cast<List>(row_it->data)));
                    }
                    schema = chunk.types(); // carry the (possibly grown/widened) layout to the next chunk
                    parameter_insert_rows_.emplace_back(std::move(chunk));
                }
                RETURN_IF_ERROR(conform_param_chunks(resource_, parameter_insert_rows_, schema));
                ins = logical_plan::make_node_insert(resource_,
                                                     vector::data_chunk_t(resource_, {}, 0),
                                                     std::move(key_translation));
            } else {
                const uint64_t cap = vector::DEFAULT_VECTOR_CAPACITY;
                const uint64_t total = vals.size();
                std::pmr::vector<vector::data_chunk_t> chunks(resource_);
                auto row_it = vals.begin();
                uint64_t global_row = 0;
                while (global_row < total) {
                    const uint64_t batch = std::min<uint64_t>(cap, total - global_row);
                    vector::data_chunk_t chunk(resource_, {}, batch);
                    chunk.set_cardinality(batch);
                    for (uint64_t chunk_row = 0; chunk_row < batch; ++chunk_row, ++row_it, ++global_row) {
                        RETURN_IF_ERROR(fill_row(chunk, chunk_row, global_row, pg_ptr_cast<List>(row_it->data)));
                    }
                    chunks.emplace_back(std::move(chunk));
                }
                ins = logical_plan::make_node_insert(resource_, std::move(chunks), std::move(key_translation));
            }
            auto* ins_node = static_cast<logical_plan::node_insert_t*>(ins.get());
            ins_node->set_literal_digits(std::move(literal_digits));
            ins_node->set_deferred_cells(std::move(deferred_cells));
            ins_node->returning() = returning;
            set_target(*ins_node, qn, target_slots::relation);
            register_write_target(qn);
            return with_on_conflict(logical_plan::node_insert_ptr{ins_node});
        } else {
            auto qn = rangevar_to_qualified_name(node.relation);
            auto res = logical_plan::make_node_insert(resource_);
            VALUE_OR_RETURN(auto source, transform_select(*pg_ptr_cast<SelectStmt>(node.selectStmt), plan));
            res->append_child(std::move(source));
            res->key_translation() = key_translation;
            res->returning() = returning;
            set_target(*res, qn, target_slots::relation);
            register_write_target(qn);
            return with_on_conflict(std::move(res));
        }
    }
} // namespace components::sql::transform
