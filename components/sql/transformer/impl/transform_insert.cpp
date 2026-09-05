#include <algorithm>
#include <atomic>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/jsonb_path.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/vector/vector_operations.hpp>
#include <sql/parser/pg_functions.h>

#include <optional>

using namespace components::expressions;

namespace {

    // The refusal every unconvertible (source, target) pair below shares. These pairs are
    // outside the promotion oracle's contract — reaching one means promote_type answered a
    // type this ladder cannot climb to. An assert(false) followed by a return of the
    // UNWIDENED VALUE is not enough: under NDEBUG the assert vanishes and a value of the
    // wrong logical type goes into the promoted column vector — the exact silent wrong
    // answer this file exists to keep out.
    core::error_t no_numeric_conversion(std::pmr::memory_resource* resource,
                                        components::types::logical_type source,
                                        components::types::logical_type target) {
        std::pmr::string msg{"INSERT cannot widen a numeric value: no conversion from type#", resource};
        msg += std::to_string(static_cast<int>(source));
        msg += " to type#";
        msg += std::to_string(static_cast<int>(target));
        return core::error_t(core::error_code_t::invalid_parameter, std::move(msg));
    }

    // Converts a numeric logical_value_t to a different numeric type without cast_as (no timezone needed).
    // Both val.type().type() and target must satisfy is_numeric().
    core::result_wrapper_t<components::types::logical_value_t>
    numeric_widen(std::pmr::memory_resource* resource,
                  const components::types::logical_value_t& val,
                  components::types::logical_type target) {
        using LT = components::types::logical_type;
        if (val.type().type() == target) {
            return val;
        }
        bool target_is_float = (target == LT::DOUBLE || target == LT::FLOAT);
        if (target_is_float) {
            double dbl;
            switch (val.type().type()) {
                case LT::BOOLEAN:
                    dbl = static_cast<double>(val.value<bool>());
                    break;
                case LT::TINYINT:
                    dbl = static_cast<double>(val.value<int8_t>());
                    break;
                case LT::SMALLINT:
                    dbl = static_cast<double>(val.value<int16_t>());
                    break;
                case LT::INTEGER:
                    dbl = static_cast<double>(val.value<int32_t>());
                    break;
                case LT::BIGINT:
                    dbl = static_cast<double>(val.value<int64_t>());
                    break;
                case LT::UTINYINT:
                    dbl = static_cast<double>(val.value<uint8_t>());
                    break;
                case LT::USMALLINT:
                    dbl = static_cast<double>(val.value<uint16_t>());
                    break;
                case LT::UINTEGER:
                    dbl = static_cast<double>(val.value<uint32_t>());
                    break;
                case LT::UBIGINT:
                    dbl = static_cast<double>(val.value<uint64_t>());
                    break;
                case LT::FLOAT:
                    dbl = static_cast<double>(val.value<float>());
                    break;
                case LT::HUGEINT:
                    dbl = static_cast<double>(val.value<components::types::int128_t>());
                    break;
                default:
                    return no_numeric_conversion(resource, val.type().type(), target);
            }
            if (target == LT::DOUBLE) {
                return components::types::logical_value_t(resource, dbl);
            }
            return components::types::logical_value_t(resource, static_cast<float>(dbl));
        } else if (components::types::is_unsigned(target)) {
            uint64_t uval;
            switch (val.type().type()) {
                case LT::BOOLEAN:
                    uval = static_cast<uint64_t>(val.value<bool>());
                    break;
                case LT::TINYINT:
                    uval = static_cast<uint64_t>(val.value<int8_t>());
                    break;
                case LT::SMALLINT:
                    uval = static_cast<uint64_t>(val.value<int16_t>());
                    break;
                case LT::INTEGER:
                    uval = static_cast<uint64_t>(val.value<int32_t>());
                    break;
                case LT::BIGINT:
                    uval = static_cast<uint64_t>(val.value<int64_t>());
                    break;
                case LT::UTINYINT:
                    uval = static_cast<uint64_t>(val.value<uint8_t>());
                    break;
                case LT::USMALLINT:
                    uval = static_cast<uint64_t>(val.value<uint16_t>());
                    break;
                case LT::UINTEGER:
                    uval = static_cast<uint64_t>(val.value<uint32_t>());
                    break;
                case LT::UBIGINT:
                    uval = val.value<uint64_t>();
                    break;
                default:
                    return no_numeric_conversion(resource, val.type().type(), target);
            }
            switch (target) {
                case LT::UTINYINT:
                    return components::types::logical_value_t(resource, static_cast<uint8_t>(uval));
                case LT::USMALLINT:
                    return components::types::logical_value_t(resource, static_cast<uint16_t>(uval));
                case LT::UINTEGER:
                    return components::types::logical_value_t(resource, static_cast<uint32_t>(uval));
                case LT::UBIGINT:
                    return components::types::logical_value_t(resource, uval);
                default:
                    return no_numeric_conversion(resource, val.type().type(), target);
            }
        } else if (target == LT::HUGEINT) {
            // The 128-bit rung of the same ladder. It exists because an integer literal past
            // int64 reaches VALUES as a HUGEINT (numeric_literal_value in
            // components/sql/transformer/utils.cpp): mixing one with a narrower row —
            // VALUES (12345678901234567890123456789), (1) — promotes the whole column here.
            // Without this arm promote_type would answer HUGEINT and the switch below would
            // fall to its default, hand back the UNWIDENED value, and trip set_value's type
            // assert on the way into the vector.
            using components::types::int128_t;
            switch (val.type().type()) {
                case LT::BOOLEAN:
                    return components::types::logical_value_t(resource, int128_t{val.value<bool>() ? 1 : 0});
                case LT::TINYINT:
                    return components::types::logical_value_t(resource, int128_t{val.value<int8_t>()});
                case LT::SMALLINT:
                    return components::types::logical_value_t(resource, int128_t{val.value<int16_t>()});
                case LT::INTEGER:
                    return components::types::logical_value_t(resource, int128_t{val.value<int32_t>()});
                case LT::BIGINT:
                    return components::types::logical_value_t(resource, int128_t{val.value<int64_t>()});
                case LT::UTINYINT:
                    return components::types::logical_value_t(resource, int128_t{val.value<uint8_t>()});
                case LT::USMALLINT:
                    return components::types::logical_value_t(resource, int128_t{val.value<uint16_t>()});
                case LT::UINTEGER:
                    return components::types::logical_value_t(resource, int128_t{val.value<uint32_t>()});
                case LT::UBIGINT:
                    return components::types::logical_value_t(resource, int128_t{val.value<uint64_t>()});
                case LT::FLOAT:
                    return components::types::logical_value_t(resource, int128_t{val.value<float>()});
                case LT::DOUBLE:
                    return components::types::logical_value_t(resource, int128_t{val.value<double>()});
                default:
                    return no_numeric_conversion(resource, val.type().type(), target);
            }
        } else {
            // Signed integer target. A HUGEINT source is deliberately absent: every caller
            // passes a target from promote_type, which is never NARROWER than the source, so
            // reaching here with 128 bits in hand would mean the promotion oracle lied — and
            // quietly cutting the value down is the silent wrong answer this file keeps out.
            int64_t ival;
            switch (val.type().type()) {
                case LT::BOOLEAN:
                    ival = static_cast<int64_t>(val.value<bool>());
                    break;
                case LT::TINYINT:
                    ival = static_cast<int64_t>(val.value<int8_t>());
                    break;
                case LT::SMALLINT:
                    ival = static_cast<int64_t>(val.value<int16_t>());
                    break;
                case LT::INTEGER:
                    ival = static_cast<int64_t>(val.value<int32_t>());
                    break;
                case LT::BIGINT:
                    ival = val.value<int64_t>();
                    break;
                case LT::UTINYINT:
                    ival = static_cast<int64_t>(val.value<uint8_t>());
                    break;
                case LT::USMALLINT:
                    ival = static_cast<int64_t>(val.value<uint16_t>());
                    break;
                case LT::UINTEGER:
                    ival = static_cast<int64_t>(val.value<uint32_t>());
                    break;
                case LT::UBIGINT:
                    ival = static_cast<int64_t>(val.value<uint64_t>());
                    break;
                case LT::FLOAT:
                    ival = static_cast<int64_t>(val.value<float>());
                    break;
                case LT::DOUBLE:
                    ival = static_cast<int64_t>(val.value<double>());
                    break;
                default:
                    return no_numeric_conversion(resource, val.type().type(), target);
            }
            switch (target) {
                case LT::BOOLEAN:
                    return components::types::logical_value_t(resource, static_cast<bool>(ival));
                case LT::TINYINT:
                    return components::types::logical_value_t(resource, static_cast<int8_t>(ival));
                case LT::SMALLINT:
                    return components::types::logical_value_t(resource, static_cast<int16_t>(ival));
                case LT::INTEGER:
                    return components::types::logical_value_t(resource, static_cast<int32_t>(ival));
                case LT::BIGINT:
                    return components::types::logical_value_t(resource, ival);
                default:
                    return no_numeric_conversion(resource, val.type().type(), target);
            }
        }
    }

    // True when two array-like (ARRAY/LIST) column types differ in shape — different
    // element type or, for fixed arrays, different declared size — so a single fixed-ARRAY
    // column vector cannot hold both. Such a column is promoted to a variable-length LIST.
    bool array_shapes_differ(const components::types::complex_logical_type& a,
                             const components::types::complex_logical_type& b) {
        using LT = components::types::logical_type;
        const bool a_arrayish = a.type() == LT::ARRAY || a.type() == LT::LIST;
        const bool b_arrayish = b.type() == LT::ARRAY || b.type() == LT::LIST;
        return a_arrayish && b_arrayish && a != b;
    }

    // Converts an ARRAY/LIST logical value into a LIST value with the given element type.
    components::types::logical_value_t to_list_value(std::pmr::memory_resource* resource,
                                                     const components::types::logical_value_t& val,
                                                     const components::types::complex_logical_type& elem_type) {
        if (val.is_null()) {
            return components::types::logical_value_t(
                resource,
                components::types::complex_logical_type{components::types::logical_type::NA});
        }
        return components::types::logical_value_t::create_list(resource, elem_type, val.children());
    }

    // Rebuilds a fixed-ARRAY column vector as a variable-length LIST(elem_type), converting
    // the rows filled so far. Used when a later VALUES row carries a different array shape.
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

        // The fixed-ARRAY child holds row r's elements contiguously at [r*stride, r*stride+stride).
        // Cast that whole support vector to the LIST's element type once (a no-op when the physical
        // types already match), then append each row's slice and point its (offset,length) list entry
        // at it — no per-element logical_value_t round-trip.
        const components::vector::vector_t& src_child = col.entry();
        std::optional<components::vector::vector_t> casted;
        const components::vector::vector_t* elems = &src_child;
        if (src_child.type().to_physical_type() != elem_type.to_physical_type()) {
            // cast_vector range-checks every element; a stored value that does not fit
            // the promoted element type refuses the INSERT instead of truncating silently.
            auto casted_result =
                components::vector::vector_ops::cast_vector(resource, src_child, elem_type, num_rows * stride);
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

    // Promotes an existing column vector to a wider numeric type, converting all stored values.
    // Caller must ensure col.type().type() and promoted are both is_numeric(), and promoted != col.type().type().
    core::result_wrapper_t<components::vector::vector_t> promote_column(std::pmr::memory_resource* resource,
                                                                        const components::vector::vector_t& col,
                                                                        size_t num_rows,
                                                                        components::types::logical_type promoted,
                                                                        uint64_t capacity) {
        components::vector::vector_t new_col(
            resource,
            components::types::complex_logical_type{promoted, std::string(col.type().alias())},
            capacity);
#ifdef DEV_MODE
        components::sql::transform::note_promoted_rows(num_rows);
#endif
        for (size_t row = 0; row < num_rows; ++row) {
            if (col.is_null(row)) {
                new_col.set_null(row, true);
            } else {
                VALUE_OR_RETURN(auto widened, numeric_widen(resource, col.value(row), promoted));
                new_col.set_value(row, std::move(widened));
            }
        }
        return new_col;
    }

    // Aligns every parameterized-INSERT chunk to one shared column layout (`schema`, the
    // single source of truth). Each chunk is born from the running schema and fill_row only
    // appends columns / widens numeric types in place, so a chunk is always a prefix of the
    // final schema: extend it with null columns for any trailing columns it lacks, and widen
    // any column that a later chunk promoted. Columns that are params in every row of a chunk
    // stay null here and are filled in transform_result::bind.
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
                                    promote_column(resource, chunk.data[i], chunk.size(), want.type(),
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
        auto fields = pg_ptr_cast<List>(node.cols)->lst;
        std::pmr::vector<expressions::key_t> key_translation(resource_);
        for (const auto& field : fields) {
            auto target = pg_ptr_cast<ResTarget>(field.data);
            if (target->indirection->lst.empty()) {
                key_translation.emplace_back(resource_, target->name);
            } else {
                // Dotted / subscripted target such as `a.b.c` or `arr[0]`: flatten
                // the WHOLE path — the base name plus EVERY indirection element —
                // into one computing-table column name, through the shared codec so
                // the write side agrees byte-for-byte with the read side. Keeping
                // every interior segment fixes the old two-element build that stored
                // `a.b.c` as "a/c" (dropping the middle); rendering a subscript with
                // indices_to_str fixes the crash where an A_Indices node was
                // dereferenced as a string (`arr[0]` -> "arr/0", not a null deref).
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
        // RETURNING projection (references the target table's columns). Parsed
        // once and attached to whichever insert node this statement lowers to.
        std::pmr::vector<expressions::expression_ptr> returning(resource_);
        if (node.returningList) {
            name_collection_t rnames;
            rnames.left_name = rangevar_to_qualified_name(node.relation);
            rnames.left_alias = construct_alias(node.relation->alias);
            VALUE_OR_RETURN(returning, transform_returning(node.returningList, rnames, plan));
        }

        if (!node.selectStmt) {
            return core::error_t(core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"INSERT ... DEFAULT VALUES is not supported", resource_});
        }
        if (pg_ptr_cast<SelectStmt>(node.selectStmt)->valuesLists) {
            auto vals = pg_ptr_cast<List>(pg_ptr_cast<SelectStmt>(node.selectStmt)->valuesLists)->lst;

            // A parameterised INSERT binds rows by absolute index in transform_result,
            // which materialises them into a single working chunk — so keep one chunk for
            // that path. A literal INSERT splits into ≤DEFAULT_VECTOR_CAPACITY chunks so no
            // oversized data_chunk_t is ever built. Detect the case up front.
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

            // Target column names, materialized ONCE per statement. key_t::as_string() builds a
            // fresh std::string every call, and the column-matching predicate below runs it per
            // candidate column per CELL. The names are fixed for the whole statement; only the
            // values vary.
            std::pmr::vector<std::string> field_names(resource_);
            field_names.reserve(key_translation.size());
            for (const auto& field : key_translation) {
                field_names.emplace_back(field.as_string());
            }

            // Fills one row of `chunk` at chunk-local index `chunk_row` from the value list
            // of global row `global_row`. Discovers/promotes columns in `chunk` as it goes
            // and records ParamRef slots (keyed by global row) in parameter_insert_map_.
            // Answers with a refusal on a malformed row.
            auto fill_row = [&](vector::data_chunk_t& chunk,
                                size_t chunk_row,
                                size_t global_row,
                                List* values_list) -> core::error_t {
                auto values = values_list->lst;
                if (values.size() != fields.size()) {
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"INSERT has more expressions than target columns", resource_});
                }

                auto it_field = key_translation.begin();
                std::size_t field_pos = 0;
                for (auto it_value = values.begin(); it_value != values.end(); ++it_field, ++it_value, ++field_pos) {
                    const std::string& field_name = field_names[field_pos];
                    if (nodeTag(it_value->data) == T_ParamRef) {
                        auto ref = pg_ptr_cast<ParamRef>(it_value->data);
                        auto loc = std::make_pair(global_row, field_name);

                        if (auto it = parameter_insert_map_.find(ref->number); it != parameter_insert_map_.end()) {
                            it->second.emplace_back(std::move(loc));
                        } else {
                            std::pmr::vector<insert_location_t> par(resource_);
                            par.emplace_back(std::move(loc));
                            parameter_insert_map_.emplace(ref->number, std::move(par));
                        }
                    } else if (nodeTag(it_value->data) == T_A_Expr) {
                        // Evaluate constant arithmetic at parse time
                        // TODO: move column matching to validation/optimizer phase for complex path resolution
                        VALUE_OR_RETURN(auto value,
                                        evaluate_const_a_expr(resource_, pg_ptr_cast<A_Expr>(it_value->data)));
                        auto it =
                            std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& column) {
                                return column.type().alias() == field_name;
                            });
                        size_t column_index = it - chunk.data.begin();
                        if (it == chunk.data.end()) {
                            value.set_alias(field_name);
                            chunk.data.emplace_back(resource_, value.type(), chunk.capacity());
                            chunk.set_value(column_index, chunk_row, std::move(value));
                        } else {
                            auto col_type = it->type().type();
                            auto val_type = value.type().type();
                            // BOOLEAN is is_numeric but has no numeric widening: asking the promotion
                            // oracle for a (numeric, BOOLEAN) common type poisons the column vector.
                            // An unpromotable mix takes the plain per-value store below.
                            if (types::is_arithmetic_numeric(col_type) && types::is_arithmetic_numeric(val_type) &&
                                col_type != val_type) {
                                auto promoted = types::promote_type(col_type, val_type);
                                if (promoted != col_type) {
                                    VALUE_OR_RETURN(
                                        auto promoted_col,
                                        promote_column(resource_, *it, chunk_row, promoted, chunk.capacity()));
                                    chunk.data[column_index] = std::move(promoted_col);
                                }
                                VALUE_OR_RETURN(auto widened, numeric_widen(resource_, value, promoted));
                                chunk.set_value(column_index, chunk_row, std::move(widened));
                            } else {
                                chunk.set_value(column_index, chunk_row, std::move(value));
                            }
                        }
                    } else {
                        VALUE_OR_RETURN(auto value, get_value(resource_, pg_ptr_cast<Node>(it_value->data)));
                        auto it =
                            std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& column) {
                                return column.type().alias() == field_name;
                            });
                        size_t column_index = it - chunk.data.begin();
                        if (it == chunk.data.end()) {
                            value.set_alias(field_name);
                            chunk.data.emplace_back(resource_, value.type(), chunk.capacity());
                            chunk.set_value(column_index, chunk_row, std::move(value));
                        } else {
                            auto col_type = it->type().type();
                            auto val_type = value.type().type();
                            // DECIMAL sits outside the widening ladder (is_arithmetic_numeric
                            // excludes it), so a DECIMAL value meeting a column of any other
                            // type — or another DECIMAL of a different (width, scale) — has
                            // no promotion path; the unchecked set_value below would store a
                            // wrongly-typed cell. Refuse the row instead.
                            if (!value.is_null() &&
                                (col_type == types::logical_type::DECIMAL ||
                                 val_type == types::logical_type::DECIMAL) &&
                                it->type() != value.type()) {
                                return core::error_t(
                                    core::error_code_t::sql_parse_error,
                                    std::pmr::string{"INSERT cannot mix a DECIMAL value with values of a "
                                                     "different type in one column of a VALUES list",
                                                     resource_});
                            }
                            // BOOLEAN is is_numeric but has no numeric widening: asking the promotion
                            // oracle for a (numeric, BOOLEAN) common type poisons the column vector.
                            // An unpromotable mix takes the plain per-value store below.
                            if (types::is_arithmetic_numeric(col_type) && types::is_arithmetic_numeric(val_type) &&
                                col_type != val_type) {
                                auto promoted = types::promote_type(col_type, val_type);
                                if (promoted != col_type) {
                                    VALUE_OR_RETURN(
                                        auto promoted_col,
                                        promote_column(resource_, *it, chunk_row, promoted, chunk.capacity()));
                                    chunk.data[column_index] = std::move(promoted_col);
                                }
                                VALUE_OR_RETURN(auto widened, numeric_widen(resource_, value, promoted));
                                chunk.set_value(column_index, chunk_row, std::move(widened));
                            } else if (array_shapes_differ(it->type(), value.type())) {
                                // VALUES rows carry array literals of different shapes (e.g. ARRAY[1]
                                // then ARRAY[2,3]): a single fixed-ARRAY vector can't hold both, so
                                // promote the column to a variable-length LIST and store every row as
                                // a list. The target column's reconciliation handles LIST -> fixed
                                // ARRAY later if needed.
                                auto elem_type = value.type().child_type();
                                if (col_type == types::logical_type::ARRAY) {
                                    VALUE_OR_RETURN(auto list_col,
                                                    promote_array_to_list(resource_,
                                                                          *it,
                                                                          chunk_row,
                                                                          elem_type,
                                                                          chunk.capacity()));
                                    chunk.data[column_index] = std::move(list_col);
                                }
                                chunk.set_value(column_index, chunk_row, to_list_value(resource_, value, elem_type));
                            } else if (col_type == types::logical_type::NA && !value.is_null()) {
                                // The column was created from a LEADING NULL literal (typed NA);
                                // a later row now carries a concrete type. Every prior row is NULL,
                                // so promote the NA column to the concrete type (nulls preserved)
                                // before storing — otherwise set_value asserts on the type mismatch.
                                VALUE_OR_RETURN(
                                    auto na_promoted,
                                    promote_column(resource_, *it, chunk_row, val_type, chunk.capacity()));
                                chunk.data[column_index] = std::move(na_promoted);
                                chunk.set_value(column_index, chunk_row, std::move(value));
                            } else {
                                chunk.set_value(column_index, chunk_row, std::move(value));
                            }
                        }
                    }
                }
                return core::error_t::no_error();
            };

            auto qn = rangevar_to_qualified_name(node.relation);
            // Identity travels via the catalog-resolve wrap; the insert node
            // itself carries only payload + table_oid() (stamped at enrich
            // time from the sibling resolve_table).
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
                // Split the literal rows into uniform ≤CAP chunks (only the last is smaller).
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
            ins_node->returning() = returning;
            ins_node->set_dbname(qn.dbname);
            ins_node->set_relname(qn.relname);
            register_catalog_resolve_table(resource_,
                                           &catalog_resolves_,
                                           qn.dbname,
                                           qn.relname,
                                           constraint_resolve_kind::outgoing);
            return ins;
        } else {
            auto qn = rangevar_to_qualified_name(node.relation);
            auto res = logical_plan::make_node_insert(resource_);
            VALUE_OR_RETURN(auto source, transform_select(*pg_ptr_cast<SelectStmt>(node.selectStmt), plan));
            res->append_child(std::move(source));
            res->key_translation() = key_translation;
            res->returning() = returning;
            res->set_dbname(qn.dbname);
            res->set_relname(qn.relname);
            register_catalog_resolve_table(resource_,
                                           &catalog_resolves_,
                                           qn.dbname,
                                           qn.relname,
                                           constraint_resolve_kind::outgoing);
            return res;
        }
    }
} // namespace components::sql::transform
