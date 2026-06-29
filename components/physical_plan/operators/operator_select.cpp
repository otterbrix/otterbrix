#include "operator_select.hpp"

#include "arithmetic_eval.hpp"
#include <components/expressions/compare_expression.hpp>

namespace components::operators {

    namespace {

        // Resolve the source chunk a key reads from. A right-side column reads
        // from the right chunk. Validation only stamps a key right when its data
        // physically lives there, so the caller must supply right_chunk — a joined
        // DELETE/UPDATE RETURNING passes the gathered USING/FROM rows, a SELECT
        // over a JOIN passes its merged chunk. A missing right_chunk here is a
        // validation/wiring bug.
        const vector::data_chunk_t& key_source_chunk(const group_key_t& key,
                                                     const vector::data_chunk_t& chunk,
                                                     const vector::data_chunk_t* right_chunk) {
            const bool from_right = key.side == expressions::side_t::right;
            assert((!from_right || right_chunk != nullptr) && "right-side column requires a right chunk");
            return from_right ? *right_chunk : chunk;
        }

        // Extract the value of a key column (coalesce / case_when, or a deep-path
        // field_ref) for a single row. Mirrors extract_key_value in
        // operator_group.cpp. A top-level field_ref (full_path.size() == 1) is NOT
        // routed here — evaluate_projection references the source column whole, with
        // no per-row logical_value_t round-trip.
        types::logical_value_t extract_select_value(std::pmr::memory_resource* resource,
                                                    const group_key_t& key,
                                                    const vector::data_chunk_t& chunk,
                                                    size_t row_idx,
                                                    const vector::data_chunk_t* right_chunk) {
            const vector::data_chunk_t& src = key_source_chunk(key, chunk, right_chunk);
            switch (key.type) {
                case group_key_t::kind::column: {
                    assert(!key.full_path.empty() && "field_ref path must be resolved before execution");
                    auto val = src.value(key.full_path, row_idx);
                    val.set_alias(std::string{key.name});
                    return val;
                }
                case group_key_t::kind::coalesce: {
                    for (const auto& entry : key.coalesce_entries) {
                        if (entry.type == group_key_t::coalesce_entry::source::constant) {
                            if (!entry.constant.is_null()) {
                                auto val = entry.constant;
                                val.set_alias(std::string{key.name});
                                return val;
                            }
                        } else {
                            if (!src.data[entry.col_index].is_null(row_idx)) {
                                auto val = src.value(entry.col_index, row_idx);
                                val.set_alias(std::string{key.name});
                                return val;
                            }
                        }
                    }
                    auto null_val =
                        types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
                    null_val.set_alias(std::string{key.name});
                    return null_val;
                }
                case group_key_t::kind::case_when: {
                    for (const auto& clause : key.case_clauses) {
                        const auto cond_val = src.value(clause.condition_col, row_idx);
                        auto cmp_result = cond_val.compare(clause.condition_value);
                        bool matches = false;
                        switch (clause.cmp) {
                            case expressions::compare_type::eq:
                                matches = cmp_result == types::compare_t::equals;
                                break;
                            case expressions::compare_type::ne:
                                matches = cmp_result != types::compare_t::equals;
                                break;
                            case expressions::compare_type::gt:
                                matches = cmp_result == types::compare_t::more;
                                break;
                            case expressions::compare_type::gte:
                                matches = cmp_result >= types::compare_t::equals;
                                break;
                            case expressions::compare_type::lt:
                                matches = cmp_result == types::compare_t::less;
                                break;
                            case expressions::compare_type::lte:
                                matches = cmp_result <= types::compare_t::equals;
                                break;
                            default:
                                matches = true;
                                break;
                        }
                        if (matches) {
                            types::logical_value_t result_val =
                                (clause.res_type == group_key_t::case_clause::result_source::constant)
                                    ? clause.res_constant
                                    : src.value(clause.res_col, row_idx);
                            result_val.set_alias(std::string{key.name});
                            return result_val;
                        }
                    }
                    // else branch
                    types::logical_value_t else_val = [&]() -> types::logical_value_t {
                        switch (key.else_type) {
                            case group_key_t::else_source::column:
                                return src.value(key.else_col, row_idx);
                            case group_key_t::else_source::constant:
                                return key.else_constant;
                            case group_key_t::else_source::null_value:
                            default:
                                return types::logical_value_t(resource,
                                                              types::complex_logical_type{types::logical_type::NA});
                        }
                    }();
                    else_val.set_alias(std::string{key.name});
                    return else_val;
                }
            }
            return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
        }

    } // anonymous namespace

    operator_select_t::operator_select_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, log, operator_type::select)
        , columns_(resource) {}

    void operator_select_t::add_column(select_column_t&& col) { columns_.push_back(std::move(col)); }

    void operator_select_t::set_output_types(const std::pmr::vector<types::complex_logical_type>& types) {
        for (size_t i = 0; i < columns_.size() && i < types.size(); ++i) {
            columns_[i].result_type = types[i];
        }
    }

    core::error_t operator_select_t::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) {
        // Streaming projection: apply the per-chunk transform to the single
        // batch handed in via `input`. No accumulation, no read of left_->output().
        // A SELECT over a JOIN receives one merged chunk holding both sides'
        // columns, so the chunk doubles as right_input: its full_path indexes the
        // merged chunk regardless of a key's resolved side.
        auto result = evaluate_projection(resource_,
                                          columns_,
                                          &input,
                                          ctx->parameters,
                                          ctx->session_tz,
                                          &input);
        if (result.has_error()) {
            return result.error();
        }
        out.emplace_back(std::move(result.value()));
        return core::error_t::no_error();
    }

    core::result_wrapper_t<vector::data_chunk_t> evaluate_projection(std::pmr::memory_resource* resource,
                                                                     const std::pmr::vector<select_column_t>& columns,
                                                                     vector::data_chunk_t* input,
                                                                     const logical_plan::storage_parameters& parameters,
                                                                     core::date::timezone_offset_t session_tz,
                                                                     vector::data_chunk_t* right_input) {
        const auto num_rows = input->size();
        const uint64_t cap = num_rows > 0 ? num_rows : 1;

        // Assemble the output chunk directly: one column per projection entry
        // (star_expand fans out to one per input column). Columns are pushed
        // into result.data as they are built; the chunk derives its types from
        // those columns.
        vector::data_chunk_t result(resource, {}, cap);

        for (const auto& col : columns) {
            switch (col.type) {
                case select_column_t::kind::field_ref: {
                    // A top-level column reference (full_path.size() == 1) is a pure
                    // 1:1 column passthrough: zero-copy reference the source vector
                    // whole instead of round-tripping every cell through
                    // logical_value_t. A deeper path (struct field / array or list
                    // element) addresses sub-elements that at()+reference() cannot
                    // reproduce, so it falls through to the per-row extractor.
                    if (num_rows > 0 && col.key.full_path.size() == 1) {
                        const vector::data_chunk_t& src = key_source_chunk(col.key, *input, right_input);
                        const vector::vector_t& source_vec = src.data[col.key.full_path.front()];
                        vector::vector_t vec(resource, source_vec.type(), cap);
                        vec.reference(source_vec);
                        vec.set_type_alias(std::string{col.key.name});
                        result.data.push_back(std::move(vec));
                        break;
                    }
                    [[fallthrough]];
                }
                case select_column_t::kind::coalesce:
                case select_column_t::kind::case_when: {
                    // Genuinely per-row key extraction (conditional COALESCE / CASE,
                    // or a deep-path field_ref). The column type follows the first
                    // value, so values are materialised before the vector. Each cell
                    // is bound to a named local to keep logical_value_t round-trips
                    // to the unavoidable minimum (R1).
                    // The column type IS the plan-resolved type, authoritatively, so the
                    // column is correctly typed even over zero rows (the per-row seed cannot
                    // run then). Values are materialised; NULLs land as NULLs of this type.
                    const types::complex_logical_type col_type = col.result_type;
                    std::pmr::vector<types::logical_value_t> values(resource);
                    values.reserve(num_rows);
                    for (uint64_t row = 0; row < num_rows; ++row) {
                        values.push_back(extract_select_value(resource, col.key, *input, row, right_input));
                    }
                    vector::vector_t vec(resource, col_type, cap);
                    for (uint64_t row = 0; row < num_rows; ++row) {
                        vec.set_value(row, values[row]);
                    }
                    vec.set_type_alias(std::string{col.key.name});
                    result.data.push_back(std::move(vec));
                    break;
                }
                case select_column_t::kind::arithmetic: {
                    auto result_vec =
                        evaluate_arithmetic(resource, col.arith_op, col.operands, *input, parameters, session_tz);
                    if (result_vec.has_error()) {
                        return result_vec.error();
                    }
                    result_vec.value().set_type_alias(std::string{col.key.name});
                    result.data.push_back(std::move(result_vec.value()));
                    break;
                }
                case select_column_t::kind::constant: {
                    // Build the literal as a CONSTANT vector (one stored value) and
                    // flatten it across the chunk, instead of a per-row set_value
                    // loop that copies the same logical_value_t num_rows times.
                    vector::vector_t vec(resource, col.constant_value, cap);
                    if (num_rows > 0) {
                        vec.flatten(num_rows);
                    }
                    vec.set_type_alias(std::string{col.key.name});
                    result.data.push_back(std::move(vec));
                    break;
                }
                case select_column_t::kind::star_expand: {
                    // Bare '*' — expand all columns of the input chunk. Qualified
                    // 'table.*' is pre-expanded to get_field columns at validation,
                    // so it never reaches here.
                    for (size_t ci = 0; ci < input->column_count(); ++ci) {
                        result.data.push_back(input->data[ci]);
                    }
                    break;
                }
            }
        }

        result.set_cardinality(num_rows);
        return result;
    }

} // namespace components::operators
