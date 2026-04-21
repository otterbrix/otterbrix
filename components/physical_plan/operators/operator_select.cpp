#include "operator_select.hpp"

#include "arithmetic_eval.hpp"
#include <components/expressions/compare_expression.hpp>

namespace components::operators {

    namespace {

        // Extract the value of a key column (field_ref / coalesce / case_when) for a single row.
        // Mirrors extract_key_value in operator_group.cpp.
        types::logical_value_t extract_select_value(std::pmr::memory_resource* resource,
                                                    const group_key_t& key,
                                                    const vector::data_chunk_t& chunk,
                                                    size_t row_idx) {
            switch (key.type) {
                case group_key_t::kind::column: {
                    assert(!key.full_path.empty() && "field_ref path must be resolved before execution");
                    auto val = chunk.value(key.full_path, row_idx);
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
                            if (!chunk.data[entry.col_index].is_null(row_idx)) {
                                auto val = chunk.value(entry.col_index, row_idx);
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
                        auto cond_val = chunk.value(clause.condition_col, row_idx);
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
                                    : chunk.value(clause.res_col, row_idx);
                            result_val.set_alias(std::string{key.name});
                            return result_val;
                        }
                    }
                    // else branch
                    types::logical_value_t else_val = [&]() -> types::logical_value_t {
                        switch (key.else_type) {
                            case group_key_t::else_source::column:
                                return chunk.value(key.else_col, row_idx);
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

    void operator_select_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (!left_ || !left_->output()) {
            // No input (no FROM clause): evaluate constants on a virtual single-row empty chunk.
            if (!columns_.empty()) {
                std::pmr::vector<types::complex_logical_type> empty_types(resource_);
                vector::data_chunk_t virtual_input(resource_, empty_types, 1);
                virtual_input.set_cardinality(1);
                auto result = evaluate(pipeline_context, virtual_input);
                output_ = operators::make_operator_data(resource_, std::move(result));
            }
            return;
        }

        auto* resource = left_->output()->resource();
        auto& in_chunks = left_->output()->chunks();
        std::vector<vector::data_chunk_t> out_chunks;
        out_chunks.reserve(in_chunks.size());
        for (auto& input : in_chunks) {
            auto result = evaluate(pipeline_context, input);
            if (has_error()) {
                return;
            }
            out_chunks.emplace_back(std::move(result));
        }
        if (out_chunks.empty()) {
            std::pmr::vector<types::complex_logical_type> empty_types(resource);
            out_chunks.emplace_back(resource, empty_types, 0);
        }
        output_ = operators::make_operator_data(resource, std::move(out_chunks));
    }

    vector::data_chunk_t operator_select_t::evaluate(pipeline::context_t* pipeline_context,
                                                     vector::data_chunk_t& input) {
        auto num_rows = input.size();

        // Build one vector_t per SELECT column.
        std::pmr::vector<vector::vector_t> out_vecs(resource_);
        out_vecs.reserve(columns_.size());

        for (const auto& col : columns_) {
            switch (col.type) {
                case select_column_t::kind::field_ref:
                case select_column_t::kind::coalesce:
                case select_column_t::kind::case_when: {
                    // Per-row key extraction.
                    types::complex_logical_type col_type{types::logical_type::NA};
                    std::pmr::vector<types::logical_value_t> values(resource_);
                    values.reserve(num_rows);
                    for (uint64_t row = 0; row < num_rows; ++row) {
                        auto val = extract_select_value(resource_, col.key, input, row);
                        if (col_type.type() == types::logical_type::NA) {
                            col_type = val.type();
                        }
                        values.push_back(std::move(val));
                    }
                    vector::vector_t vec(resource_, col_type, num_rows);
                    for (uint64_t row = 0; row < num_rows; ++row) {
                        vec.set_value(row, values[row]);
                    }
                    vec.set_type_alias(std::string{col.key.name});
                    out_vecs.push_back(std::move(vec));
                    break;
                }
                case select_column_t::kind::arithmetic: {
                    auto result_vec =
                        evaluate_arithmetic(resource_, col.arith_op, col.operands, input, pipeline_context->parameters);
                    if (result_vec.has_error()) {
                        set_error(result_vec.error());
                        return vector::data_chunk_t(resource_, {}, 0);
                    }
                    result_vec.value().set_type_alias(std::string{col.key.name});
                    out_vecs.push_back(std::move(result_vec.value()));
                    break;
                }
                case select_column_t::kind::constant: {
                    uint64_t cap = num_rows > 0 ? num_rows : 1;
                    vector::vector_t vec(resource_, col.constant_value.type(), cap);
                    for (uint64_t row = 0; row < num_rows; ++row) {
                        vec.set_value(row, col.constant_value);
                    }
                    vec.set_type_alias(std::string{col.key.name});
                    out_vecs.push_back(std::move(vec));
                    break;
                }
                case select_column_t::kind::star_expand: {
                    // Expand all columns from input chunk.
                    for (size_t ci = 0; ci < input.column_count(); ++ci) {
                        out_vecs.push_back(input.data[ci]);
                    }
                    break;
                }
            }
        }

        // Assemble output chunk from the per-column vectors.
        std::pmr::vector<types::complex_logical_type> types(resource_);
        types.reserve(out_vecs.size());
        for (const auto& vec : out_vecs) {
            types.push_back(vec.type());
        }
        vector::data_chunk_t result(resource_, types, num_rows > 0 ? num_rows : 1);
        result.set_cardinality(num_rows);
        for (size_t ci = 0; ci < out_vecs.size(); ++ci) {
            result.data[ci] = std::move(out_vecs[ci]);
        }
        return result;
    }

} // namespace components::operators
