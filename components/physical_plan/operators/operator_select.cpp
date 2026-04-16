#include "operator_select.hpp"

#include "arithmetic_eval.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/physical_plan/operators/aggregate/operator_func.hpp>
#include <components/physical_plan/operators/operator_batch.hpp>

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

    operator_select_t::operator_select_t(std::pmr::memory_resource* resource, log_t log, mode m)
        : read_write_operator_t(resource, log, operator_type::select)
        , columns_(resource)
        , mode_(m) {}

    void operator_select_t::add_column(select_column_t&& col) { columns_.push_back(std::move(col)); }

    void operator_select_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (!left_ || !left_->output()) {
            // No input — emit an empty chunk if we have a known schema, or nothing.
            if (mode_ == mode::global_aggregate && !columns_.empty()) {
                // Global aggregate over empty input: evaluate all aggregates over zero rows.
                // accumulate() only reads input.size(), so column types are irrelevant here.
                vector::data_chunk_t empty_input(resource_, {}, 0);
                empty_input.set_cardinality(0);
                auto result = accumulate(pipeline_context, empty_input);
                output_ = operators::make_operator_data(resource_, std::move(result));
            } else if (mode_ == mode::evaluation && !columns_.empty()) {
                // Constants-only query (no FROM clause): evaluate on a virtual single-row empty chunk.
                std::pmr::vector<types::complex_logical_type> empty_types(resource_);
                vector::data_chunk_t virtual_input(resource_, empty_types, 1);
                virtual_input.set_cardinality(1);
                auto result = evaluate(pipeline_context, virtual_input);
                output_ = operators::make_operator_data(resource_, std::move(result));
            }
            return;
        }

        auto& input = left_->output()->data_chunk();

        if (mode_ == mode::global_aggregate) {
            auto result = accumulate(pipeline_context, input);
            output_ = operators::make_operator_data(left_->output()->resource(), std::move(result));
        } else {
            auto result = evaluate(pipeline_context, input);
            output_ = operators::make_operator_data(left_->output()->resource(), std::move(result));
        }
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
                    // Expand all columns from input chunk (SELECT *).
                    for (size_t ci = 0; ci < input.column_count(); ++ci) {
                        out_vecs.push_back(input.data[ci]);
                    }
                    break;
                }
                case select_column_t::kind::aggregate: {
                    // Aggregate in evaluation mode: run the aggregator over all rows.
                    // This handles the case where there's a mix; for a pure global aggregate
                    // use global_aggregate mode instead.
                    col.aggregator->clear();
                    {
                        std::vector<vector::data_chunk_t> agg_chunks;
                        if (num_rows > 0) {
                            agg_chunks.emplace_back(input.slice_contiguous(resource_, 0, num_rows));
                        }
                        col.aggregator->set_children(make_operator_batch(resource_, std::move(agg_chunks)));
                    }
                    col.aggregator->on_execute(pipeline_context);
                    auto val = col.aggregator->value();
                    val.set_alias(std::string{col.key.name});

                    uint64_t cap = num_rows > 0 ? num_rows : 1;
                    vector::vector_t vec(resource_, val.type(), cap);
                    for (uint64_t row = 0; row < num_rows; ++row) {
                        vec.set_value(row, val);
                    }
                    vec.set_type_alias(std::string{col.key.name});
                    out_vecs.push_back(std::move(vec));
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

    vector::data_chunk_t operator_select_t::accumulate(pipeline::context_t* pipeline_context,
                                                       vector::data_chunk_t& input) {
        // Two-phase execution for global aggregate mode:
        //
        // Phase 1: Run ALL aggregate columns (visible + internal hidden ones at tail).
        //          Build a 1-row intermediate chunk with named columns.
        //
        // Phase 2: Evaluate each visible output column over the intermediate chunk.
        //          Arithmetic columns can reference aggregate results by name (name-based fallback
        //          in arithmetic_eval.cpp).
        //
        // Visible columns = columns_[0 .. size - internal_aggregate_count_ - 1]
        // Internal (hidden) aggregate columns = columns_[size - internal_aggregate_count_ .. size - 1]

        auto num_rows = input.size();
        size_t visible_count = columns_.size() - internal_aggregate_count_;

        // --- Phase 1: run all aggregates and build 1-row intermediate chunk ---
        // Collect aggregate results keyed by alias.
        std::pmr::vector<types::logical_value_t> all_agg_vals(resource_);
        std::pmr::vector<std::pmr::string> all_agg_names(resource_);

        for (const auto& col : columns_) {
            if (col.type == select_column_t::kind::aggregate) {
                std::vector<vector::data_chunk_t> group_chunks;
                if (num_rows > 0) {
                    group_chunks.emplace_back(input.slice_contiguous(resource_, 0, num_rows));
                }
                col.aggregator->clear();
                col.aggregator->set_children(make_operator_batch(resource_, std::move(group_chunks)));
                col.aggregator->on_execute(pipeline_context);

                auto datum = col.aggregator->take_batch_values();
                types::logical_value_t val(resource_, types::logical_type::NA);
                if (std::holds_alternative<std::pmr::vector<types::logical_value_t>>(datum)) {
                    auto& vals = std::get<std::pmr::vector<types::logical_value_t>>(datum);
                    if (!vals.empty()) {
                        val = std::move(vals[0]);
                    }
                }
                val.set_alias(std::string{col.key.name});
                all_agg_names.push_back(col.key.name);
                all_agg_vals.push_back(std::move(val));
            }
        }

        // Build 1-row intermediate chunk with all aggregate results.
        std::pmr::vector<types::complex_logical_type> inter_types(resource_);
        for (const auto& val : all_agg_vals) {
            inter_types.push_back(val.type());
        }
        vector::data_chunk_t inter(resource_, inter_types, 1);
        inter.set_cardinality(1);
        for (size_t i = 0; i < all_agg_vals.size(); ++i) {
            inter.data[i].set_type_alias(std::string{all_agg_names[i]});
            inter.set_value(i, 0, all_agg_vals[i]);
        }

        // --- Phase 2: evaluate visible output columns over the intermediate chunk ---
        std::pmr::vector<vector::vector_t> out_vecs(resource_);
        out_vecs.reserve(visible_count);

        size_t agg_idx_in_inter = 0; // index into all_agg_vals for visible aggregate columns
        for (size_t ci = 0; ci < visible_count; ++ci) {
            const auto& col = columns_[ci];
            switch (col.type) {
                case select_column_t::kind::aggregate: {
                    // Grab from intermediate chunk.
                    types::complex_logical_type col_type = agg_idx_in_inter < all_agg_vals.size()
                                                               ? all_agg_vals[agg_idx_in_inter].type()
                                                               : types::complex_logical_type{types::logical_type::NA};
                    vector::vector_t vec(resource_, col_type, 1);
                    if (agg_idx_in_inter < all_agg_vals.size()) {
                        vec.set_value(0, all_agg_vals[agg_idx_in_inter]);
                    }
                    vec.set_type_alias(std::string{col.key.name});
                    out_vecs.push_back(std::move(vec));
                    ++agg_idx_in_inter;
                    break;
                }
                case select_column_t::kind::arithmetic: {
                    // Evaluate arithmetic expression over the intermediate (aggregate result) chunk.
                    auto result_vec =
                        evaluate_arithmetic(resource_, col.arith_op, col.operands, inter, pipeline_context->parameters);
                    if (result_vec.has_error()) {
                        set_error(result_vec.error());
                        return vector::data_chunk_t(resource_, {}, 0);
                    }
                    result_vec.value().set_type_alias(std::string{col.key.name});
                    out_vecs.push_back(std::move(result_vec.value()));
                    break;
                }
                case select_column_t::kind::constant: {
                    vector::vector_t vec(resource_, col.constant_value.type(), 1);
                    vec.set_value(0, col.constant_value);
                    vec.set_type_alias(std::string{col.key.name});
                    out_vecs.push_back(std::move(vec));
                    break;
                }
                case select_column_t::kind::field_ref:
                case select_column_t::kind::coalesce:
                case select_column_t::kind::case_when: {
                    // Try to extract from intermediate chunk by name.
                    auto val = extract_select_value(resource_, col.key, inter, 0);
                    vector::vector_t vec(resource_, val.type(), 1);
                    vec.set_value(0, val);
                    vec.set_type_alias(std::string{col.key.name});
                    out_vecs.push_back(std::move(vec));
                    break;
                }
                case select_column_t::kind::star_expand: {
                    // Expand all columns from the intermediate chunk.
                    for (size_t ci = 0; ci < inter.column_count(); ++ci) {
                        out_vecs.push_back(inter.data[ci]);
                    }
                    break;
                }
            }
        }

        // Assemble output chunk.
        std::pmr::vector<types::complex_logical_type> types(resource_);
        types.reserve(out_vecs.size());
        for (const auto& vec : out_vecs) {
            types.push_back(vec.type());
        }
        vector::data_chunk_t result(resource_, types, 1);
        result.set_cardinality(1);
        for (size_t ci = 0; ci < out_vecs.size(); ++ci) {
            result.data[ci] = std::move(out_vecs[ci]);
        }
        return result;
    }

} // namespace components::operators
