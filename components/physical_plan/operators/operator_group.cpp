#include "operator_group.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/operators/operator_empty.hpp>
#include "arithmetic_eval.hpp"

namespace components::operators {

    namespace {
        bool keys_match(const vector::data_chunk_t& chunk,
                        const std::pmr::vector<size_t>& col_indices,
                        size_t row_idx,
                        const std::pmr::vector<types::logical_value_t>& group_key) {
            for (size_t k = 0; k < col_indices.size(); k++) {
                if (chunk.value(col_indices[k], row_idx) != group_key[k]) {
                    return false;
                }
            }
            return true;
        }
    } // anonymous namespace

    operator_group_t::operator_group_t(std::pmr::memory_resource* resource, log_t log,
                                       expressions::expression_ptr having)
        : read_write_operator_t(resource, log, operator_type::aggregate)
        , keys_(resource_)
        , values_(resource_)
        , computed_columns_(resource_)
        , post_aggregates_(resource_)
        , having_(std::move(having))
        , row_ids_per_group_(resource_)
        , group_keys_(resource_)
        , group_index_(resource_) {}

    void operator_group_t::add_key(const std::pmr::string& name, get::operator_get_ptr&& getter) {
        keys_.push_back({name, std::move(getter)});
    }

    void operator_group_t::add_value(const std::pmr::string& name, aggregate::operator_aggregate_ptr&& aggregator) {
        values_.push_back({name, std::move(aggregator)});
    }

    void operator_group_t::add_computed_column(computed_column_t&& col) {
        computed_columns_.emplace_back(std::move(col));
    }

    void operator_group_t::add_post_aggregate(post_aggregate_column_t&& col) {
        post_aggregates_.emplace_back(std::move(col));
    }

    void operator_group_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (left_ && left_->output()) {
            auto& chunk = left_->output()->data_chunk();

            // Phase 1: Pre-compute arithmetic columns (before grouping)
            for (auto& comp : computed_columns_) {
                auto result_vec = evaluate_arithmetic(
                    resource_, comp.op, comp.operands, chunk, pipeline_context->parameters);
                result_vec.type().set_alias(std::string(comp.alias));
                chunk.data.emplace_back(std::move(result_vec));
            }

            // Phase 2: Group by keys (columnar, no transpose)
            create_list_rows();

            // Phase 3: Aggregate per group + build result chunk
            auto result = calc_aggregate_values(pipeline_context);

            // Phase 4: Post-aggregate arithmetic (columnar)
            calc_post_aggregates(pipeline_context, result);

            // Phase 5: Remove internal __agg_ columns
            if (!post_aggregates_.empty()) {
                size_t key_count = group_keys_.empty() ? 0 : group_keys_[0].size();
                for (size_t i = values_.size(); i > 0; --i) {
                    auto& name = values_[i - 1].name;
                    if (name.find("__agg_") == 0) {
                        size_t col_idx = key_count + (i - 1);
                        if (col_idx < result.data.size()) {
                            result.data.erase(result.data.begin() +
                                              static_cast<std::ptrdiff_t>(col_idx));
                        }
                    }
                }
            }

            // Phase 6: HAVING filter (columnar)
            if (having_) {
                filter_having(pipeline_context, result);
            }

            // Phase 7: Output — already a data_chunk_t, no transpose needed
            output_ = operators::make_operator_data(left_->output()->resource(), std::move(result));

            // Clear temporary grouping state
            row_ids_per_group_.clear();
            group_keys_.clear();
            group_index_.clear();
        } else if (!computed_columns_.empty()) {
            // Constants-only query (no FROM clause): evaluate arithmetic on a virtual single row
            std::pmr::vector<types::complex_logical_type> empty_types(resource_);
            vector::data_chunk_t chunk(resource_, empty_types, 1);
            chunk.set_cardinality(1);

            for (auto& comp : computed_columns_) {
                auto result_vec = evaluate_arithmetic(
                    resource_, comp.op, comp.operands, chunk, pipeline_context->parameters);
                result_vec.type().set_alias(std::string(comp.alias));
                chunk.data.emplace_back(std::move(result_vec));
            }

            output_ = operators::make_operator_data(resource_, std::move(chunk));
        }
    }

    void operator_group_t::create_list_rows() {
        auto& chunk = left_->output()->data_chunk();
        auto num_rows = chunk.size();

        if (num_rows == 0) {
            return;
        }

        // Try fast path: resolve all keys to simple top-level column indices
        bool use_fast_path = true;
        std::pmr::vector<size_t> key_col_indices(resource_);
        for (const auto& key : keys_) {
            if (std::string_view(key.name) == "*") {
                use_fast_path = false;
                break;
            }
            bool found = false;
            for (size_t col = 0; col < chunk.column_count(); col++) {
                if (chunk.data[col].type().alias() == std::string_view(key.name)) {
                    key_col_indices.push_back(col);
                    found = true;
                    break;
                }
            }
            if (!found) {
                use_fast_path = false;
                break;
            }
        }

        if (use_fast_path && !key_col_indices.empty()) {
            // Batch hash all rows at once using type-dispatched vector_ops::hash
            vector::vector_t hash_vec(resource_, types::logical_type::UBIGINT, num_rows);
            std::vector<uint64_t> col_ids(key_col_indices.begin(), key_col_indices.end());
            chunk.hash(col_ids, hash_vec);
            auto* hashes = hash_vec.data<uint64_t>();

            for (size_t row_idx = 0; row_idx < num_rows; row_idx++) {
                // Check for NULL keys via validity mask (no logical_value_t creation)
                bool is_valid = true;
                for (size_t col_idx : key_col_indices) {
                    if (chunk.data[col_idx].is_null(row_idx)) {
                        is_valid = false;
                        break;
                    }
                }
                if (!is_valid) {
                    continue;
                }

                auto h = static_cast<size_t>(hashes[row_idx]);
                auto it = group_index_.find(h);
                bool is_new = true;
                if (it != group_index_.end()) {
                    for (size_t idx : it->second) {
                        if (keys_match(chunk, key_col_indices, row_idx, group_keys_[idx])) {
                            row_ids_per_group_[idx].push_back(row_idx);
                            is_new = false;
                            break;
                        }
                    }
                }
                if (is_new) {
                    // Only extract key values when creating a new group
                    std::pmr::vector<types::logical_value_t> key_vals(resource_);
                    for (size_t ki = 0; ki < key_col_indices.size(); ki++) {
                        auto val = chunk.value(key_col_indices[ki], row_idx);
                        val.set_alias(std::string{keys_[ki].name});
                        key_vals.push_back(std::move(val));
                    }
                    size_t idx = group_keys_.size();
                    group_index_[h].push_back(idx);
                    group_keys_.push_back(std::move(key_vals));
                    std::pmr::vector<size_t> row_ids(resource_);
                    row_ids.push_back(row_idx);
                    row_ids_per_group_.push_back(std::move(row_ids));
                }
            }
        } else {
            // Slow path: getter-based key extraction (handles wildcards, nested paths, etc.)
            for (size_t row_idx = 0; row_idx < num_rows; row_idx++) {
                std::pmr::vector<types::logical_value_t> key_vals(resource_);
                bool is_valid = true;

                if (use_fast_path) {
                    // Fast path without batch hash (empty key_col_indices case)
                    size_t key_i = 0;
                    for (size_t col_idx : key_col_indices) {
                        auto val = chunk.value(col_idx, row_idx);
                        if (val.is_null()) {
                            is_valid = false;
                            break;
                        }
                        val.set_alias(std::string{keys_[key_i].name});
                        key_vals.push_back(std::move(val));
                        key_i++;
                    }
                } else {
                    std::pmr::vector<types::logical_value_t> row(resource_);
                    row.reserve(chunk.column_count());
                    for (size_t c = 0; c < chunk.column_count(); c++) {
                        row.push_back(chunk.value(c, row_idx));
                    }
                    for (const auto& key : keys_) {
                        auto values = key.getter->values(row);
                        if (values.empty()) {
                            is_valid = false;
                            break;
                        }
                        for (auto& val : values) {
                            if (std::string_view(key.name) != "*") {
                                val.set_alias(std::string{key.name});
                            }
                            key_vals.push_back(std::move(val));
                        }
                    }
                }
                if (!is_valid) {
                    continue;
                }

                size_t h = types::hash_row(key_vals);
                auto it = group_index_.find(h);
                bool is_new = true;
                if (it != group_index_.end()) {
                    for (size_t idx : it->second) {
                        if (key_vals == group_keys_[idx]) {
                            row_ids_per_group_[idx].push_back(row_idx);
                            is_new = false;
                            break;
                        }
                    }
                }
                if (is_new) {
                    size_t idx = group_keys_.size();
                    group_index_[h].push_back(idx);
                    group_keys_.push_back(std::move(key_vals));
                    std::pmr::vector<size_t> row_ids(resource_);
                    row_ids.push_back(row_idx);
                    row_ids_per_group_.push_back(std::move(row_ids));
                }
            }
        }
    }

    vector::data_chunk_t operator_group_t::calc_aggregate_values(pipeline::context_t* pipeline_context) {
        auto& chunk = left_->output()->data_chunk();
        size_t num_groups = group_keys_.size();
        size_t key_count = num_groups > 0 ? group_keys_[0].size() : 0;

        // Compute aggregate results: agg_results[agg_idx][group_idx]
        std::pmr::vector<std::pmr::vector<types::logical_value_t>> agg_results(resource_);
        agg_results.reserve(values_.size());

        for (const auto& value : values_) {
            auto& aggregator = value.aggregator;
            std::pmr::vector<types::logical_value_t> results(resource_);
            results.reserve(num_groups);

            for (size_t i = 0; i < num_groups; i++) {
                auto sub_chunk = chunk.slice(resource_, row_ids_per_group_[i]);
                aggregator->clear();
                aggregator->set_children(boost::intrusive_ptr(new operator_empty_t(
                    resource_,
                    operators::make_operator_data(left_->output()->resource(), std::move(sub_chunk)))));
                aggregator->on_execute(pipeline_context);
                auto agg_val = aggregator->value();
                agg_val.set_alias(std::string(value.name));
                results.push_back(std::move(agg_val));
            }
            agg_results.push_back(std::move(results));
        }

        // Build result types: key types + aggregate types
        std::pmr::vector<types::complex_logical_type> result_types(resource_);
        if (num_groups > 0) {
            for (size_t k = 0; k < key_count; k++) {
                result_types.push_back(group_keys_[0][k].type());
            }
        }
        for (size_t a = 0; a < values_.size(); a++) {
            if (!agg_results[a].empty()) {
                result_types.push_back(agg_results[a][0].type());
            }
        }

        // Create result chunk
        uint64_t cap = num_groups > 0 ? static_cast<uint64_t>(num_groups) : 1;
        vector::data_chunk_t result(resource_, result_types, cap);
        result.set_cardinality(static_cast<uint64_t>(num_groups));

        // Fill key columns
        for (size_t g = 0; g < num_groups; g++) {
            for (size_t k = 0; k < key_count; k++) {
                result.set_value(k, g, std::move(group_keys_[g][k]));
            }
        }

        // Fill aggregate columns
        for (size_t a = 0; a < values_.size(); a++) {
            for (size_t g = 0; g < num_groups; g++) {
                result.set_value(key_count + a, g, std::move(agg_results[a][g]));
            }
        }

        return result;
    }

    void operator_group_t::calc_post_aggregates(pipeline::context_t* pipeline_context,
                                                 vector::data_chunk_t& result) {
        auto num_groups = result.size();
        for (auto& post : post_aggregates_) {
            // Determine result type from first row computation
            types::complex_logical_type col_type{types::logical_type::NA};

            auto resolve = [&](const expressions::param_storage& param,
                               size_t row_idx,
                               auto& self) -> types::logical_value_t {
                if (std::holds_alternative<expressions::key_t>(param)) {
                    auto& key = std::get<expressions::key_t>(param);
                    for (size_t c = 0; c < result.column_count(); c++) {
                        if (result.data[c].type().alias() == key.as_string()) {
                            return result.value(c, row_idx);
                        }
                    }
                    throw std::logic_error("Post-aggregate: column not found: " + key.as_string());
                } else if (std::holds_alternative<core::parameter_id_t>(param)) {
                    auto id = std::get<core::parameter_id_t>(param);
                    return pipeline_context->parameters.parameters.at(id);
                } else {
                    auto& sub_expr = std::get<expressions::expression_ptr>(param);
                    if (sub_expr->group() == expressions::expression_group::scalar) {
                        auto* sub_scalar =
                            static_cast<const expressions::scalar_expression_t*>(sub_expr.get());
                        if (sub_scalar->params().size() >= 2) {
                            auto left_val = self(sub_scalar->params()[0], row_idx, self);
                            auto right_val = self(sub_scalar->params()[1], row_idx, self);
                            switch (sub_scalar->type()) {
                                case expressions::scalar_type::add:
                                    return types::logical_value_t::sum(left_val, right_val);
                                case expressions::scalar_type::subtract:
                                    return types::logical_value_t::subtract(left_val, right_val);
                                case expressions::scalar_type::multiply:
                                    return types::logical_value_t::mult(left_val, right_val);
                                case expressions::scalar_type::divide:
                                    return types::logical_value_t::divide(left_val, right_val);
                                case expressions::scalar_type::mod:
                                    return types::logical_value_t::modulus(left_val, right_val);
                                default:
                                    break;
                            }
                        }
                    }
                    throw std::logic_error("Post-aggregate: unsupported sub-expression");
                }
            };

            // Compute result for each group and collect into a new vector
            std::pmr::vector<types::logical_value_t> col_values(resource_);
            for (size_t g = 0; g < num_groups; g++) {
                auto left_val = resolve(post.operands[0], g, resolve);
                auto right_val = resolve(post.operands[1], g, resolve);
                types::logical_value_t result_val(resource_, types::logical_type::NA);
                switch (post.op) {
                    case expressions::scalar_type::add:
                        result_val = types::logical_value_t::sum(left_val, right_val);
                        break;
                    case expressions::scalar_type::subtract:
                        result_val = types::logical_value_t::subtract(left_val, right_val);
                        break;
                    case expressions::scalar_type::multiply:
                        result_val = types::logical_value_t::mult(left_val, right_val);
                        break;
                    case expressions::scalar_type::divide:
                        result_val = types::logical_value_t::divide(left_val, right_val);
                        break;
                    case expressions::scalar_type::mod:
                        result_val = types::logical_value_t::modulus(left_val, right_val);
                        break;
                    default:
                        break;
                }
                result_val.set_alias(std::string(post.alias));
                if (g == 0) {
                    col_type = result_val.type();
                }
                col_values.push_back(std::move(result_val));
            }

            // Add new column to result chunk
            vector::vector_t new_col(resource_, col_type, result.capacity());
            for (size_t g = 0; g < num_groups; g++) {
                new_col.set_value(g, std::move(col_values[g]));
            }
            new_col.type().set_alias(std::string(post.alias));
            result.data.emplace_back(std::move(new_col));
        }
    }

    void operator_group_t::filter_having(pipeline::context_t* pipeline_context,
                                          vector::data_chunk_t& result) {
        if (!having_ || having_->group() != expressions::expression_group::compare) {
            return;
        }
        auto* cmp = static_cast<const expressions::compare_expression_t*>(having_.get());

        auto resolve = [&](const expressions::param_storage& param,
                           size_t row_idx,
                           auto& self) -> types::logical_value_t {
            if (std::holds_alternative<expressions::key_t>(param)) {
                auto& key = std::get<expressions::key_t>(param);
                for (size_t c = 0; c < result.column_count(); c++) {
                    if (result.data[c].type().has_alias() && result.data[c].type().alias() == key.as_string()) {
                        return result.value(c, row_idx);
                    }
                }
                return types::logical_value_t(resource_, types::complex_logical_type{types::logical_type::NA});
            } else if (std::holds_alternative<core::parameter_id_t>(param)) {
                auto id = std::get<core::parameter_id_t>(param);
                return pipeline_context->parameters.parameters.at(id);
            } else {
                auto& sub_expr = std::get<expressions::expression_ptr>(param);
                if (sub_expr->group() == expressions::expression_group::scalar) {
                    auto* scalar = static_cast<const expressions::scalar_expression_t*>(sub_expr.get());
                    if (scalar->params().size() >= 2) {
                        auto l = self(scalar->params()[0], row_idx, self);
                        auto r = self(scalar->params()[1], row_idx, self);
                        switch (scalar->type()) {
                            case expressions::scalar_type::add:
                                return types::logical_value_t::sum(l, r);
                            case expressions::scalar_type::subtract:
                                return types::logical_value_t::subtract(l, r);
                            case expressions::scalar_type::multiply:
                                return types::logical_value_t::mult(l, r);
                            case expressions::scalar_type::divide:
                                return types::logical_value_t::divide(l, r);
                            case expressions::scalar_type::mod:
                                return types::logical_value_t::modulus(l, r);
                            default:
                                break;
                        }
                    }
                }
                return types::logical_value_t(resource_, types::complex_logical_type{types::logical_type::NA});
            }
        };

        std::pmr::vector<size_t> keep_indices(resource_);
        for (size_t g = 0; g < result.size(); g++) {
            auto left_val = resolve(cmp->left(), g, resolve);
            auto right_val = resolve(cmp->right(), g, resolve);
            auto cmp_result = left_val.compare(right_val);
            bool passes = false;
            switch (cmp->type()) {
                case expressions::compare_type::gt:
                    passes = cmp_result == types::compare_t::more;
                    break;
                case expressions::compare_type::gte:
                    passes = cmp_result >= types::compare_t::equals;
                    break;
                case expressions::compare_type::lt:
                    passes = cmp_result == types::compare_t::less;
                    break;
                case expressions::compare_type::lte:
                    passes = cmp_result <= types::compare_t::equals;
                    break;
                case expressions::compare_type::eq:
                    passes = cmp_result == types::compare_t::equals;
                    break;
                case expressions::compare_type::ne:
                    passes = cmp_result != types::compare_t::equals;
                    break;
                default:
                    passes = true;
                    break;
            }
            if (passes) {
                keep_indices.push_back(g);
            }
        }

        if (keep_indices.size() < result.size()) {
            result = result.slice(resource_, keep_indices);
        }
    }

} // namespace components::operators
