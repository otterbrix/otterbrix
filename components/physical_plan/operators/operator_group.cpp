#include "operator_group.hpp"

#include "transformation.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/operators/operator_empty.hpp>
#include "arithmetic_eval.hpp"

namespace components::operators {

    operator_group_t::operator_group_t(std::pmr::memory_resource* resource, log_t log,
                                       expressions::expression_ptr having)
        : read_write_operator_t(resource, log, operator_type::aggregate)
        , keys_(resource_)
        , values_(resource_)
        , computed_columns_(resource_)
        , post_aggregates_(resource_)
        , having_(std::move(having))
        , inputs_(resource_)
        , result_types_(resource_)
        , transposed_output_(resource_) {}

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

            // Phase 2: Group by keys
            create_list_rows();

            // Phase 3: Aggregate per group
            calc_aggregate_values(pipeline_context);

            // Phase 4: Post-aggregate arithmetic
            calc_post_aggregates(pipeline_context);

            // Phase 5: Remove internal aggregate columns (auto-generated aliases)
            if (!post_aggregates_.empty()) {
                // Identify internal aggregate indices to remove
                std::vector<size_t> remove_indices;
                size_t key_count = keys_.empty() ? 0 : result_types_.size() - values_.size() - post_aggregates_.size();
                for (size_t i = 0; i < values_.size(); i++) {
                    auto& name = values_[i].name;
                    if (name.find("__agg_") == 0) {
                        remove_indices.push_back(key_count + i);
                    }
                }
                // Remove in reverse order to preserve indices
                for (auto it = remove_indices.rbegin(); it != remove_indices.rend(); ++it) {
                    result_types_.erase(result_types_.begin() + static_cast<std::ptrdiff_t>(*it));
                    for (auto& row : transposed_output_) {
                        if (*it < row.size()) {
                            row.erase(row.begin() + static_cast<std::ptrdiff_t>(*it));
                        }
                    }
                }
            }
            
            if (having_) {
                filter_having(pipeline_context);
            }

            output_ = operators::make_operator_data(
                left_->output()->resource(),
                impl::transpose(left_->output()->resource(), transposed_output_, result_types_));
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

        auto matrix = impl::transpose(left_->output()->resource(), chunk);
        if (!matrix.empty()) {
            for (const auto& key : keys_) {
                auto values = key.getter->values(matrix.front());
                for (const auto& val : values) {
                    result_types_.emplace_back(val.type());
                }
            }
        }

        for (const auto& row : matrix) {
            std::pmr::vector<types::logical_value_t> new_row(row.get_allocator().resource());
            bool is_valid = true;

            for (const auto& key : keys_) {
                auto values = key.getter->values(row);
                if (values.empty()) {
                    is_valid = false;
                    break;
                } else {
                    for (auto& val : values) {
                        if (key.name != "*") {
                            val.set_alias(std::string{key.name});
                        }
                        new_row.emplace_back(std::move(val));
                    }
                }
            }
            if (is_valid) {
                bool is_new = true;
                for (size_t i = 0; i < transposed_output_.size(); i++) {
                    if (new_row == transposed_output_[i]) {
                        inputs_.at(i).emplace_back(std::move(row));
                        is_new = false;
                        break;
                    }
                }
                if (is_new) {
                    transposed_output_.emplace_back(std::move(new_row));
                    auto input = impl::value_matrix_t(row.get_allocator().resource());
                    input.emplace_back(std::move(row));
                    inputs_.emplace_back(std::move(input));
                }
            }
        }
    }

    void operator_group_t::calc_aggregate_values(pipeline::context_t* pipeline_context) {
        auto types = left_->output()->data_chunk().types();
        for (const auto& value : values_) {
            auto& aggregator = value.aggregator;
            for (size_t i = 0; i < transposed_output_.size(); i++) {
                aggregator->clear(); //todo: need copy aggregator
                aggregator->set_children(boost::intrusive_ptr(new components::operators::operator_empty_t(
                    resource_,
                    operators::make_operator_data(
                        left_->output()->resource(),
                        impl::transpose(left_->output()->resource(), inputs_.at(i), types)))));
                aggregator->on_execute(pipeline_context);
                aggregator->set_value(transposed_output_[i], value.name);
            }
            result_types_.emplace_back(aggregator->value().type());
        }
    }

    void operator_group_t::calc_post_aggregates(pipeline::context_t* pipeline_context) {
        for (auto& post : post_aggregates_) {
            for (auto& row : transposed_output_) {
                // Resolve operands from the row
                auto resolve = [&](const expressions::param_storage& param,
                                   auto& self) -> types::logical_value_t {
                    if (std::holds_alternative<expressions::key_t>(param)) {
                        auto& key = std::get<expressions::key_t>(param);
                        for (auto& val : row) {
                            if (val.type().alias() == key.as_string()) {
                                return val;
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
                                auto left_val = self(sub_scalar->params()[0], self);
                                auto right_val = self(sub_scalar->params()[1], self);
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

                types::logical_value_t left_val = resolve(post.operands[0], resolve);
                types::logical_value_t right_val = resolve(post.operands[1], resolve);
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
                row.emplace_back(std::move(result_val));
            }
            // Add result type from the first row
            if (!transposed_output_.empty()) {
                result_types_.emplace_back(transposed_output_.front().back().type());
            }
        }
    }

    void operator_group_t::filter_having(pipeline::context_t* pipeline_context) {
        if (!having_ || having_->group() != expressions::expression_group::compare) {
            return;
        }
        auto* cmp = static_cast<const expressions::compare_expression_t*>(having_.get());

        // Resolve a param_storage against a row of logical_value_t
        auto resolve = [&](const expressions::param_storage& param,
                           const std::pmr::vector<types::logical_value_t>& row,
                           auto& self) -> types::logical_value_t {
            if (std::holds_alternative<expressions::key_t>(param)) {
                auto& key = std::get<expressions::key_t>(param);
                for (auto& val : row) {
                    if (val.type().has_alias() && val.type().alias() == key.as_string()) {
                        return val;
                    }
                }
                return types::logical_value_t(resource_, types::logical_type::NA);
            } else if (std::holds_alternative<core::parameter_id_t>(param)) {
                auto id = std::get<core::parameter_id_t>(param);
                return pipeline_context->parameters.parameters.at(id);
            } else {
                auto& sub_expr = std::get<expressions::expression_ptr>(param);
                if (sub_expr->group() == expressions::expression_group::scalar) {
                    auto* scalar = static_cast<const expressions::scalar_expression_t*>(sub_expr.get());
                    if (scalar->params().size() >= 2) {
                        auto l = self(scalar->params()[0], row, self);
                        auto r = self(scalar->params()[1], row, self);
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
                return types::logical_value_t(resource_, types::logical_type::NA);
            }
        };

        impl::value_matrix_t filtered(resource_);
        for (auto& row : transposed_output_) {
            auto left_val = resolve(cmp->left(), row, resolve);
            auto right_val = resolve(cmp->right(), row, resolve);
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
                filtered.emplace_back(std::move(row));
            }
        }
        transposed_output_ = std::move(filtered);
    }

} // namespace components::operators
