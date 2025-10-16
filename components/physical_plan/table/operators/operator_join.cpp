#include "operator_join.hpp"

#include "check_expr.hpp"

#include <services/collection/collection.hpp>
#include <vector>

namespace components::table::operators {

    operator_join_t::operator_join_t(services::collection::context_collection_t* context,
                                     type join_type,
                                     const expressions::compare_expression_ptr& expression)
        : read_only_operator_t(context, operator_type::join)
        , join_type_(join_type)
        , expression_(std::move(expression)) {}

    bool operator_join_t::check_predicate_(pipeline::context_t* context, size_t row_left, size_t row_right) const {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        return check_expr_general(expression_,
                                  &context->parameters,
                                  chunk_left,
                                  chunk_right,
                                  name_index_map_left_,
                                  name_index_map_right_,
                                  row_left,
                                  row_right);
    }

    void operator_join_t::on_execute_impl(pipeline::context_t* context) {
        if (!left_ || !right_) {
            return;
        }
        if (left_->output() && right_->output()) {
            const auto& chunk_left = left_->output()->data_chunk();
            const auto& chunk_right = right_->output()->data_chunk();

            auto res_types = chunk_left.types();
            for (const auto& type : chunk_right.types()) {
                if (std::find_if(res_types.begin(), res_types.end(), [&type](const types::complex_logical_type& rhs) {
                        return type.alias() == rhs.alias();
                    }) == res_types.end()) {
                    res_types.emplace_back(type);
                }
            }

            output_ = base::operators::make_operator_data(left_->output()->resource(), res_types);

            if (context_) {
                // With introduction of raw_data without context, log is not guaranteed to be here
                // TODO: acquire log from different means
                trace(context_->log(), "operator_join::left_size(): {}", chunk_left.size());
                trace(context_->log(), "operator_join::right_size(): {}", chunk_right.size());
            }

            name_index_map_left_.clear();
            for (size_t i = 0; i < chunk_left.data.size(); i++) {
                name_index_map_left_.emplace(chunk_left.data[i].type().alias(), i);
            }
            name_index_map_right_.clear();
            for (size_t i = 0; i < chunk_right.data.size(); i++) {
                name_index_map_right_.emplace(chunk_right.data[i].type().alias(), i);
            }
            name_index_map_res_.clear();
            for (size_t i = 0; i < res_types.size(); i++) {
                name_index_map_res_.emplace(res_types[i].alias(), i);
            }

            switch (join_type_) {
                case type::inner:
                    inner_join_(context);
                    break;
                case type::full:
                    outer_full_join_(context);
                    break;
                case type::left:
                    outer_left_join_(context);
                    break;
                case type::right:
                    outer_right_join_(context);
                    break;
                case type::cross:
                    cross_join_(context);
                    break;
                default:
                    break;
            }

            if (context_) {
                // Same reason as above
                trace(context_->log(), "operator_join::result_size(): {}", output_->size());
            }
        }
    }

    void operator_join_t::inner_join_(pipeline::context_t* context) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();

        size_t res_index = 0;
        for (size_t i = 0; i < chunk_left.size(); i++) {
            for (size_t j = 0; j < chunk_right.size(); j++) {
                if (check_predicate_(context, i, j)) {
                    for (const auto& column : chunk_left.data) {
                        chunk_res.set_value(name_index_map_res_.at(column.type().alias()), res_index, column.value(i));
                    }
                    for (const auto& column : chunk_right.data) {
                        chunk_res.set_value(name_index_map_res_.at(column.type().alias()), res_index, column.value(j));
                    }
                    vector::validate_chunk_capacity(chunk_res, ++res_index);
                }
            }
        }
        chunk_res.set_cardinality(res_index);
    }

    void operator_join_t::outer_full_join_(pipeline::context_t* context) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();

        std::vector<bool> visited_right(right_->output()->size(), false);

        size_t res_index = 0;
        for (size_t i = 0; i < chunk_left.size(); i++) {
            bool visited_left = false;
            size_t right_index = 0;
            for (size_t j = 0; j < chunk_right.size(); j++) {
                if (check_predicate_(context, i, j)) {
                    visited_left = true;
                    visited_right[right_index] = true;
                    for (const auto& column : chunk_left.data) {
                        chunk_res.set_value(name_index_map_res_.at(column.type().alias()), res_index, column.value(i));
                    }
                    for (const auto& column : chunk_right.data) {
                        chunk_res.set_value(name_index_map_res_.at(column.type().alias()), res_index, column.value(j));
                    }
                    vector::validate_chunk_capacity(chunk_res, ++res_index);
                }
                right_index++;
                if (!visited_left) {
                    for (const auto& column : chunk_left.data) {
                        chunk_res.set_value(name_index_map_res_.at(column.type().alias()), res_index, column.value(i));
                    }
                    for (const auto& column : chunk_right.data) {
                        chunk_res.set_value(name_index_map_res_.at(column.type().alias()),
                                            res_index,
                                            types::logical_value_t{nullptr});
                    }
                    vector::validate_chunk_capacity(chunk_res, ++res_index);
                }
            }
        }
        for (size_t i = 0; i < visited_right.size(); ++i) {
            if (visited_right[i]) {
                continue;
            }
            for (const auto& column : chunk_left.data) {
                chunk_res.set_value(name_index_map_res_.at(column.type().alias()),
                                    res_index,
                                    types::logical_value_t{nullptr});
            }
            for (const auto& column : chunk_right.data) {
                chunk_res.set_value(name_index_map_res_.at(column.type().alias()), res_index, column.value(i));
            }
            vector::validate_chunk_capacity(chunk_res, ++res_index);
        }
    }

    void operator_join_t::outer_left_join_(pipeline::context_t* context) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();

        size_t res_index = 0;
        for (size_t i = 0; i < chunk_left.size(); i++) {
            bool visited_left = false;
            for (size_t j = 0; j < chunk_right.size(); j++) {
                if (check_predicate_(context, i, j)) {
                    visited_left = true;
                    for (const auto& column : chunk_left.data) {
                        chunk_res.set_value(name_index_map_res_.at(column.type().alias()), res_index, column.value(i));
                    }
                    for (const auto& column : chunk_right.data) {
                        chunk_res.set_value(name_index_map_res_.at(column.type().alias()), res_index, column.value(j));
                    }
                    vector::validate_chunk_capacity(chunk_res, ++res_index);
                }
                if (!visited_left) {
                    for (const auto& column : chunk_left.data) {
                        chunk_res.set_value(name_index_map_res_.at(column.type().alias()), res_index, column.value(i));
                    }
                    for (const auto& column : chunk_right.data) {
                        chunk_res.set_value(name_index_map_res_.at(column.type().alias()),
                                            res_index,
                                            types::logical_value_t{nullptr});
                    }
                    vector::validate_chunk_capacity(chunk_res, ++res_index);
                }
            }
        }
    }

    void operator_join_t::outer_right_join_(pipeline::context_t* context) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();

        size_t res_index = 0;
        for (size_t i = 0; i < chunk_right.size(); i++) {
            bool visited_right = false;
            for (size_t j = 0; j < chunk_left.size(); j++) {
                if (check_predicate_(context, i, j)) {
                    visited_right = true;
                    for (const auto& column : chunk_left.data) {
                        chunk_res.set_value(name_index_map_res_.at(column.type().alias()), res_index, column.value(i));
                    }
                    for (const auto& column : chunk_right.data) {
                        chunk_res.set_value(name_index_map_res_.at(column.type().alias()), res_index, column.value(j));
                    }
                    vector::validate_chunk_capacity(chunk_res, ++res_index);
                }
                if (!visited_right) {
                    for (const auto& column : chunk_left.data) {
                        chunk_res.set_value(name_index_map_res_.at(column.type().alias()),
                                            res_index,
                                            types::logical_value_t{nullptr});
                    }
                    for (const auto& column : chunk_right.data) {
                        chunk_res.set_value(name_index_map_res_.at(column.type().alias()), res_index, column.value(j));
                    }
                    vector::validate_chunk_capacity(chunk_res, ++res_index);
                }
            }
        }
    }

    void operator_join_t::cross_join_(pipeline::context_t* context) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();

        size_t res_index = 0;
        for (size_t i = 0; i < chunk_left.size(); i++) {
            for (size_t j = 0; j < chunk_right.size(); j++) {
                for (const auto& column : chunk_left.data) {
                    chunk_res.set_value(name_index_map_res_.at(column.type().alias()), res_index, column.value(i));
                }
                for (const auto& column : chunk_right.data) {
                    chunk_res.set_value(name_index_map_res_.at(column.type().alias()), res_index, column.value(j));
                }
                vector::validate_chunk_capacity(chunk_res, ++res_index);
            }
        }
    }

} // namespace components::table::operators
