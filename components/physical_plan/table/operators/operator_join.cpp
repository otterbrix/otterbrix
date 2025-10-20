#include "operator_join.hpp"

#include <components/vector/vector_operations.hpp>
#include <services/collection/collection.hpp>
#include <vector>

namespace components::table::operators {

    operator_join_t::operator_join_t(services::collection::context_collection_t* context,
                                     type join_type,
                                     const expressions::compare_expression_ptr& expression)
        : read_only_operator_t(context, operator_type::join)
        , join_type_(join_type)
        , expression_(std::move(expression)) {}

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
            std::unordered_map<std::string, size_t> name_index_map_res;
            name_index_map_res.clear();
            for (size_t i = 0; i < res_types.size(); i++) {
                name_index_map_res.emplace(res_types[i].alias(), i);
            }
            indices_left_.clear();
            indices_right_.clear();
            indices_left_.reserve(chunk_left.column_count());
            indices_right_.reserve(chunk_right.column_count());
            for (const auto& column : chunk_left.data) {
                indices_left_.emplace_back(name_index_map_res.at(column.type().alias()));
            }
            for (const auto& column : chunk_right.data) {
                indices_right_.emplace_back(name_index_map_res.at(column.type().alias()));
            }

            auto predicate = expression_ ? predicates::create_predicate(expression_,
                                                                        chunk_left.types(),
                                                                        chunk_right.types(),
                                                                        &context->parameters)
                                         : predicates::create_all_true_predicate(output_->resource());

            switch (join_type_) {
                case type::inner:
                    inner_join_(std::move(predicate), context);
                    break;
                case type::full:
                    outer_full_join_(std::move(predicate), context);
                    break;
                case type::left:
                    outer_left_join_(std::move(predicate), context);
                    break;
                case type::right:
                    outer_right_join_(std::move(predicate), context);
                    break;
                case type::cross:
                    cross_join_(std::move(predicate), context);
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

    void operator_join_t::inner_join_(predicates::predicate_ptr predicate, pipeline::context_t* context) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();

        std::vector<uint64_t> copy_indices_left;
        std::vector<uint64_t> copy_indices_right;

        size_t res_count = 0;
        for (size_t i = 0; i < chunk_left.size(); i++) {
            for (size_t j = 0; j < chunk_right.size(); j++) {
                if (predicate->check(chunk_left, chunk_right, i, j)) {
                    copy_indices_left.emplace_back(i);
                    copy_indices_right.emplace_back(j);
                    ++res_count;
                }
            }
        }

        vector::validate_chunk_capacity(chunk_res, res_count);
        vector::indexing_vector_t left_indexing(output_->resource(), copy_indices_left.data());
        vector::indexing_vector_t right_indexing(output_->resource(), copy_indices_right.data());
        for (size_t i = 0; i < chunk_left.column_count(); i++) {
            vector::vector_ops::copy(chunk_left.data[i],
                                     chunk_res.data[indices_left_.at(i)],
                                     left_indexing,
                                     res_count,
                                     0,
                                     0);
        }
        for (size_t i = 0; i < chunk_right.column_count(); i++) {
            vector::vector_ops::copy(chunk_right.data[i],
                                     chunk_res.data[indices_right_.at(i)],
                                     right_indexing,
                                     res_count,
                                     0,
                                     0);
        }
        chunk_res.set_cardinality(res_count);
    }

    void operator_join_t::outer_full_join_(predicates::predicate_ptr predicate, pipeline::context_t* context) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();

        std::vector<bool> visited_right(right_->output()->size(), false);
        std::vector<uint64_t> copy_indices_left;
        std::vector<uint64_t> copy_indices_right;

        size_t res_count = 0;
        for (size_t i = 0; i < chunk_left.size(); i++) {
            bool visited_left = false;
            size_t right_index = 0;
            for (size_t j = 0; j < chunk_right.size(); j++) {
                if (predicate->check(chunk_left, chunk_right, i, j)) {
                    visited_left = true;
                    visited_right[right_index] = true;
                    copy_indices_left.emplace_back(i);
                    copy_indices_right.emplace_back(j);
                    ++res_count;
                }
                right_index++;
                if (!visited_left) {
                    copy_indices_left.emplace_back(i);
                    copy_indices_right.emplace_back(std::numeric_limits<uint64_t>::max());
                    ++res_count;
                }
            }
        }
        for (size_t i = 0; i < visited_right.size(); ++i) {
            if (visited_right[i]) {
                continue;
            }
            copy_indices_left.emplace_back(std::numeric_limits<uint64_t>::max());
            copy_indices_right.emplace_back(i);
            ++res_count;
        }

        vector::validate_chunk_capacity(chunk_res, res_count);
        vector::indexing_vector_t left_indexing(output_->resource(), copy_indices_left.data());
        vector::indexing_vector_t right_indexing(output_->resource(), copy_indices_right.data());
        for (size_t i = 0; i < chunk_left.column_count(); i++) {
            vector::vector_ops::copy(chunk_left.data[i],
                                     chunk_res.data[indices_left_.at(i)],
                                     left_indexing,
                                     res_count,
                                     0,
                                     0);
        }
        for (size_t i = 0; i < chunk_right.column_count(); i++) {
            vector::vector_ops::copy(chunk_right.data[i],
                                     chunk_res.data[indices_right_.at(i)],
                                     right_indexing,
                                     res_count,
                                     0,
                                     0);
        }
        chunk_res.set_cardinality(res_count);
    }

    void operator_join_t::outer_left_join_(predicates::predicate_ptr predicate, pipeline::context_t* context) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();

        std::vector<uint64_t> copy_indices_left;
        std::vector<uint64_t> copy_indices_right;

        size_t res_count = 0;
        for (size_t i = 0; i < chunk_left.size(); i++) {
            bool visited_left = false;
            for (size_t j = 0; j < chunk_right.size(); j++) {
                if (predicate->check(chunk_left, chunk_right, i, j)) {
                    visited_left = true;
                    copy_indices_left.emplace_back(i);
                    copy_indices_right.emplace_back(j);
                    ++res_count;
                }
                if (!visited_left) {
                    copy_indices_left.emplace_back(i);
                    copy_indices_right.emplace_back(std::numeric_limits<uint64_t>::max());
                    ++res_count;
                }
            }
        }

        vector::validate_chunk_capacity(chunk_res, res_count);
        vector::indexing_vector_t left_indexing(output_->resource(), copy_indices_left.data());
        vector::indexing_vector_t right_indexing(output_->resource(), copy_indices_right.data());
        for (size_t i = 0; i < chunk_left.column_count(); i++) {
            vector::vector_ops::copy(chunk_left.data[i],
                                     chunk_res.data[indices_left_.at(i)],
                                     left_indexing,
                                     res_count,
                                     0,
                                     0);
        }
        for (size_t i = 0; i < chunk_right.column_count(); i++) {
            vector::vector_ops::copy(chunk_right.data[i],
                                     chunk_res.data[indices_right_.at(i)],
                                     right_indexing,
                                     res_count,
                                     0,
                                     0);
        }
        chunk_res.set_cardinality(res_count);
    }

    void operator_join_t::outer_right_join_(predicates::predicate_ptr predicate, pipeline::context_t* context) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();

        std::vector<uint64_t> copy_indices_left;
        std::vector<uint64_t> copy_indices_right;

        size_t res_count = 0;
        for (size_t i = 0; i < chunk_right.size(); i++) {
            bool visited_right = false;
            for (size_t j = 0; j < chunk_left.size(); j++) {
                if (predicate->check(chunk_left, chunk_right, i, j)) {
                    visited_right = true;
                    copy_indices_left.emplace_back(i);
                    copy_indices_right.emplace_back(j);
                    ++res_count;
                }
                if (!visited_right) {
                    copy_indices_left.emplace_back(std::numeric_limits<uint64_t>::max());
                    copy_indices_right.emplace_back(j);
                    ++res_count;
                }
            }
        }

        vector::validate_chunk_capacity(chunk_res, res_count);
        vector::indexing_vector_t left_indexing(output_->resource(), copy_indices_left.data());
        vector::indexing_vector_t right_indexing(output_->resource(), copy_indices_right.data());
        for (size_t i = 0; i < chunk_left.column_count(); i++) {
            vector::vector_ops::copy(chunk_left.data[i],
                                     chunk_res.data[indices_left_.at(i)],
                                     left_indexing,
                                     res_count,
                                     0,
                                     0);
        }
        for (size_t i = 0; i < chunk_right.column_count(); i++) {
            vector::vector_ops::copy(chunk_right.data[i],
                                     chunk_res.data[indices_right_.at(i)],
                                     right_indexing,
                                     res_count,
                                     0,
                                     0);
        }
        chunk_res.set_cardinality(res_count);
    }

    void operator_join_t::cross_join_(predicates::predicate_ptr, pipeline::context_t* context) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();

        size_t res_count = chunk_left.size() * chunk_right.size();
        std::vector<uint64_t> copy_indices_left;
        std::vector<uint64_t> copy_indices_right;
        copy_indices_left.reserve(res_count);
        copy_indices_right.reserve(res_count);

        for (size_t i = 0; i < chunk_left.size(); i++) {
            for (size_t j = 0; j < chunk_right.size(); j++) {
                copy_indices_left.emplace_back(i);
                copy_indices_right.emplace_back(j);
            }
        }

        vector::validate_chunk_capacity(chunk_res, res_count);
        vector::indexing_vector_t left_indexing(output_->resource(), copy_indices_left.data());
        vector::indexing_vector_t right_indexing(output_->resource(), copy_indices_right.data());
        for (size_t i = 0; i < chunk_left.column_count(); i++) {
            vector::vector_ops::copy(chunk_left.data[i],
                                     chunk_res.data[indices_left_.at(i)],
                                     left_indexing,
                                     res_count,
                                     0,
                                     0);
        }
        for (size_t i = 0; i < chunk_right.column_count(); i++) {
            vector::vector_ops::copy(chunk_right.data[i],
                                     chunk_res.data[indices_right_.at(i)],
                                     right_indexing,
                                     res_count,
                                     0,
                                     0);
        }
        chunk_res.set_cardinality(res_count);
    }

} // namespace components::table::operators
