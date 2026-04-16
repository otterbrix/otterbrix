#include "operator_join.hpp"
#include "predicates/predicate.hpp"

#include <components/vector/vector_operations.hpp>

namespace components::operators {

    namespace {
        // Placeholder columns (produced by projected scans) have no buffer and no auxiliary.
        // They must be skipped when copying — vector_ops::copy would dereference a null data_.
        bool is_placeholder(const vector::vector_t& v) noexcept {
            return v.data() == nullptr && v.auxiliary() == nullptr;
        }
    } // namespace

    operator_join_t::operator_join_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     type join_type,
                                     const expressions::expression_ptr& expression)
        : read_only_operator_t(resource, log, operator_type::join)
        , join_type_(join_type)
        , expression_(expression) {}

    void operator_join_t::on_execute_impl(pipeline::context_t* context) {
        if (!left_ || !right_) {
            return;
        }
        if (left_->output() && right_->output()) {
            const auto& chunk_left = left_->output()->data_chunk();
            const auto& chunk_right = right_->output()->data_chunk();

            // Build joined schema AND the list of projected indices (slots that have data).
            // The join output mirrors the sparse pattern of its inputs so downstream operators
            // see the same placeholder positions, not a fully materialized chunk.
            auto res_types = chunk_left.types();
            auto right_types = chunk_right.types();
            res_types.insert(res_types.end(), right_types.begin(), right_types.end());

            std::vector<size_t> joined_projected;
            joined_projected.reserve(chunk_left.column_count() + chunk_right.column_count());
            for (size_t i = 0; i < chunk_left.column_count(); i++) {
                if (!is_placeholder(chunk_left.data[i])) {
                    joined_projected.push_back(i);
                }
            }
            for (size_t i = 0; i < chunk_right.column_count(); i++) {
                if (!is_placeholder(chunk_right.data[i])) {
                    joined_projected.push_back(chunk_left.column_count() + i);
                }
            }

            if (joined_projected.size() == res_types.size()) {
                // Dense inputs — regular chunk.
                output_ = operators::make_operator_data(left_->output()->resource(), res_types);
            } else {
                // Sparse inputs — build a sparse output chunk directly and hand it to operator_data.
                vector::data_chunk_t sparse_chunk(left_->output()->resource(),
                                                  res_types,
                                                  joined_projected,
                                                  vector::DEFAULT_VECTOR_CAPACITY);
                output_ = operators::make_operator_data(left_->output()->resource(), std::move(sparse_chunk));
            }

            if (log_.is_valid()) {
                trace(log(), "operator_join::left_size(): {}", chunk_left.size());
                trace(log(), "operator_join::right_size(): {}", chunk_right.size());
            }

            indices_left_.clear();
            indices_right_.clear();
            indices_left_.reserve(chunk_left.column_count());
            indices_right_.reserve(chunk_right.column_count());
            for (size_t i = 0; i < chunk_left.column_count(); i++) {
                indices_left_.emplace_back(i);
            }
            for (size_t i = 0; i < chunk_right.column_count(); i++) {
                indices_right_.emplace_back(chunk_left.column_count() + i);
            }

            auto predicate = expression_ ? predicates::create_predicate(left_->output()->resource(),
                                                                        context->function_registry,
                                                                        expression_,
                                                                        chunk_left.types(),
                                                                        chunk_right.types(),
                                                                        &context->parameters)
                                         : predicates::create_all_true_predicate(output_->resource());

            switch (join_type_) {
                case type::inner:
                    inner_join_(predicate, context);
                    break;
                case type::full:
                    outer_full_join_(predicate, context);
                    break;
                case type::left:
                    outer_left_join_(predicate, context);
                    break;
                case type::right:
                    outer_right_join_(predicate, context);
                    break;
                case type::cross:
                    cross_join_(context);
                    break;
                default:
                    break;
            }

            if (log_.is_valid()) {
                trace(log(), "operator_join::result_size(): {}", output_->size());
            }
        }
    }

    void operator_join_t::inner_join_(const predicates::predicate_ptr& predicate, pipeline::context_t*) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();

        std::vector<uint64_t> copy_indices_left;
        std::vector<uint64_t> copy_indices_right;

        size_t res_count = 0;
        for (size_t i = 0; i < chunk_left.size(); i++) {
            auto results = predicates::batch_check_1vN(predicate, chunk_left, chunk_right, i, chunk_right.size());
            if (results.has_error()) {
                set_error(results.error());
                return;
            }
            for (size_t j = 0; j < chunk_right.size(); j++) {
                if (results.value()[j]) {
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
            if (is_placeholder(chunk_left.data[i])) continue;
            vector::vector_ops::copy(chunk_left.data[i],
                                     chunk_res.data[indices_left_.at(i)],
                                     left_indexing,
                                     res_count,
                                     0,
                                     0);
        }
        for (size_t i = 0; i < chunk_right.column_count(); i++) {
            if (is_placeholder(chunk_right.data[i])) continue;
            vector::vector_ops::copy(chunk_right.data[i],
                                     chunk_res.data[indices_right_.at(i)],
                                     right_indexing,
                                     res_count,
                                     0,
                                     0);
        }
        chunk_res.set_cardinality(res_count);
    }

    void operator_join_t::outer_full_join_(const predicates::predicate_ptr& predicate, pipeline::context_t*) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();

        std::vector<bool> visited_right(right_->output()->size(), false);
        std::vector<uint64_t> copy_indices_left;
        std::vector<uint64_t> copy_indices_right;
        std::vector<uint64_t> null_right_positions;
        std::vector<uint64_t> null_left_positions;

        size_t res_count = 0;
        for (size_t i = 0; i < chunk_left.size(); i++) {
            bool visited_left = false;
            auto results = predicates::batch_check_1vN(predicate, chunk_left, chunk_right, i, chunk_right.size());
            if (results.has_error()) {
                set_error(results.error());
                return;
            }
            for (size_t j = 0; j < chunk_right.size(); j++) {
                if (results.value()[j]) {
                    visited_left = true;
                    visited_right[j] = true;
                    copy_indices_left.emplace_back(i);
                    copy_indices_right.emplace_back(j);
                    ++res_count;
                }
            }
            if (!visited_left) {
                copy_indices_left.emplace_back(i);
                copy_indices_right.emplace_back(0);
                null_right_positions.emplace_back(res_count);
                ++res_count;
            }
        }
        for (size_t i = 0; i < visited_right.size(); ++i) {
            if (visited_right[i]) {
                continue;
            }
            copy_indices_left.emplace_back(0);
            copy_indices_right.emplace_back(i);
            null_left_positions.emplace_back(res_count);
            ++res_count;
        }

        vector::validate_chunk_capacity(chunk_res, res_count);
        vector::indexing_vector_t left_indexing(output_->resource(), copy_indices_left.data());
        vector::indexing_vector_t right_indexing(output_->resource(), copy_indices_right.data());
        for (size_t i = 0; i < chunk_left.column_count(); i++) {
            if (is_placeholder(chunk_left.data[i])) continue;
            vector::vector_ops::copy(chunk_left.data[i],
                                     chunk_res.data[indices_left_.at(i)],
                                     left_indexing,
                                     res_count,
                                     0,
                                     0);
            for (auto pos : null_left_positions) {
                chunk_res.data[indices_left_.at(i)].validity().set_invalid(pos);
            }
        }
        for (size_t i = 0; i < chunk_right.column_count(); i++) {
            if (is_placeholder(chunk_right.data[i])) continue;
            vector::vector_ops::copy(chunk_right.data[i],
                                     chunk_res.data[indices_right_.at(i)],
                                     right_indexing,
                                     res_count,
                                     0,
                                     0);
            for (auto pos : null_right_positions) {
                chunk_res.data[indices_right_.at(i)].validity().set_invalid(pos);
            }
        }
        chunk_res.set_cardinality(res_count);
    }

    void operator_join_t::outer_left_join_(const predicates::predicate_ptr& predicate, pipeline::context_t*) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();

        std::vector<uint64_t> copy_indices_left;
        std::vector<uint64_t> copy_indices_right;
        std::vector<uint64_t> null_right_positions;

        size_t res_count = 0;
        for (size_t i = 0; i < chunk_left.size(); i++) {
            bool visited_left = false;
            auto results = predicates::batch_check_1vN(predicate, chunk_left, chunk_right, i, chunk_right.size());
            if (results.has_error()) {
                set_error(results.error());
                return;
            }
            for (size_t j = 0; j < chunk_right.size(); j++) {
                if (results.value()[j]) {
                    visited_left = true;
                    copy_indices_left.emplace_back(i);
                    copy_indices_right.emplace_back(j);
                    ++res_count;
                }
            }
            if (!visited_left) {
                copy_indices_left.emplace_back(i);
                copy_indices_right.emplace_back(0);
                null_right_positions.emplace_back(res_count);
                ++res_count;
            }
        }

        vector::validate_chunk_capacity(chunk_res, res_count);
        vector::indexing_vector_t left_indexing(output_->resource(), copy_indices_left.data());
        vector::indexing_vector_t right_indexing(output_->resource(), copy_indices_right.data());
        for (size_t i = 0; i < chunk_left.column_count(); i++) {
            if (is_placeholder(chunk_left.data[i])) continue;
            vector::vector_ops::copy(chunk_left.data[i],
                                     chunk_res.data[indices_left_.at(i)],
                                     left_indexing,
                                     res_count,
                                     0,
                                     0);
        }
        for (size_t i = 0; i < chunk_right.column_count(); i++) {
            if (is_placeholder(chunk_right.data[i])) continue;
            vector::vector_ops::copy(chunk_right.data[i],
                                     chunk_res.data[indices_right_.at(i)],
                                     right_indexing,
                                     res_count,
                                     0,
                                     0);
            for (auto pos : null_right_positions) {
                chunk_res.data[indices_right_.at(i)].validity().set_invalid(pos);
            }
        }
        chunk_res.set_cardinality(res_count);
    }

    void operator_join_t::outer_right_join_(const predicates::predicate_ptr& predicate, pipeline::context_t*) {
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();
        auto& chunk_res = output_->data_chunk();

        std::vector<uint64_t> copy_indices_left;
        std::vector<uint64_t> copy_indices_right;
        std::vector<uint64_t> null_left_positions;

        size_t res_count = 0;
        for (size_t i = 0; i < chunk_right.size(); i++) {
            bool visited_right = false;
            auto results = predicates::batch_check_Nv1(predicate, chunk_left, chunk_right, chunk_left.size(), i);
            if (results.has_error()) {
                set_error(results.error());
                return;
            }
            for (size_t j = 0; j < chunk_left.size(); j++) {
                if (results.value()[j]) {
                    visited_right = true;
                    copy_indices_left.emplace_back(j);
                    copy_indices_right.emplace_back(i);
                    ++res_count;
                }
            }
            if (!visited_right) {
                copy_indices_left.emplace_back(0);
                copy_indices_right.emplace_back(i);
                null_left_positions.emplace_back(res_count);
                ++res_count;
            }
        }

        vector::validate_chunk_capacity(chunk_res, res_count);
        vector::indexing_vector_t left_indexing(output_->resource(), copy_indices_left.data());
        vector::indexing_vector_t right_indexing(output_->resource(), copy_indices_right.data());
        for (size_t i = 0; i < chunk_left.column_count(); i++) {
            if (is_placeholder(chunk_left.data[i])) continue;
            vector::vector_ops::copy(chunk_left.data[i],
                                     chunk_res.data[indices_left_.at(i)],
                                     left_indexing,
                                     res_count,
                                     0,
                                     0);
            for (auto pos : null_left_positions) {
                chunk_res.data[indices_left_.at(i)].validity().set_invalid(pos);
            }
        }
        for (size_t i = 0; i < chunk_right.column_count(); i++) {
            if (is_placeholder(chunk_right.data[i])) continue;
            vector::vector_ops::copy(chunk_right.data[i],
                                     chunk_res.data[indices_right_.at(i)],
                                     right_indexing,
                                     res_count,
                                     0,
                                     0);
        }
        chunk_res.set_cardinality(res_count);
    }

    void operator_join_t::cross_join_(pipeline::context_t*) {
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
            if (is_placeholder(chunk_left.data[i])) continue;
            vector::vector_ops::copy(chunk_left.data[i],
                                     chunk_res.data[indices_left_.at(i)],
                                     left_indexing,
                                     res_count,
                                     0,
                                     0);
        }
        for (size_t i = 0; i < chunk_right.column_count(); i++) {
            if (is_placeholder(chunk_right.data[i])) continue;
            vector::vector_ops::copy(chunk_right.data[i],
                                     chunk_res.data[indices_right_.at(i)],
                                     right_indexing,
                                     res_count,
                                     0,
                                     0);
        }
        chunk_res.set_cardinality(res_count);
    }

} // namespace components::operators
