#include "operator_update.hpp"
#include "predicates/predicate.hpp"
#include <components/vector/vector_operations.hpp>

namespace components::operators {

    operator_update::operator_update(std::pmr::memory_resource* resource,
                                     log_t log,
                                     collection_full_name_t name,
                                     std::pmr::vector<expressions::update_expr_ptr> updates,
                                     bool upsert,
                                     expressions::expression_ptr expr)
        : read_write_operator_t(resource, log, operator_type::update)
        , name_(std::move(name))
        , updates_(std::move(updates))
        , expr_(std::move(expr))
        , upsert_(upsert) {}

    namespace {
        // Applies all update expressions to out_chunk[0..match_count) and
        // populates modified_/no_modified_ lists.
        void apply_updates(std::pmr::memory_resource* resource,
                           const std::pmr::vector<expressions::update_expr_ptr>& updates,
                           vector::data_chunk_t& out_chunk,
                           const vector::data_chunk_t& from_chunk,
                           uint64_t match_count,
                           const logical_plan::storage_parameters& parameters,
                           core::date::timezone_offset_t session_tz,
                           operators::operator_write_data_ptr& modified,
                           operators::operator_write_data_ptr& no_modified) {
            std::pmr::vector<bool> any_modified(match_count, false, resource);
            for (const auto& expr : updates) {
                auto row_flags = expr->execute(resource, out_chunk, from_chunk, match_count, &parameters, session_tz);
                for (uint64_t i = 0; i < match_count; i++) {
                    if (i < row_flags.size() && row_flags[i]) {
                        any_modified[i] = true;
                    }
                }
            }
            for (uint64_t i = 0; i < match_count; i++) {
                if (any_modified[i]) {
                    modified->append(i);
                } else {
                    no_modified->append(i);
                }
            }
        }
    } // anonymous namespace

    void operator_update::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (left_ && left_->output() && right_ && right_->output()) {
            auto* resource = left_->output()->resource();
            const auto& left_chunks = left_->output()->chunks();
            const auto& right_chunks = right_->output()->chunks();

            std::pmr::vector<types::complex_logical_type> types_left(resource);
            std::pmr::vector<types::complex_logical_type> types_right(resource);
            if (!left_chunks.empty()) {
                types_left = left_chunks.front().types();
            }
            if (!right_chunks.empty()) {
                types_right = right_chunks.front().types();
            }

            const uint64_t left_size = left_->output()->size();
            const uint64_t right_size = right_->output()->size();

            if (left_size == 0 && right_size == 0) {
                if (upsert_) {
                    output_ = operators::make_operator_data(resource, types_left);
                    // upsert path: synthesise a row by running update exprs against an empty context.
                    vector::data_chunk_t empty_left(resource, types_left);
                    vector::data_chunk_t empty_right(resource, types_right);
                    for (const auto& expr : updates_) {
                        expr->execute(resource,
                                      empty_left,
                                      empty_right,
                                      0,
                                      &pipeline_context->parameters,
                                      pipeline_context->session_tz);
                    }
                    modified_ = operators::make_operator_write_data(resource);
                }
            } else {
                modified_ = operators::make_operator_write_data(resource);
                no_modified_ = operators::make_operator_write_data(resource);

                auto predicate = expr_ ? predicates::create_predicate(resource,
                                                                      pipeline_context->function_registry,
                                                                      expr_,
                                                                      types_left,
                                                                      types_right,
                                                                      &pipeline_context->parameters,
                                                                      pipeline_context->session_tz)
                                       : predicates::create_all_true_predicate(resource);

                chunks_vector_t out_chunks(resource);
                out_chunks.reserve(left_chunks.size());

                for (auto& chunk_left : left_chunks) {
                    if (chunk_left.size() == 0) {
                        continue;
                    }
                    vector::data_chunk_t out_chunk(resource, types_left, chunk_left.size());
                    vector::data_chunk_t right_chunk(resource, types_right, chunk_left.size());
                    size_t index = 0;
                    for (size_t i = 0; i < chunk_left.size(); ++i) {
                        for (const auto& chunk_right : right_chunks) {
                            if (chunk_right.size() == 0) {
                                continue;
                            }
                            auto results =
                                predicates::batch_check_1vN(predicate, chunk_left, chunk_right, i, chunk_right.size());
                            if (results.has_error()) {
                                set_error(results.error());
                                return;
                            }
                            for (size_t j = 0; j < chunk_right.size(); ++j) {
                                if (!results.value()[j]) {
                                    continue;
                                }
                                out_chunk.row_ids.data<int64_t>()[index] = chunk_left.row_ids.data<int64_t>()[i];
                                for (size_t k = 0; k < chunk_left.column_count(); ++k) {
                                    vector::vector_ops::copy(chunk_left.data[k], out_chunk.data[k], i + 1, i, index);
                                }
                                for (size_t k = 0; k < chunk_right.column_count(); ++k) {
                                    vector::vector_ops::copy(chunk_right.data[k], right_chunk.data[k], j + 1, j, index);
                                }
                                ++index;
                                vector::validate_chunk_capacity(out_chunk, index);
                                vector::validate_chunk_capacity(right_chunk, index);
                            }
                        }
                    }
                    out_chunk.set_cardinality(index);
                    right_chunk.set_cardinality(index);
                    if (index > 0) {
                        apply_updates(resource,
                                      updates_,
                                      out_chunk,
                                      right_chunk,
                                      index,
                                      pipeline_context->parameters,
                                      pipeline_context->session_tz,
                                      modified_,
                                      no_modified_);
                        out_chunks.emplace_back(std::move(out_chunk));
                    }
                }
                if (out_chunks.empty()) {
                    output_ = operators::make_operator_data(resource, types_left, 0);
                } else {
                    output_ = operators::make_operator_data(resource, std::move(out_chunks));
                }
            }
        } else if (left_ && left_->output()) {
            auto* resource = left_->output()->resource();
            const auto& in_chunks = left_->output()->chunks();
            std::pmr::vector<types::complex_logical_type> types(resource);
            if (!in_chunks.empty()) {
                types = in_chunks.front().types();
            }

            if (left_->output()->size() == 0) {
                if (upsert_) {
                    output_ = operators::make_operator_data(resource, types);
                }
            } else {
                modified_ = operators::make_operator_write_data(resource);
                no_modified_ = operators::make_operator_write_data(resource);

                auto predicate = expr_ ? predicates::create_predicate(resource,
                                                                      pipeline_context->function_registry,
                                                                      expr_,
                                                                      types,
                                                                      types,
                                                                      &pipeline_context->parameters,
                                                                      pipeline_context->session_tz)
                                       : predicates::create_all_true_predicate(resource);

                chunks_vector_t out_chunks(resource);
                out_chunks.reserve(in_chunks.size());

                for (auto& chunk : in_chunks) {
                    if (chunk.size() == 0) {
                        continue;
                    }
                    vector::data_chunk_t out_chunk(resource, types, chunk.size());
                    size_t index = 0;
                    for (size_t i = 0; i < chunk.size(); ++i) {
                        auto res = predicate->check(chunk, i);
                        if (res.has_error()) {
                            set_error(res.error());
                            return;
                        }
                        if (!res.value()) {
                            continue;
                        }
                        if (chunk.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                            out_chunk.row_ids.data<int64_t>()[index] =
                                static_cast<int64_t>(chunk.data.front().indexing().get_index(i));
                        } else {
                            out_chunk.row_ids.data<int64_t>()[index] = chunk.row_ids.data<int64_t>()[i];
                        }
                        for (size_t k = 0; k < chunk.column_count(); ++k) {
                            vector::vector_ops::copy(chunk.data[k], out_chunk.data[k], i + 1, i, index);
                        }
                        vector::validate_chunk_capacity(out_chunk, ++index);
                    }
                    out_chunk.set_cardinality(index);
                    if (index > 0) {
                        apply_updates(resource,
                                      updates_,
                                      out_chunk,
                                      out_chunk,
                                      index,
                                      pipeline_context->parameters,
                                      pipeline_context->session_tz,
                                      modified_,
                                      no_modified_);
                        out_chunks.emplace_back(std::move(out_chunk));
                    }
                }
                if (out_chunks.empty()) {
                    output_ = operators::make_operator_data(resource, types, 0);
                } else {
                    output_ = operators::make_operator_data(resource, std::move(out_chunks));
                }
            }
        }

        if (output_ && modified_ && modified_->size() > 0 && !name_.empty()) {
            async_wait();
        }
    }

} // namespace components::operators
