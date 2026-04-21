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

    void operator_update::on_execute_impl(pipeline::context_t* pipeline_context) {
        // Predicate matching + data prep only — storage I/O is handled by await_async_and_resume.
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
                    auto& out_chunk = output_->data_chunk();
                    // upsert path: synthesise a row by running update exprs against an empty context.
                    vector::data_chunk_t empty_left(resource, types_left);
                    vector::data_chunk_t empty_right(resource, types_right);
                    for (const auto& expr : updates_) {
                        expr->execute(empty_left, empty_right, 0, 0, &pipeline_context->parameters);
                    }
                    (void)out_chunk;
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
                                                                      &pipeline_context->parameters)
                                       : predicates::create_all_true_predicate(resource);

                std::vector<vector::data_chunk_t> out_chunks;
                out_chunks.reserve(left_chunks.size());

                for (auto& chunk_left : left_chunks) {
                    if (chunk_left.size() == 0) {
                        continue;
                    }
                    vector::data_chunk_t out_chunk(resource, types_left, chunk_left.size());
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
                                    vector::vector_ops::copy(chunk_left.data[k],
                                                             out_chunk.data[k],
                                                             i + 1,
                                                             i,
                                                             index);
                                }
                                bool modified = false;
                                for (const auto& expr : updates_) {
                                    modified |= expr->execute(out_chunk,
                                                              chunk_right,
                                                              index,
                                                              j,
                                                              &pipeline_context->parameters);
                                }
                                if (modified) {
                                    modified_->append(index);
                                } else {
                                    no_modified_->append(index);
                                }
                                vector::validate_chunk_capacity(out_chunk, ++index);
                            }
                        }
                    }
                    out_chunk.set_cardinality(index);
                    if (index > 0) {
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
                                                                      &pipeline_context->parameters)
                                       : predicates::create_all_true_predicate(resource);

                std::vector<vector::data_chunk_t> out_chunks;
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
                        bool modified = false;
                        for (const auto& expr : updates_) {
                            modified |=
                                expr->execute(out_chunk, out_chunk, index, index, &pipeline_context->parameters);
                        }
                        if (modified) {
                            modified_->append(index);
                        } else {
                            no_modified_->append(index);
                        }
                        vector::validate_chunk_capacity(out_chunk, ++index);
                    }
                    out_chunk.set_cardinality(index);
                    if (index > 0) {
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
