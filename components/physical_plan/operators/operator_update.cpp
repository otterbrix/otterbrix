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
            auto& chunk_left = left_->output()->data_chunk();
            auto& chunk_right = right_->output()->data_chunk();
            auto types_left = chunk_left.types();
            auto types_right = chunk_right.types();

            if (chunk_left.empty() && chunk_right.empty()) {
                if (upsert_) {
                    output_ = operators::make_operator_data(resource(), types_left);
                    modified_ = operators::make_operator_write_data(resource());
                    no_modified_ = operators::make_operator_write_data(resource());
                    auto& out_chunk = output_->data_chunk();
                    // Build an empty right-side chunk with matching type layout
                    auto right_placeholder = operators::make_operator_data(resource(), types_right);
                    apply_updates(resource(),
                                  updates_,
                                  out_chunk,
                                  right_placeholder->data_chunk(),
                                  0,
                                  pipeline_context->parameters,
                                  pipeline_context->session_tz,
                                  modified_,
                                  no_modified_);
                }
            } else {
                output_ = operators::make_operator_data(left_->output()->resource(), types_left);
                modified_ = operators::make_operator_write_data(resource());
                no_modified_ = operators::make_operator_write_data(resource());
                auto& out_chunk = output_->data_chunk();

                // Build ordered right-side chunk so both chunks are row-aligned after matching.
                auto right_ordered = operators::make_operator_data(resource(), types_right);
                auto& right_ordered_chunk = right_ordered->data_chunk();

                auto predicate = expr_ ? predicates::create_predicate(left_->output()->resource(),
                                                                      pipeline_context->function_registry,
                                                                      expr_,
                                                                      types_left,
                                                                      types_right,
                                                                      &pipeline_context->parameters,
                                                                      pipeline_context->session_tz)
                                       : predicates::create_all_true_predicate(left_->output()->resource());

                uint64_t match_count = 0;
                for (size_t i = 0; i < chunk_left.size(); i++) {
                    auto results =
                        predicates::batch_check_1vN(predicate, chunk_left, chunk_right, i, chunk_right.size());
                    if (results.has_error()) {
                        set_error(results.error());
                        return;
                    }
                    for (size_t j = 0; j < chunk_right.size(); j++) {
                        if (results.value()[j]) {
                            out_chunk.row_ids.data<int64_t>()[match_count] = chunk_left.row_ids.data<int64_t>()[i];
                            for (size_t k = 0; k < chunk_left.column_count(); k++) {
                                vector::vector_ops::copy(chunk_left.data[k], out_chunk.data[k], i + 1, i, match_count);
                            }
                            for (size_t k = 0; k < chunk_right.column_count(); k++) {
                                vector::vector_ops::copy(chunk_right.data[k],
                                                         right_ordered_chunk.data[k],
                                                         j + 1,
                                                         j,
                                                         match_count);
                            }
                            vector::validate_chunk_capacity(out_chunk, match_count + 1);
                            vector::validate_chunk_capacity(right_ordered_chunk, ++match_count);
                        }
                    }
                }
                out_chunk.set_cardinality(match_count);
                right_ordered_chunk.set_cardinality(match_count);

                apply_updates(resource(),
                              updates_,
                              out_chunk,
                              right_ordered_chunk,
                              match_count,
                              pipeline_context->parameters,
                              pipeline_context->session_tz,
                              modified_,
                              no_modified_);
            }
        } else if (left_ && left_->output()) {
            if (left_->output()->size() == 0) {
                if (upsert_) {
                    output_ = operators::make_operator_data(resource(), left_->output()->data_chunk().types());
                    modified_ = operators::make_operator_write_data(resource());
                    no_modified_ = operators::make_operator_write_data(resource());
                }
            } else {
                auto& chunk = left_->output()->data_chunk();
                auto types = chunk.types();
                output_ = operators::make_operator_data(left_->output()->resource(), types);
                modified_ = operators::make_operator_write_data(resource());
                no_modified_ = operators::make_operator_write_data(resource());
                auto& out_chunk = output_->data_chunk();

                auto predicate = expr_ ? predicates::create_predicate(left_->output()->resource(),
                                                                      pipeline_context->function_registry,
                                                                      expr_,
                                                                      types,
                                                                      types,
                                                                      &pipeline_context->parameters,
                                                                      pipeline_context->session_tz)
                                       : predicates::create_all_true_predicate(left_->output()->resource());

                uint64_t match_count = 0;
                for (size_t i = 0; i < chunk.size(); i++) {
                    auto res = predicate->check(chunk, i);
                    if (!res.has_error() && res.value()) {
                        if (chunk.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                            out_chunk.row_ids.data<int64_t>()[match_count] =
                                static_cast<int64_t>(chunk.data.front().indexing().get_index(i));
                        } else {
                            out_chunk.row_ids.data<int64_t>()[match_count] = chunk.row_ids.data<int64_t>()[i];
                        }
                        for (size_t j = 0; j < chunk.column_count(); j++) {
                            vector::vector_ops::copy(chunk.data[j], out_chunk.data[j], i + 1, i, match_count);
                        }
                        vector::validate_chunk_capacity(out_chunk, ++match_count);
                    }
                }
                out_chunk.set_cardinality(match_count);

                apply_updates(resource(),
                              updates_,
                              out_chunk,
                              out_chunk,
                              match_count,
                              pipeline_context->parameters,
                              pipeline_context->session_tz,
                              modified_,
                              no_modified_);
            }
        }

        if (output_ && modified_ && modified_->size() > 0 && !name_.empty()) {
            async_wait();
        }
    }

} // namespace components::operators
