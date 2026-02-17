#include "operator_update.hpp"
#include "predicates/predicate.hpp"
#include <components/vector/vector_operations.hpp>

namespace components::operators {

    operator_update::operator_update(std::pmr::memory_resource* resource, log_t log,
                                     collection_full_name_t name,
                                     std::pmr::vector<expressions::update_expr_ptr> updates,
                                     bool upsert,
                                     expressions::compare_expression_ptr comp_expr)
        : read_write_operator_t(resource, log, operator_type::update)
        , name_(std::move(name))
        , updates_(std::move(updates))
        , comp_expr_(std::move(comp_expr))
        , upsert_(upsert) {}

    void operator_update::on_execute_impl(pipeline::context_t* pipeline_context) {
        // Predicate matching + data prep only — table.update()/table.append() are now handled
        // by executor via send(disk_address_, &manager_disk_t::storage_update/storage_append).
        if (left_ && left_->output() && right_ && right_->output()) {
            auto& chunk_left = left_->output()->data_chunk();
            auto& chunk_right = right_->output()->data_chunk();
            auto types_left = chunk_left.types();
            auto types_right = chunk_right.types();
            if (left_->output()->data_chunk().size() == 0 && right_->output()->data_chunk().size() == 0) {
                if (upsert_) {
                    output_ = operators::make_operator_data(resource(), types_left);
                    for (const auto& expr : updates_) {
                        expr->execute(chunk_left, chunk_right, 0, 0, &pipeline_context->parameters);
                    }
                    modified_ = operators::make_operator_write_data(resource());
                }
            } else {
                modified_ = operators::make_operator_write_data(resource());
                no_modified_ = operators::make_operator_write_data(resource());
                output_ = operators::make_operator_data(left_->output()->resource(), types_left);
                auto& out_chunk = output_->data_chunk();
                auto predicate = comp_expr_ ? predicates::create_predicate(left_->output()->resource(),
                                                                           comp_expr_,
                                                                           types_left,
                                                                           types_right,
                                                                           &pipeline_context->parameters)
                                            : predicates::create_all_true_predicate(left_->output()->resource());
                size_t index = 0;
                for (size_t i = 0; i < chunk_left.size(); i++) {
                    for (size_t j = 0; j < chunk_right.size(); j++) {
                        if (predicate->check(chunk_left, chunk_right, i, j)) {
                            out_chunk.row_ids.data<int64_t>()[index] = chunk_left.row_ids.data<int64_t>()[i];
                            // Copy original values to output first (preserves scan data for executor)
                            for (size_t k = 0; k < chunk_left.column_count(); k++) {
                                vector::vector_ops::copy(chunk_left.data[k], out_chunk.data[k], i + 1, i, index);
                            }
                            bool modified = false;
                            for (const auto& expr : updates_) {
                                modified |= expr->execute(out_chunk, chunk_right, index, j, &pipeline_context->parameters);
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
            }
        } else if (left_ && left_->output()) {
            if (left_->output()->size() == 0) {
                if (upsert_) {
                    output_ = operators::make_operator_data(resource(),
                                                                  left_->output()->data_chunk().types());
                }
            } else {
                auto& chunk = left_->output()->data_chunk();
                auto types = chunk.types();
                output_ = operators::make_operator_data(left_->output()->resource(), types);
                auto& out_chunk = output_->data_chunk();
                modified_ = operators::make_operator_write_data(resource());
                no_modified_ = operators::make_operator_write_data(resource());
                auto predicate = comp_expr_ ? predicates::create_predicate(left_->output()->resource(),
                                                                           comp_expr_,
                                                                           types,
                                                                           types,
                                                                           &pipeline_context->parameters)
                                            : predicates::create_all_true_predicate(left_->output()->resource());
                size_t index = 0;
                for (size_t i = 0; i < chunk.size(); i++) {
                    if (predicate->check(chunk, i)) {
                        if (chunk.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                            out_chunk.row_ids.data<int64_t>()[index] =
                                static_cast<int64_t>(chunk.data.front().indexing().get_index(i));
                        } else {
                            out_chunk.row_ids.data<int64_t>()[index] = chunk.row_ids.data<int64_t>()[i];
                        }

                        // Copy original values to output first (preserves scan data for executor)
                        for (size_t j = 0; j < chunk.column_count(); j++) {
                            vector::vector_ops::copy(chunk.data[j], out_chunk.data[j], i + 1, i, index);
                        }
                        bool modified = false;
                        for (const auto& expr : updates_) {
                            modified |= expr->execute(out_chunk, out_chunk, index, index, &pipeline_context->parameters);
                        }
                        if (modified) {
                            modified_->append(index);
                        } else {
                            no_modified_->append(index);
                        }
                        vector::validate_chunk_capacity(out_chunk, ++index);
                    }
                }
                out_chunk.set_cardinality(index);
            }
        }
    }

} // namespace components::operators
