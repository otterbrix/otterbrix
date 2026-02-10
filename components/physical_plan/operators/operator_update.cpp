#include "operator_update.hpp"
#include "predicates/predicate.hpp"

#include <services/collection/collection.hpp>

namespace components::operators {

    operator_update::operator_update(services::collection::context_collection_t* context,
                                     std::pmr::vector<expressions::update_expr_ptr> updates,
                                     bool upsert,
                                     expressions::compare_expression_ptr comp_expr)
        : read_write_operator_t(context, operator_type::update)
        , updates_(std::move(updates))
        , comp_expr_(std::move(comp_expr))
        , upsert_(upsert) {}

    void operator_update::on_execute_impl(pipeline::context_t* pipeline_context) {
        // TODO: worth to create separate update_join operator or mutable_join with callback
        if (left_ && left_->output() && right_ && right_->output()) {
            auto& chunk_left = left_->output()->data_chunk();
            auto& chunk_right = right_->output()->data_chunk();
            auto types_left = chunk_left.types();
            auto types_right = chunk_right.types();
            if (left_->output()->data_chunk().size() == 0 && right_->output()->data_chunk().size() == 0) {
                if (upsert_) {
                    output_ = operators::make_operator_data(context_->resource(), types_left);
                    for (const auto& expr : updates_) {
                        expr->execute(chunk_left, chunk_right, 0, 0, &pipeline_context->parameters);
                    }
                    modified_ = operators::make_operator_write_data(context_->resource());
                    table::table_append_state state(context_->resource());
                    context_->table_storage().table().initialize_append(state);
                    for (size_t id = 0; id < output_->data_chunk().size(); id++) {
                        modified_->append(id + static_cast<size_t>(state.row_start));
                        context_->index_engine()->insert_row(output_->data_chunk(), id, pipeline_context);
                    }
                    context_->table_storage().table().append(output_->data_chunk(), state);
                }
            } else {
                modified_ = operators::make_operator_write_data(context_->resource());
                no_modified_ = operators::make_operator_write_data(context_->resource());
                output_ = operators::make_operator_data(left_->output()->resource(), types_left);
                auto state = context_->table_storage().table().initialize_update({});
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
                            context_->index_engine()->delete_row(chunk_left, i, pipeline_context);
                            bool modified = false;
                            for (const auto& expr : updates_) {
                                modified |= expr->execute(chunk_left, chunk_right, i, j, &pipeline_context->parameters);
                            }
                            if (modified) {
                                modified_->append(i);
                            } else {
                                no_modified_->append(i);
                            }
                            for (size_t k = 0; k < chunk_left.column_count(); k++) {
                                out_chunk.set_value(k, index, chunk_left.value(k, i));
                            }
                            vector::validate_chunk_capacity(out_chunk, ++index);
                            context_->index_engine()->insert_row(chunk_left, i, pipeline_context);
                        }
                    }
                }
                out_chunk.set_cardinality(index);
                context_->table_storage().table().update(*state, out_chunk.row_ids, chunk_left);
            }
        } else if (left_ && left_->output()) {
            if (left_->output()->size() == 0) {
                if (upsert_) {
                    output_ = operators::make_operator_data(context_->resource(),
                                                                  left_->output()->data_chunk().types());

                    table::table_append_state state(context_->resource());
                    context_->table_storage().table().initialize_append(state);
                    context_->table_storage().table().append(output_->data_chunk(), state);
                }
            } else {
                auto& chunk = left_->output()->data_chunk();
                auto types = chunk.types();
                output_ = operators::make_operator_data(left_->output()->resource(), types);
                auto& out_chunk = output_->data_chunk();
                modified_ = operators::make_operator_write_data(context_->resource());
                no_modified_ = operators::make_operator_write_data(context_->resource());
                auto state = context_->table_storage().table().initialize_update({});
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

                        context_->index_engine()->delete_row(chunk, i, pipeline_context);
                        bool modified = false;
                        for (const auto& expr : updates_) {
                            modified |= expr->execute(chunk, chunk, i, i, &pipeline_context->parameters);
                        }
                        if (modified) {
                            modified_->append(i);
                        } else {
                            no_modified_->append(i);
                        }
                        for (size_t j = 0; j < chunk.column_count(); j++) {
                            out_chunk.set_value(j, index, chunk.value(j, i));
                        }
                        vector::validate_chunk_capacity(out_chunk, ++index);
                        context_->index_engine()->insert_row(chunk, i, pipeline_context);
                    }
                }
                out_chunk.set_cardinality(index);
                context_->table_storage().table().update(*state, out_chunk.row_ids, left_->output()->data_chunk());
            }
        }
    }

} // namespace components::operators
