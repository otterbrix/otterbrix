#include "operator_delete.hpp"
#include "predicates/predicate.hpp"

namespace components::operators {

    operator_delete::operator_delete(std::pmr::memory_resource* resource,
                                     log_t log,
                                     collection_full_name_t name,
                                     expressions::expression_ptr expr)
        : read_write_operator_t(resource, log, operator_type::remove)
        , name_(std::move(name))
        , expression_(std::move(expr)) {}

    void operator_delete::on_execute_impl(pipeline::context_t* pipeline_context) {
        // Predicate matching only — table.delete_rows() is now handled by
        // await_async_and_resume via send(disk_address_, &manager_disk_t::storage_delete_rows).
        if (left_ && left_->output() && right_ && right_->output()) {
            auto* resource = left_->output()->resource();
            modified_ = operators::make_operator_write_data(resource);
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

            auto predicate = expression_ ? predicates::create_predicate(resource,
                                                                        pipeline_context->function_registry,
                                                                        expression_,
                                                                        types_left,
                                                                        types_right,
                                                                        &pipeline_context->parameters)
                                         : predicates::create_all_true_predicate(resource);

            size_t matches = 0;
            for (const auto& chunk_left : left_chunks) {
                for (size_t i = 0; i < chunk_left.size(); ++i) {
                    for (const auto& chunk_right : right_chunks) {
                        auto results =
                            predicates::batch_check_1vN(predicate, chunk_left, chunk_right, i, chunk_right.size());
                        if (results.has_error()) {
                            set_error(results.error());
                            return;
                        }
                        for (size_t j = 0; j < chunk_right.size(); ++j) {
                            if (results.value()[j]) {
                                modified_->append(static_cast<size_t>(i));
                                ++matches;
                            }
                        }
                    }
                }
            }
            for (const auto& type : types_left) {
                modified_->updated_types_map()[{std::pmr::string(type.alias(), resource), type}] += matches;
            }
        } else if (left_ && left_->output()) {
            auto* resource = left_->output()->resource();
            modified_ = operators::make_operator_write_data(resource);
            const auto& in_chunks = left_->output()->chunks();

            std::pmr::vector<types::complex_logical_type> types(resource);
            if (!in_chunks.empty()) {
                types = in_chunks.front().types();
            }

            auto predicate = expression_ ? predicates::create_predicate(resource,
                                                                        pipeline_context->function_registry,
                                                                        expression_,
                                                                        types,
                                                                        types,
                                                                        &pipeline_context->parameters)
                                         : predicates::create_all_true_predicate(resource);

            size_t matches = 0;
            for (const auto& chunk : in_chunks) {
                for (size_t i = 0; i < chunk.size(); ++i) {
                    auto res = predicate->check(chunk, i);
                    if (res.has_error()) {
                        set_error(res.error());
                        return;
                    }
                    if (res.value()) {
                        size_t id;
                        if (chunk.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                            id = static_cast<size_t>(chunk.data.front().indexing().get_index(i));
                        } else {
                            id = static_cast<size_t>(chunk.row_ids.data<int64_t>()[i]);
                        }
                        modified_->append(id);
                        ++matches;
                    }
                }
            }
            for (const auto& type : types) {
                modified_->updated_types_map()[{std::pmr::string(type.alias(), resource), type}] += matches;
            }
        }

        if (modified_ && modified_->size() > 0 && !name_.empty()) {
            async_wait();
        }
    }

} // namespace components::operators
