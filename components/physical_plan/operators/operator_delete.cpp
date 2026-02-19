#include "operator_delete.hpp"
#include "predicates/predicate.hpp"

#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <components/context/execution_context.hpp>

namespace components::operators {

    operator_delete::operator_delete(std::pmr::memory_resource* resource, log_t log,
                                     collection_full_name_t name,
                                     expressions::compare_expression_ptr expr)
        : read_write_operator_t(resource, log, operator_type::remove)
        , name_(std::move(name))
        , compare_expression_(std::move(expr)) {}

    void operator_delete::on_execute_impl(pipeline::context_t* pipeline_context) {
        // Predicate matching only — table.delete_rows() is now handled by
        // await_async_and_resume via send(disk_address_, &manager_disk_t::storage_delete_rows).
        if (left_ && left_->output() && right_ && right_->output()) {
            modified_ = operators::make_operator_write_data(left_->output()->resource());
            auto& chunk_left = left_->output()->data_chunk();
            auto& chunk_right = right_->output()->data_chunk();
            auto types_left = chunk_left.types();
            auto types_right = chunk_right.types();
            auto ids_capacity = vector::DEFAULT_VECTOR_CAPACITY;
            vector::vector_t ids(left_->output()->resource(), types::logical_type::BIGINT, ids_capacity);
            auto predicate = compare_expression_ ? predicates::create_predicate(left_->output()->resource(),
                                                                                compare_expression_,
                                                                                types_left,
                                                                                types_right,
                                                                                &pipeline_context->parameters)
                                                 : predicates::create_all_true_predicate(left_->output()->resource());

            size_t index = 0;
            for (size_t i = 0; i < chunk_left.size(); i++) {
                for (size_t j = 0; j < chunk_right.size(); j++) {
                    if (predicate->check(chunk_left, chunk_right, i, j)) {
                        ids.data<int64_t>()[index++] = static_cast<int64_t>(i);
                        if (index >= ids_capacity) {
                            ids.resize(ids_capacity, ids_capacity * 2);
                            ids_capacity *= 2;
                        }
                    }
                }
            }
            for (size_t i = 0; i < index; i++) {
                size_t id = static_cast<size_t>(ids.data<int64_t>()[i]);
                modified_->append(id);
            }
            for (const auto& type : types_left) {
                modified_->updated_types_map()[{std::pmr::string(type.alias(), left_->output()->resource()), type}] +=
                    index;
            }
        } else if (left_ && left_->output()) {
            modified_ = operators::make_operator_write_data(left_->output()->resource());
            auto& chunk = left_->output()->data_chunk();
            auto types = chunk.types();

            vector::vector_t ids(left_->output()->resource(), types::logical_type::BIGINT, chunk.size());
            auto predicate = compare_expression_ ? predicates::create_predicate(left_->output()->resource(),
                                                                                compare_expression_,
                                                                                types,
                                                                                types,
                                                                                &pipeline_context->parameters)
                                                 : predicates::create_all_true_predicate(left_->output()->resource());

            size_t index = 0;
            for (size_t i = 0; i < chunk.size(); i++) {
                if (predicate->check(chunk, i)) {
                    if (chunk.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                        ids.data<int64_t>()[index++] = static_cast<int64_t>(chunk.data.front().indexing().get_index(i));
                    } else {
                        ids.data<int64_t>()[index++] = chunk.row_ids.data<int64_t>()[i];
                    }
                }
            }
            ids.resize(chunk.size(), index);
            for (size_t i = 0; i < index; i++) {
                size_t id = static_cast<size_t>(ids.data<int64_t>()[i]);
                modified_->append(id);
            }
            for (const auto& type : types) {
                modified_->updated_types_map()[{std::pmr::string(type.alias(), left_->output()->resource()), type}] +=
                    index;
            }
        }

        if (modified_ && modified_->size() > 0 && !name_.empty()) {
            async_wait();
        }
    }

    actor_zeta::unique_future<void> operator_delete::await_async_and_resume(pipeline::context_t* ctx) {
        auto& ids = modified_->ids();
        size_t modified_size = modified_->size();
        execution_context_t exec_ctx{ctx->session, ctx->txn, name_};

        // storage_delete_rows
        vector::vector_t row_ids(resource_, types::logical_type::BIGINT, modified_size);
        for (size_t i = 0; i < modified_size; i++) {
            row_ids.data<int64_t>()[i] = static_cast<int64_t>(ids[i]);
        }
        auto [_d, df] = actor_zeta::send(ctx->disk_address,
            &services::disk::manager_disk_t::storage_delete_rows,
            exec_ctx, std::move(row_ids), static_cast<uint64_t>(modified_size));
        co_await std::move(df);

        delete_txn_id_ = ctx->txn.transaction_id;

        // Mirror to index
        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            if (auto scan_out = left_ ? left_->output() : nullptr) {
                auto& sc = scan_out->data_chunk();
                auto idx_data = std::make_unique<vector::data_chunk_t>(resource_, sc.types(), sc.size());
                sc.copy(*idx_data, 0);
                auto idx_ids = std::pmr::vector<size_t>(resource_);
                idx_ids.reserve(modified_size);
                for (size_t i = 0; i < modified_size; i++) {
                    idx_ids.push_back(i);
                }
                auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                    &services::index::manager_index_t::delete_rows_txn,
                    exec_ctx, std::move(idx_data), std::move(idx_ids));
                co_await std::move(ixf);
            }
        }

        // Build result (need types from storage)
        auto [_t, tf] = actor_zeta::send(ctx->disk_address,
            &services::disk::manager_disk_t::storage_types, ctx->session, name_);
        auto types = co_await std::move(tf);
        vector::data_chunk_t chunk(resource_, types, modified_size);
        chunk.set_cardinality(modified_size);
        output_ = make_operator_data(resource_, std::move(chunk));
        mark_executed();
        co_return;
    }

} // namespace components::operators
