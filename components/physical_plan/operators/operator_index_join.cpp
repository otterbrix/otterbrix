#include "operator_index_join.hpp"
#include "join/join_builder.hpp"

#include <algorithm>
#include <optional>
#include <unordered_map>
#include <services/index/manager_index.hpp>

namespace components::operators {

    namespace {
        std::optional<std::size_t>
        find_key_in_types(const std::pmr::vector<types::complex_logical_type>& types, const expressions::key_t& key) {
            const auto key_name = key.as_string();
            for (std::size_t i = 0; i < types.size(); ++i) {
                if (types[i].alias() == key_name) {
                    return i;
                }
            }
            return std::nullopt;
        }

        struct equi_join_key_info_t {
            std::size_t left_idx;
            std::size_t right_idx;
            expressions::key_t left_key;
            expressions::key_t right_key;
        };

        struct row_ref_t {
            std::size_t chunk_idx;
            uint64_t row_idx;
        };

        std::optional<equi_join_key_info_t>
        try_extract_inner_equi_keys(const expressions::expression_ptr& expression,
                                    const std::pmr::vector<types::complex_logical_type>& left_types,
                                    const std::pmr::vector<types::complex_logical_type>& right_types,
                                    std::pmr::memory_resource* resource) {
            if (!expression || expression->group() != expressions::expression_group::compare) {
                return std::nullopt;
            }
            auto comp = static_cast<const expressions::compare_expression_t*>(expression.get());
            if (comp->type() != expressions::compare_type::eq || !comp->children().empty()) {
                return std::nullopt;
            }
            if (!std::holds_alternative<expressions::key_t>(comp->left()) ||
                !std::holds_alternative<expressions::key_t>(comp->right())) {
                return std::nullopt;
            }

            const auto& lhs_key = std::get<expressions::key_t>(comp->left());
            const auto& rhs_key = std::get<expressions::key_t>(comp->right());
            auto lhs_left = find_key_in_types(left_types, lhs_key);
            auto lhs_right = find_key_in_types(right_types, lhs_key);
            auto rhs_left = find_key_in_types(left_types, rhs_key);
            auto rhs_right = find_key_in_types(right_types, rhs_key);

            if (lhs_left.has_value() && rhs_right.has_value()) {
                return equi_join_key_info_t{*lhs_left,
                                            *rhs_right,
                                            expressions::key_t(resource, lhs_key.as_string()),
                                            expressions::key_t(resource, rhs_key.as_string())};
            }
            if (rhs_left.has_value() && lhs_right.has_value()) {
                return equi_join_key_info_t{*rhs_left,
                                            *lhs_right,
                                            expressions::key_t(resource, rhs_key.as_string()),
                                            expressions::key_t(resource, lhs_key.as_string())};
            }
            return std::nullopt;
        }
    } // namespace

    operator_index_join_t::operator_index_join_t(std::pmr::memory_resource* resource,
                                                 log_t log,
                                                 type join_type,
                                                 const expressions::expression_ptr& expression,
                                                 components::catalog::oid_t probe_table_oid,
                                                 probe_side_t probe_side)
        : read_only_operator_t(resource, std::move(log), operator_type::join)
        , join_type_(join_type)
        , expression_(expression)
        , probe_table_oid_(probe_table_oid)
        , probe_side_(probe_side) {}

    void operator_index_join_t::on_execute_impl(pipeline::context_t* context) {
        if (!left_ || !right_) {
            return;
        }
        if (!left_->output() || !right_->output()) {
            return;
        }
        if (join_type_ != type::inner || probe_table_oid_ == components::catalog::INVALID_OID ||
            context->index_address == actor_zeta::address_t::empty_address()) {
            return;
        }
        async_wait();
    }

    actor_zeta::unique_future<void> operator_index_join_t::await_async_and_resume(pipeline::context_t* ctx) {
        auto left_out = left_->output();
        auto right_out = right_->output();
        auto& left_chunks = left_out->chunks();
        auto& right_chunks = right_out->chunks();

        if (left_chunks.empty() || right_chunks.empty()) {
            output_ = make_operator_data(left_out->resource(), std::pmr::vector<types::complex_logical_type>{resource_});
            mark_executed();
            co_return;
        }

        auto res_types = left_chunks.front().types();
        auto right_types = right_chunks.front().types();
        size_t left_col_count = left_chunks.front().column_count();
        size_t right_col_count = right_chunks.front().column_count();

        indices_left_.clear();
        indices_right_.clear();
        indices_left_.reserve(left_col_count);
        indices_right_.reserve(right_col_count);
        for (size_t i = 0; i < left_col_count; ++i) {
            indices_left_.emplace_back(i);
        }
        for (size_t i = 0; i < right_col_count; ++i) {
            const auto& alias = right_types[i].alias();
            auto dup =
                std::find_if(res_types.begin(), res_types.end(), [&](const auto& t) { return t.alias() == alias; });
            if (dup != res_types.end()) {
                indices_right_.emplace_back(static_cast<size_t>(std::distance(res_types.begin(), dup)));
            } else {
                indices_right_.emplace_back(res_types.size());
                res_types.push_back(right_types[i]);
            }
        }

        auto key_info =
            try_extract_inner_equi_keys(expression_, left_chunks.front().types(), right_chunks.front().types(), resource_);
        if (!key_info.has_value()) {
            set_error(core::error_t(core::error_code_t::create_physical_plan_error,
                                    std::pmr::string{"operator_index_join requires simple inner equi-join",
                                                     resource_}));
            co_return;
        }

        chunks_vector_t out_chunks(left_out->resource());
        join::join_builder_t builder(left_out->resource(), res_types, indices_left_, indices_right_, out_chunks);
        // Build row_id -> row_ref map for the table selected as probe side.
        std::pmr::unordered_map<int64_t, std::pmr::vector<row_ref_t>> probe_rows_by_id(resource_);
        if (probe_side_ == probe_side_t::right) {
            for (std::size_t rci = 0; rci < right_chunks.size(); ++rci) {
                const auto& R = right_chunks[rci];
                for (uint64_t rj = 0; rj < R.size(); ++rj) {
                    const auto row_id = R.row_ids.data<int64_t>()[rj];
                    auto it = probe_rows_by_id.find(row_id);
                    if (it == probe_rows_by_id.end()) {
                        std::pmr::vector<row_ref_t> refs(resource_);
                        refs.emplace_back(row_ref_t{rci, rj});
                        probe_rows_by_id.emplace(row_id, std::move(refs));
                    } else {
                        it->second.emplace_back(row_ref_t{rci, rj});
                    }
                }
            }
        } else {
            for (std::size_t lci = 0; lci < left_chunks.size(); ++lci) {
                const auto& L = left_chunks[lci];
                for (uint64_t li = 0; li < L.size(); ++li) {
                    const auto row_id = L.row_ids.data<int64_t>()[li];
                    auto it = probe_rows_by_id.find(row_id);
                    if (it == probe_rows_by_id.end()) {
                        std::pmr::vector<row_ref_t> refs(resource_);
                        refs.emplace_back(row_ref_t{lci, li});
                        probe_rows_by_id.emplace(row_id, std::move(refs));
                    } else {
                        it->second.emplace_back(row_ref_t{lci, li});
                    }
                }
            }
        }

        if (probe_side_ == probe_side_t::right) {
            for (const auto& L : left_chunks) {
                for (uint64_t li = 0; li < L.size(); ++li) {
                    auto left_key_val = L.data[key_info->left_idx].value(li);
                    if (left_key_val.is_null()) {
                        continue;
                    }

                    components::index::keys_base_storage_t search_keys(resource_);
                    search_keys.emplace_back(key_info->right_key);
                    auto [_hs, hsf] = actor_zeta::send(ctx->index_address,
                                                       &services::index::manager_index_t::search_by_type,
                                                       ctx->session,
                                                       probe_table_oid_,
                                                       components::index::keys_base_storage_t{search_keys},
                                                       types::logical_value_t{resource_, left_key_val},
                                                       expressions::compare_type::eq,
                                                       ctx->txn.start_time,
                                                       ctx->txn.transaction_id,
                                                       components::logical_plan::index_type::hashed);
                    auto ids = co_await std::move(hsf);
                    if (ids.empty()) {
                        auto [_s, sf] = actor_zeta::send(ctx->index_address,
                                                         &services::index::manager_index_t::search,
                                                         ctx->session,
                                                         probe_table_oid_,
                                                         std::move(search_keys),
                                                         types::logical_value_t{resource_, left_key_val},
                                                         expressions::compare_type::eq,
                                                         ctx->txn.start_time,
                                                         ctx->txn.transaction_id);
                        ids = co_await std::move(sf);
                    }
                    if (ids.empty()) {
                        continue;
                    }
                    for (auto id : ids) {
                        auto it = probe_rows_by_id.find(id);
                        if (it == probe_rows_by_id.end()) {
                            continue;
                        }
                        for (const auto& ref : it->second) {
                            const auto& R = right_chunks[ref.chunk_idx];
                            builder.emit_matched(L, li, R, ref.row_idx);
                        }
                    }
                }
            }
        } else {
            for (const auto& R : right_chunks) {
                for (uint64_t rj = 0; rj < R.size(); ++rj) {
                    auto right_key_val = R.data[key_info->right_idx].value(rj);
                    if (right_key_val.is_null()) {
                        continue;
                    }

                    components::index::keys_base_storage_t search_keys(resource_);
                    search_keys.emplace_back(key_info->left_key);
                    auto [_hs, hsf] = actor_zeta::send(ctx->index_address,
                                                       &services::index::manager_index_t::search_by_type,
                                                       ctx->session,
                                                       probe_table_oid_,
                                                       components::index::keys_base_storage_t{search_keys},
                                                       types::logical_value_t{resource_, right_key_val},
                                                       expressions::compare_type::eq,
                                                       ctx->txn.start_time,
                                                       ctx->txn.transaction_id,
                                                       components::logical_plan::index_type::hashed);
                    auto ids = co_await std::move(hsf);
                    if (ids.empty()) {
                        auto [_s, sf] = actor_zeta::send(ctx->index_address,
                                                         &services::index::manager_index_t::search,
                                                         ctx->session,
                                                         probe_table_oid_,
                                                         std::move(search_keys),
                                                         types::logical_value_t{resource_, right_key_val},
                                                         expressions::compare_type::eq,
                                                         ctx->txn.start_time,
                                                         ctx->txn.transaction_id);
                        ids = co_await std::move(sf);
                    }
                    if (ids.empty()) {
                        continue;
                    }
                    for (auto id : ids) {
                        auto it = probe_rows_by_id.find(id);
                        if (it == probe_rows_by_id.end()) {
                            continue;
                        }
                        for (const auto& ref : it->second) {
                            const auto& L = left_chunks[ref.chunk_idx];
                            builder.emit_matched(L, ref.row_idx, R, rj);
                        }
                    }
                }
            }
        }

        builder.flush();
        if (out_chunks.empty()) {
            out_chunks.emplace_back(left_out->resource(), res_types, 0);
        }
        output_ = make_operator_data(left_out->resource(), std::move(out_chunks));
        mark_executed();
        co_return;
    }

} // namespace components::operators
