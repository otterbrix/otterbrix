#include "operator_index_join.hpp"
#include "join/join_builder.hpp"

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
            auto rhs_right = find_key_in_types(right_types, rhs_key);

            if (lhs_left.has_value() && rhs_right.has_value()) {
                return equi_join_key_info_t{*lhs_left,
                                            *rhs_right,
                                            expressions::key_t(resource, lhs_key.as_string()),
                                            expressions::key_t(resource, rhs_key.as_string())};
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

    void operator_index_join_t::add_row_ref(std::pmr::unordered_map<int64_t, std::pmr::vector<row_ref_t>>& refs_by_id,
                                            int64_t row_id,
                                            row_ref_t ref,
                                            std::pmr::memory_resource* resource) {
        auto it = refs_by_id.find(row_id);
        if (it == refs_by_id.end()) {
            std::pmr::vector<row_ref_t> refs(resource);
            refs.emplace_back(ref);
            refs_by_id.emplace(row_id, std::move(refs));
        } else {
            it->second.emplace_back(ref);
        }
    }

    actor_zeta::unique_future<std::pmr::vector<int64_t>>
    operator_index_join_t::search_ids(pipeline::context_t* ctx,
                                      const expressions::key_t& probe_key,
                                      const types::logical_value_t& value) {
        components::index::keys_base_storage_t search_keys(resource_);
        search_keys.emplace_back(probe_key);
        auto [_s, sf] = actor_zeta::send(ctx->index_address,
                                         &services::index::manager_index_t::search_with_preferred_type,
                                         ctx->session,
                                         probe_table_oid_,
                                         std::move(search_keys),
                                         types::logical_value_t{resource_, value},
                                         expressions::compare_type::eq,
                                         ctx->txn.start_time,
                                         ctx->txn.transaction_id,
                                         components::logical_plan::index_type::hashed);
        auto ids = co_await std::move(sf);
        co_return ids;
    }

    void operator_index_join_t::emit_match(join::join_builder_t& builder,
                                           const chunks_vector_t& left_chunks,
                                           const chunks_vector_t& right_chunks,
                                           bool probe_right,
                                           const vector::data_chunk_t& source_chunk,
                                           uint64_t source_row,
                                           const row_ref_t& ref) {
        if (probe_right) {
            const auto& R = right_chunks[ref.chunk_idx];
            builder.emit_matched(source_chunk, source_row, R, ref.row_idx);
        } else {
            const auto& L = left_chunks[ref.chunk_idx];
            builder.emit_matched(L, ref.row_idx, source_chunk, source_row);
        }
    }

    std::optional<std::size_t>
    operator_index_join_t::find_type_alias_index(const std::pmr::vector<types::complex_logical_type>& types,
                                                 const std::string& alias) {
        for (std::size_t i = 0; i < types.size(); ++i) {
            if (types[i].alias() == alias) {
                return i;
            }
        }
        return std::nullopt;
    }

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
            auto dup_idx = find_type_alias_index(res_types, alias);
            if (dup_idx.has_value()) {
                indices_right_.emplace_back(*dup_idx);
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
        const bool probe_right = probe_side_ == probe_side_t::right;
        const auto& probe_chunks = probe_right ? right_chunks : left_chunks;
        for (std::size_t ci = 0; ci < probe_chunks.size(); ++ci) {
            const auto& chunk = probe_chunks[ci];
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                add_row_ref(probe_rows_by_id, chunk.row_ids.data<int64_t>()[row], row_ref_t{ci, row}, resource_);
            }
        }

        const auto& source_chunks = probe_right ? left_chunks : right_chunks;
        const auto source_key_idx = probe_right ? key_info->left_idx : key_info->right_idx;
        const auto& probe_key = probe_right ? key_info->right_key : key_info->left_key;

        for (const auto& source_chunk : source_chunks) {
            for (uint64_t source_row = 0; source_row < source_chunk.size(); ++source_row) {
                auto key_val = source_chunk.data[source_key_idx].value(source_row);
                if (key_val.is_null()) {
                    continue;
                }
                auto ids = co_await search_ids(ctx, probe_key, key_val);
                for (auto id : ids) {
                    auto it = probe_rows_by_id.find(id);
                    if (it == probe_rows_by_id.end()) {
                        continue;
                    }
                    for (const auto& ref : it->second) {
                        emit_match(builder, left_chunks, right_chunks, probe_right, source_chunk, source_row, ref);
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
