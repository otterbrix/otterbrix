#include "operator_hash_join.hpp"
#include "join_utils.hpp"

#include <components/types/logical_value.hpp>

#include <unordered_map>
#include <utility>
#include <vector>

namespace components::operators {

    using join_detail::join_builder;

    namespace {
        struct lv_hash {
            size_t operator()(const types::logical_value_t& v) const noexcept { return v.hash(); }
        };

        // Multimap from a right-side key value → (chunk_idx, row_idx). A multimap
        // (not map) because equi-join keys need not be unique on the right side.
        using right_index_t = std::unordered_multimap<types::logical_value_t,
                                                      std::pair<size_t, uint64_t>,
                                                      lv_hash,
                                                      std::equal_to<types::logical_value_t>>;

        // Build the probe table from `right_chunks[*][right_col]`. NULL right values
        // are skipped — they never join under SQL equi-join semantics.
        right_index_t build_right_hash_index(const chunks_vector_t& right_chunks, size_t right_col) {
            right_index_t table;
            size_t total = 0;
            for (const auto& R : right_chunks) {
                total += R.size();
            }
            table.reserve(total);
            for (size_t ci = 0; ci < right_chunks.size(); ++ci) {
                const auto& R = right_chunks[ci];
                if (right_col >= R.column_count()) {
                    continue;
                }
                const auto& col = R.data[right_col];
                for (uint64_t rj = 0; rj < R.size(); ++rj) {
                    if (!col.validity().row_is_valid(rj)) {
                        continue;
                    }
                    table.emplace(col.value(rj), std::make_pair(ci, rj));
                }
            }
            return table;
        }
    } // namespace

    operator_hash_join_t::operator_hash_join_t(std::pmr::memory_resource* resource,
                                               log_t log,
                                               type join_type,
                                               size_t left_col,
                                               size_t right_col)
        : read_only_operator_t(resource, std::move(log), operator_type::hash_join)
        , join_type_(join_type)
        , left_col_(left_col)
        , right_col_(right_col) {}

    void operator_hash_join_t::on_execute_impl(pipeline::context_t*) {
        if (!left_ || !right_) {
            return;
        }
        if (!left_->output() || !right_->output()) {
            return;
        }

        auto left_out = left_->output();
        auto right_out = right_->output();
        auto& left_chunks = left_out->chunks();
        auto& right_chunks = right_out->chunks();

        // operator_data_t always holds at least one (possibly empty) chunk per side.
        assert(!left_chunks.empty());
        assert(!right_chunks.empty());

        std::pmr::vector<types::complex_logical_type> res_types{left_out->resource()};
        join_detail::compute_join_layout(left_chunks.front(),
                                         right_chunks.front(),
                                         res_types,
                                         indices_left_,
                                         indices_right_);

        if (log_.is_valid()) {
            trace(log(), "operator_hash_join::left_size(): {}", left_out->size());
            trace(log(), "operator_hash_join::right_size(): {}", right_out->size());
        }

        auto* res_resource = left_out->resource();
        chunks_vector_t out_chunks(res_resource);

        switch (join_type_) {
            case type::inner:
                inner_join_hash_(res_types, out_chunks);
                break;
            case type::full:
                outer_full_join_hash_(res_types, out_chunks);
                break;
            case type::left:
                outer_left_join_hash_(res_types, out_chunks);
                break;
            case type::right:
                outer_right_join_hash_(res_types, out_chunks);
                break;
            default:
                // cross / invalid are never substituted with a hash join.
                break;
        }

        if (out_chunks.empty()) {
            out_chunks.emplace_back(res_resource, res_types, 0);
        }
        output_ = operators::make_operator_data(res_resource, std::move(out_chunks));

        if (log_.is_valid()) {
            trace(log(), "operator_hash_join::result_size(): {}", output_->size());
        }
    }

    void operator_hash_join_t::inner_join_hash_(const std::pmr::vector<types::complex_logical_type>& out_types,
                                                chunks_vector_t& out_chunks) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        auto table = build_right_hash_index(right_chunks, right_col_);
        for (const auto& L : left_chunks) {
            if (left_col_ >= L.column_count()) {
                continue;
            }
            const auto& lcol = L.data[left_col_];
            for (uint64_t li = 0; li < L.size(); ++li) {
                if (!lcol.validity().row_is_valid(li)) {
                    continue;
                }
                auto rng = table.equal_range(lcol.value(li));
                for (auto it = rng.first; it != rng.second; ++it) {
                    auto [ci, rj] = it->second;
                    builder.emit_matched(L, li, right_chunks[ci], rj);
                }
            }
        }
        builder.flush();
    }

    void operator_hash_join_t::outer_left_join_hash_(const std::pmr::vector<types::complex_logical_type>& out_types,
                                                     chunks_vector_t& out_chunks) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        auto table = build_right_hash_index(right_chunks, right_col_);
        for (const auto& L : left_chunks) {
            if (left_col_ >= L.column_count()) {
                // Probe column missing — every left row gets NULL on the right side.
                for (uint64_t li = 0; li < L.size(); ++li) {
                    builder.emit_left_only(L, li);
                }
                continue;
            }
            const auto& lcol = L.data[left_col_];
            for (uint64_t li = 0; li < L.size(); ++li) {
                bool matched = false;
                if (lcol.validity().row_is_valid(li)) {
                    auto rng = table.equal_range(lcol.value(li));
                    for (auto it = rng.first; it != rng.second; ++it) {
                        auto [ci, rj] = it->second;
                        builder.emit_matched(L, li, right_chunks[ci], rj);
                        matched = true;
                    }
                }
                if (!matched) {
                    builder.emit_left_only(L, li);
                }
            }
        }
        builder.flush();
    }

    void operator_hash_join_t::outer_right_join_hash_(const std::pmr::vector<types::complex_logical_type>& out_types,
                                                      chunks_vector_t& out_chunks) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        // Track which right rows got matched so unmatched ones can be NULL-padded.
        std::vector<std::vector<bool>> visited(right_chunks.size());
        for (size_t ci = 0; ci < right_chunks.size(); ++ci) {
            visited[ci].assign(right_chunks[ci].size(), false);
        }

        auto table = build_right_hash_index(right_chunks, right_col_);
        for (const auto& L : left_chunks) {
            if (left_col_ >= L.column_count()) {
                continue;
            }
            const auto& lcol = L.data[left_col_];
            for (uint64_t li = 0; li < L.size(); ++li) {
                if (!lcol.validity().row_is_valid(li)) {
                    continue;
                }
                auto rng = table.equal_range(lcol.value(li));
                for (auto it = rng.first; it != rng.second; ++it) {
                    auto [ci, rj] = it->second;
                    builder.emit_matched(L, li, right_chunks[ci], rj);
                    visited[ci][rj] = true;
                }
            }
        }
        // Right rows without any left match (incl. those skipped at build time for NULL keys).
        for (size_t ci = 0; ci < right_chunks.size(); ++ci) {
            const auto& R = right_chunks[ci];
            for (uint64_t rj = 0; rj < R.size(); ++rj) {
                if (!visited[ci][rj]) {
                    builder.emit_right_only(R, rj);
                }
            }
        }
        builder.flush();
    }

    void operator_hash_join_t::outer_full_join_hash_(const std::pmr::vector<types::complex_logical_type>& out_types,
                                                     chunks_vector_t& out_chunks) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        std::vector<std::vector<bool>> visited(right_chunks.size());
        for (size_t ci = 0; ci < right_chunks.size(); ++ci) {
            visited[ci].assign(right_chunks[ci].size(), false);
        }

        auto table = build_right_hash_index(right_chunks, right_col_);
        for (const auto& L : left_chunks) {
            if (left_col_ >= L.column_count()) {
                for (uint64_t li = 0; li < L.size(); ++li) {
                    builder.emit_left_only(L, li);
                }
                continue;
            }
            const auto& lcol = L.data[left_col_];
            for (uint64_t li = 0; li < L.size(); ++li) {
                bool matched = false;
                if (lcol.validity().row_is_valid(li)) {
                    auto rng = table.equal_range(lcol.value(li));
                    for (auto it = rng.first; it != rng.second; ++it) {
                        auto [ci, rj] = it->second;
                        builder.emit_matched(L, li, right_chunks[ci], rj);
                        visited[ci][rj] = true;
                        matched = true;
                    }
                }
                if (!matched) {
                    builder.emit_left_only(L, li);
                }
            }
        }
        for (size_t ci = 0; ci < right_chunks.size(); ++ci) {
            const auto& R = right_chunks[ci];
            for (uint64_t rj = 0; rj < R.size(); ++rj) {
                if (!visited[ci][rj]) {
                    builder.emit_right_only(R, rj);
                }
            }
        }
        builder.flush();
    }

} // namespace components::operators
