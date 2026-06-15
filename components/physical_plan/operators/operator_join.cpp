#include "operator_join.hpp"
#include "join_utils.hpp"
#include "predicates/predicate.hpp"

#include <algorithm>
#include <components/vector/vector_operations.hpp>
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <unordered_map>

namespace components::operators {

    using join_detail::join_builder;

    namespace {
        bool trace_join_enabled() {
            const char* raw = std::getenv("OTTERBRIX_EXEC_TRACE_NODES");
            return raw && raw[0] != '\0' && raw[0] != '0';
        }

        const char* join_type_name(operator_join_t::type type) {
            switch (type) {
                case operator_join_t::type::inner:
                    return "inner";
                case operator_join_t::type::full:
                    return "full";
                case operator_join_t::type::left:
                    return "left";
                case operator_join_t::type::right:
                    return "right";
                case operator_join_t::type::cross:
                    return "cross";
                default:
                    return "invalid";
            }
        }

        struct row_ref_t {
            const vector::data_chunk_t* chunk = nullptr;
            uint64_t row = 0;
        };

        struct hash_join_key_t {
            std::pmr::vector<size_t> left_path;
            std::pmr::vector<size_t> right_path;
        };

        struct logical_value_hash {
            size_t operator()(const types::logical_value_t& value) const noexcept { return value.hash(); }
        };

        struct logical_value_equal {
            bool operator()(const types::logical_value_t& left, const types::logical_value_t& right) const {
                if (left.type() != right.type()) {
                    return false;
                }
                return left == right;
            }
        };

        std::optional<hash_join_key_t> extract_hash_join_key(const expressions::compare_expression_t& compare);

        std::optional<hash_join_key_t> extract_hash_join_key(const expressions::expression_ptr& expression) {
            if (!expression || expression->group() != expressions::expression_group::compare) {
                return std::nullopt;
            }
            return extract_hash_join_key(*static_cast<const expressions::compare_expression_t*>(expression.get()));
        }

        std::optional<hash_join_key_t> extract_hash_join_key(const expressions::compare_expression_t& compare) {
            if (compare.type() == expressions::compare_type::union_and) {
                for (const auto& child : compare.children()) {
                    auto key = extract_hash_join_key(child);
                    if (key) {
                        return key;
                    }
                }
                return std::nullopt;
            }

            if (compare.type() != expressions::compare_type::eq) {
                return std::nullopt;
            }
            if (!std::holds_alternative<expressions::key_t>(compare.left()) ||
                !std::holds_alternative<expressions::key_t>(compare.right())) {
                return std::nullopt;
            }

            const auto& lhs = std::get<expressions::key_t>(compare.left());
            const auto& rhs = std::get<expressions::key_t>(compare.right());
            if (lhs.path().empty() || rhs.path().empty()) {
                return std::nullopt;
            }

            if (lhs.side() == expressions::side_t::left && rhs.side() == expressions::side_t::right) {
                return hash_join_key_t{lhs.path(), rhs.path()};
            }
            if (lhs.side() == expressions::side_t::right && rhs.side() == expressions::side_t::left) {
                return hash_join_key_t{rhs.path(), lhs.path()};
            }
            return std::nullopt;
        }

        bool key_column_available(const vector::data_chunk_t& chunk, const std::pmr::vector<size_t>& path) {
            if (path.empty() || path.front() >= chunk.column_count()) {
                return false;
            }
            return !join_detail::is_placeholder(chunk.data[path.front()]);
        }

        bool hash_key_columns_available(const chunks_vector_t& chunks,
                                        const std::pmr::vector<size_t>& path) {
            for (const auto& chunk : chunks) {
                if (chunk.size() == 0) {
                    continue;
                }
                if (!key_column_available(chunk, path)) {
                    return false;
                }
            }
            return true;
        }

        std::optional<types::logical_value_t> read_hash_key(const vector::data_chunk_t& chunk,
                                                            const std::pmr::vector<size_t>& path,
                                                            uint64_t row) {
            auto value = chunk.value(path, row);
            if (value.is_null()) {
                return std::nullopt;
            }
            return value;
        }
    } // namespace

    operator_join_t::operator_join_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     type join_type,
                                     const expressions::expression_ptr& expression)
        : read_only_operator_t(resource, log, operator_type::join)
        , join_type_(join_type)
        , expression_(expression) {}

    void operator_join_t::on_execute_impl(pipeline::context_t* context) {
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
            trace(log(), "operator_join::left_size(): {}", left_out->size());
            trace(log(), "operator_join::right_size(): {}", right_out->size());
        }

        auto predicate = expression_ ? predicates::create_predicate(left_out->resource(),
                                                                    context->function_registry,
                                                                    expression_,
                                                                    left_chunks.front().types(),
                                                                    right_chunks.front().types(),
                                                                    &context->parameters,
                                                                    context->session_tz)
                                     : predicates::create_all_true_predicate(left_out->resource());

        auto* res_resource = left_out->resource();
        chunks_vector_t out_chunks(res_resource);

        const auto trace_enabled = trace_join_enabled();
        stats_t stats;
        auto* trace_stats = trace_enabled ? &stats : nullptr;
        switch (join_type_) {
            case type::inner:
                inner_join_(predicate, context, res_types, out_chunks, trace_stats);
                break;
            case type::full:
                outer_full_join_(predicate, context, res_types, out_chunks, trace_stats);
                break;
            case type::left:
                outer_left_join_(predicate, context, res_types, out_chunks, trace_stats);
                break;
            case type::right:
                outer_right_join_(predicate, context, res_types, out_chunks, trace_stats);
                break;
            case type::cross:
                cross_join_(context, res_types, out_chunks, trace_stats);
                break;
            default:
                break;
        }

        if (out_chunks.empty()) {
            out_chunks.emplace_back(res_resource, res_types, 0);
        }
        output_ = operators::make_operator_data(res_resource, std::move(out_chunks));
        if (trace_enabled) {
            std::fprintf(stderr,
                         "OTBX_JOIN type=%s rows_left=%zu rows_right=%zu rows_out=%zu "
                         "algorithm=%s build_rows=%llu hash_table_size=%llu "
                         "probe_batches=%llu probe_rows=%llu matches=%llu\n",
                         join_type_name(join_type_),
                         left_out->size(),
                         right_out->size(),
                         output_->size(),
                         stats.hash_join ? "hash" : "nested",
                         static_cast<unsigned long long>(stats.build_rows),
                         static_cast<unsigned long long>(stats.hash_table_size),
                         static_cast<unsigned long long>(stats.probe_batches),
                         static_cast<unsigned long long>(stats.probe_rows),
                         static_cast<unsigned long long>(stats.matches));
        }

        if (log_.is_valid()) {
            trace(log(), "operator_join::result_size(): {}", output_->size());
        }
    }

    void operator_join_t::inner_join_(const predicates::predicate_ptr& predicate,
                                      pipeline::context_t*,
                                      const std::pmr::vector<types::complex_logical_type>& out_types,
                                      chunks_vector_t& out_chunks,
                                      stats_t* stats) {
        if (inner_hash_join_(predicate, out_types, out_chunks, stats)) {
            return;
        }

        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        for (const auto& L : left_chunks) {
            for (uint64_t li = 0; li < L.size(); ++li) {
                for (const auto& R : right_chunks) {
                    if (R.size() == 0) {
                        continue;
                    }
                    if (stats) {
                        ++stats->probe_batches;
                        stats->probe_rows += R.size();
                    }
                    auto results = predicates::batch_check_1vN(predicate, L, R, li, R.size());
                    if (results.has_error()) {
                        set_error(results.error());
                        return;
                    }
                    const auto& mask = results.value();
                    for (uint64_t rj = 0; rj < R.size(); ++rj) {
                        if (mask[rj]) {
                            if (stats) {
                                ++stats->matches;
                            }
                            builder.emit_matched(L, li, R, rj);
                        }
                    }
                }
            }
        }
        builder.flush();
    }

    bool operator_join_t::inner_hash_join_(const predicates::predicate_ptr& predicate,
                                           const std::pmr::vector<types::complex_logical_type>& out_types,
                                           chunks_vector_t& out_chunks,
                                           stats_t* stats) {
        auto key = extract_hash_join_key(expression_);
        if (!key) {
            return false;
        }

        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        if (!hash_key_columns_available(left_chunks, key->left_path) ||
            !hash_key_columns_available(right_chunks, key->right_path)) {
            return false;
        }

        auto* resource = left_->output()->resource();
        using hash_table_t =
            std::unordered_map<types::logical_value_t, std::vector<row_ref_t>, logical_value_hash, logical_value_equal>;
        hash_table_t hash_table;

        for (const auto& R : right_chunks) {
            for (uint64_t rj = 0; rj < R.size(); ++rj) {
                auto build_key = read_hash_key(R, key->right_path, rj);
                if (!build_key) {
                    continue;
                }
                hash_table[std::move(*build_key)].push_back(row_ref_t{&R, rj});
                if (stats) {
                    ++stats->build_rows;
                }
            }
        }

        if (stats) {
            stats->hash_join = true;
            stats->hash_table_size = hash_table.size();
        }

        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);
        for (const auto& L : left_chunks) {
            if (stats && L.size() > 0) {
                ++stats->probe_batches;
            }
            for (uint64_t li = 0; li < L.size(); ++li) {
                if (stats) {
                    ++stats->probe_rows;
                }
                auto probe_key = read_hash_key(L, key->left_path, li);
                if (!probe_key) {
                    continue;
                }
                auto range = hash_table.find(*probe_key);
                if (range == hash_table.end()) {
                    continue;
                }
                for (const auto& right_ref : range->second) {
                    auto passed = predicate->check(L, *right_ref.chunk, li, right_ref.row);
                    if (passed.has_error()) {
                        set_error(passed.error());
                        return true;
                    }
                    if (passed.value()) {
                        if (stats) {
                            ++stats->matches;
                        }
                        builder.emit_matched(L, li, *right_ref.chunk, right_ref.row);
                    }
                }
            }
        }
        builder.flush();
        return true;
    }

    void operator_join_t::outer_full_join_(const predicates::predicate_ptr& predicate,
                                           pipeline::context_t*,
                                           const std::pmr::vector<types::complex_logical_type>& out_types,
                                           chunks_vector_t& out_chunks,
                                           stats_t* stats) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        // visited_right[ci_r][rj] — tracks which right rows got matched during the probe.
        std::vector<std::vector<bool>> visited_right(right_chunks.size());
        for (size_t ci_r = 0; ci_r < right_chunks.size(); ++ci_r) {
            visited_right[ci_r].assign(right_chunks[ci_r].size(), false);
        }

        for (const auto& L : left_chunks) {
            for (uint64_t li = 0; li < L.size(); ++li) {
                bool any_match = false;
                for (size_t ci_r = 0; ci_r < right_chunks.size(); ++ci_r) {
                    const auto& R = right_chunks[ci_r];
                    if (R.size() == 0) {
                        continue;
                    }
                    if (stats) {
                        ++stats->probe_batches;
                        stats->probe_rows += R.size();
                    }
                    auto results = predicates::batch_check_1vN(predicate, L, R, li, R.size());
                    if (results.has_error()) {
                        set_error(results.error());
                        return;
                    }
                    const auto& mask = results.value();
                    for (uint64_t rj = 0; rj < R.size(); ++rj) {
                        if (mask[rj]) {
                            if (stats) {
                                ++stats->matches;
                            }
                            any_match = true;
                            visited_right[ci_r][rj] = true;
                            builder.emit_matched(L, li, R, rj);
                        }
                    }
                }
                if (!any_match) {
                    builder.emit_left_only(L, li);
                }
            }
        }

        // Emit all right rows not visited by any left row — NULL-padded on the left side.
        for (size_t ci_r = 0; ci_r < right_chunks.size(); ++ci_r) {
            const auto& R = right_chunks[ci_r];
            for (uint64_t rj = 0; rj < R.size(); ++rj) {
                if (!visited_right[ci_r][rj]) {
                    builder.emit_right_only(R, rj);
                }
            }
        }
        builder.flush();
    }

    void operator_join_t::outer_left_join_(const predicates::predicate_ptr& predicate,
                                           pipeline::context_t*,
                                           const std::pmr::vector<types::complex_logical_type>& out_types,
                                           chunks_vector_t& out_chunks,
                                           stats_t* stats) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        for (const auto& L : left_chunks) {
            for (uint64_t li = 0; li < L.size(); ++li) {
                bool any_match = false;
                for (const auto& R : right_chunks) {
                    if (R.size() == 0) {
                        continue;
                    }
                    if (stats) {
                        ++stats->probe_batches;
                        stats->probe_rows += R.size();
                    }
                    auto results = predicates::batch_check_1vN(predicate, L, R, li, R.size());
                    if (results.has_error()) {
                        set_error(results.error());
                        return;
                    }
                    const auto& mask = results.value();
                    for (uint64_t rj = 0; rj < R.size(); ++rj) {
                        if (mask[rj]) {
                            if (stats) {
                                ++stats->matches;
                            }
                            any_match = true;
                            builder.emit_matched(L, li, R, rj);
                        }
                    }
                }
                if (!any_match) {
                    builder.emit_left_only(L, li);
                }
            }
        }
        builder.flush();
    }

    void operator_join_t::outer_right_join_(const predicates::predicate_ptr& predicate,
                                            pipeline::context_t*,
                                            const std::pmr::vector<types::complex_logical_type>& out_types,
                                            chunks_vector_t& out_chunks,
                                            stats_t* stats) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        for (const auto& R : right_chunks) {
            for (uint64_t rj = 0; rj < R.size(); ++rj) {
                bool any_match = false;
                for (const auto& L : left_chunks) {
                    if (L.size() == 0) {
                        continue;
                    }
                    if (stats) {
                        ++stats->probe_batches;
                        stats->probe_rows += L.size();
                    }
                    auto results = predicates::batch_check_Nv1(predicate, L, R, L.size(), rj);
                    if (results.has_error()) {
                        set_error(results.error());
                        return;
                    }
                    const auto& mask = results.value();
                    for (uint64_t li = 0; li < L.size(); ++li) {
                        if (mask[li]) {
                            if (stats) {
                                ++stats->matches;
                            }
                            any_match = true;
                            builder.emit_matched(L, li, R, rj);
                        }
                    }
                }
                if (!any_match) {
                    builder.emit_right_only(R, rj);
                }
            }
        }
        builder.flush();
    }

    void operator_join_t::cross_join_(pipeline::context_t*,
                                      const std::pmr::vector<types::complex_logical_type>& out_types,
                                      chunks_vector_t& out_chunks,
                                      stats_t* stats) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        for (const auto& L : left_chunks) {
            for (uint64_t li = 0; li < L.size(); ++li) {
                for (const auto& R : right_chunks) {
                    if (stats) {
                        ++stats->probe_batches;
                        stats->probe_rows += R.size();
                    }
                    for (uint64_t rj = 0; rj < R.size(); ++rj) {
                        if (stats) {
                            ++stats->matches;
                        }
                        builder.emit_matched(L, li, R, rj);
                    }
                }
            }
        }
        builder.flush();
    }

} // namespace components::operators
