#include "operator_join.hpp"
#include "join_utils.hpp"
#include "predicates/predicate.hpp"

#include <algorithm>
#include <components/vector/vector_operations.hpp>

namespace components::operators {

    using join_detail::join_builder;

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

        switch (join_type_) {
            case type::inner:
                inner_join_(predicate, context, res_types, out_chunks);
                break;
            case type::full:
                outer_full_join_(predicate, context, res_types, out_chunks);
                break;
            case type::left:
                outer_left_join_(predicate, context, res_types, out_chunks);
                break;
            case type::right:
                outer_right_join_(predicate, context, res_types, out_chunks);
                break;
            case type::cross:
                cross_join_(context, res_types, out_chunks);
                break;
            default:
                break;
        }

        if (out_chunks.empty()) {
            out_chunks.emplace_back(res_resource, res_types, 0);
        }
        output_ = operators::make_operator_data(res_resource, std::move(out_chunks));

        if (log_.is_valid()) {
            trace(log(), "operator_join::result_size(): {}", output_->size());
        }
    }

    void operator_join_t::inner_join_(const predicates::predicate_ptr& predicate,
                                      pipeline::context_t*,
                                      const std::pmr::vector<types::complex_logical_type>& out_types,
                                      chunks_vector_t& out_chunks) {
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
                    auto results = predicates::batch_check_1vN(predicate, L, R, li, R.size());
                    if (results.has_error()) {
                        set_error(results.error());
                        return;
                    }
                    const auto& mask = results.value();
                    for (uint64_t rj = 0; rj < R.size(); ++rj) {
                        if (mask[rj]) {
                            builder.emit_matched(L, li, R, rj);
                        }
                    }
                }
            }
        }
        builder.flush();
    }

    void operator_join_t::outer_full_join_(const predicates::predicate_ptr& predicate,
                                           pipeline::context_t*,
                                           const std::pmr::vector<types::complex_logical_type>& out_types,
                                           chunks_vector_t& out_chunks) {
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
                    auto results = predicates::batch_check_1vN(predicate, L, R, li, R.size());
                    if (results.has_error()) {
                        set_error(results.error());
                        return;
                    }
                    const auto& mask = results.value();
                    for (uint64_t rj = 0; rj < R.size(); ++rj) {
                        if (mask[rj]) {
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
                                           chunks_vector_t& out_chunks) {
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
                    auto results = predicates::batch_check_1vN(predicate, L, R, li, R.size());
                    if (results.has_error()) {
                        set_error(results.error());
                        return;
                    }
                    const auto& mask = results.value();
                    for (uint64_t rj = 0; rj < R.size(); ++rj) {
                        if (mask[rj]) {
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
                                            chunks_vector_t& out_chunks) {
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
                    auto results = predicates::batch_check_Nv1(predicate, L, R, L.size(), rj);
                    if (results.has_error()) {
                        set_error(results.error());
                        return;
                    }
                    const auto& mask = results.value();
                    for (uint64_t li = 0; li < L.size(); ++li) {
                        if (mask[li]) {
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
                                      chunks_vector_t& out_chunks) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        for (const auto& L : left_chunks) {
            for (uint64_t li = 0; li < L.size(); ++li) {
                for (const auto& R : right_chunks) {
                    for (uint64_t rj = 0; rj < R.size(); ++rj) {
                        builder.emit_matched(L, li, R, rj);
                    }
                }
            }
        }
        builder.flush();
    }

} // namespace components::operators
