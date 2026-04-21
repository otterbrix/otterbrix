#include "operator_match.hpp"

#include "predicates/predicate.hpp"
#include <components/expressions/function_expression.hpp>

namespace components::operators {

    namespace {
        // Placeholder columns (produced by projected scans) have no buffer and no auxiliary.
        // They must be skipped when reading values — vector_t::value() would crash otherwise.
        bool is_placeholder(const vector::vector_t& v) noexcept {
            return v.data() == nullptr && v.auxiliary() == nullptr;
        }
    } // namespace

    operator_match_t::operator_match_t(std::pmr::memory_resource* resource,
                                       log_t log,
                                       const expressions::expression_ptr& expression,
                                       logical_plan::limit_t limit)
        : read_only_operator_t(resource, log, operator_type::match)
        , expression_(std::move(expression))
        , limit_(limit) {}

    void operator_match_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        int64_t total = 0;
        if (!limit_.check(total)) {
            return;
        }
        if (!left_ || !left_->output()) {
            return;
        }

        auto* resource = left_->output()->resource();
        const auto& in_chunks = left_->output()->chunks();
        std::pmr::vector<types::complex_logical_type> types{resource};
        if (!in_chunks.empty()) {
            types = in_chunks.front().types();
        }

        // Build populated_cols from the first chunk: only slots with data flow downstream.
        // Schema is identical across chunks, so this is computed once.
        std::vector<size_t> populated_cols;
        bool sparse = false;
        if (!in_chunks.empty()) {
            populated_cols.reserve(in_chunks.front().column_count());
            for (size_t j = 0; j < in_chunks.front().column_count(); j++) {
                if (!is_placeholder(in_chunks.front().data[j])) {
                    populated_cols.push_back(j);
                }
            }
            sparse = populated_cols.size() != in_chunks.front().column_count();
        }

        std::vector<vector::data_chunk_t> out_chunks;

        auto predicate = expression_ ? predicates::create_predicate(resource,
                                                                    pipeline_context->function_registry,
                                                                    expression_,
                                                                    types,
                                                                    types,
                                                                    &pipeline_context->parameters)
                                     : predicates::create_all_true_predicate(resource);

        bool reached_limit = false;
        for (const auto& chunk : in_chunks) {
            if (reached_limit || chunk.size() == 0) {
                continue;
            }
            vector::data_chunk_t out_chunk = sparse
                ? vector::data_chunk_t(resource, types, populated_cols, chunk.size())
                : vector::data_chunk_t(resource, types, chunk.size());
            vector::indexing_vector_t all_indices(nullptr, nullptr);
            auto results = predicate->batch_check(chunk, chunk, all_indices, all_indices, chunk.size());
            if (results.has_error()) {
                set_error(results.error());
                return;
            }
            int64_t out_count = 0;
            for (size_t i = 0; i < chunk.size(); i++) {
                if (results.value()[i]) {
                    if (!limit_.is_skipping(total)) {
                        for (size_t j : populated_cols) {
                            out_chunk.set_value(j, static_cast<uint64_t>(out_count), chunk.data[j].value(i));
                        }
                        out_chunk.row_ids.data<int64_t>()[out_count] = chunk.row_ids.data<int64_t>()[i];
                        ++out_count;
                    }
                    ++total;
                    if (!limit_.check(total)) {
                        reached_limit = true;
                        break;
                    }
                }
            }
            out_chunk.set_cardinality(static_cast<uint64_t>(out_count));
            if (out_count > 0) {
                out_chunks.emplace_back(std::move(out_chunk));
            }
        }

        if (out_chunks.empty()) {
            output_ = operators::make_operator_data(resource, types, 0);
        } else {
            output_ = operators::make_operator_data(resource, std::move(out_chunks));
        }
    }

} // namespace components::operators
