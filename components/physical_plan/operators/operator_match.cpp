#include "operator_match.hpp"

#include "predicates/predicate.hpp"
#include <components/expressions/function_expression.hpp>
#include <components/vector/vector_operations.hpp>

namespace components::operators {

    operator_match_t::operator_match_t(std::pmr::memory_resource* resource,
                                       log_t log,
                                       const expressions::expression_ptr& expression,
                                       logical_plan::limit_t limit)
        : read_only_operator_t(resource, log, operator_type::match)
        , expression_(std::move(expression))
        , limit_(limit) {}

    void operator_match_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (!limit_.check(0)) {
            return; // limit = 0
        }
        if (!left_) {
            return;
        }
        if (left_->output()) {
            const auto& chunk = left_->output()->data_chunk();
            auto types = chunk.types();
            output_ = operators::make_operator_data(left_->output()->resource(), types, chunk.size());
            auto& out_chunk = output_->data_chunk();
            auto predicate = expression_ ? predicates::create_predicate(left_->output()->resource(),
                                                                        pipeline_context->function_registry,
                                                                        expression_,
                                                                        types,
                                                                        types,
                                                                        &pipeline_context->parameters)
                                         : predicates::create_all_true_predicate(left_->output()->resource());

            // Collect matching row indices, then gather all columns at once (avoids per-value boxing/unboxing)
            std::pmr::vector<uint64_t> matched(left_->output()->resource());
            matched.reserve(chunk.size());
            for (size_t i = 0; i < chunk.size(); i++) {
                if (predicate->check(chunk, i)) {
                    matched.push_back(static_cast<uint64_t>(i));
                    if (!limit_.check(static_cast<int>(matched.size()))) {
                        break;
                    }
                }
            }

            auto count = matched.size();
            if (count > 0) {
                vector::indexing_vector_t indexing(left_->output()->resource(), matched.data());
                chunk.copy(out_chunk, indexing, static_cast<uint64_t>(count), 0);
                vector::vector_ops::copy(chunk.row_ids, out_chunk.row_ids, indexing, count, 0, 0);
            }
            out_chunk.set_cardinality(count);
        }
    }

} // namespace components::operators
