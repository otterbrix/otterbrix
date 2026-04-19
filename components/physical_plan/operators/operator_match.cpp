#include "operator_match.hpp"

#include "predicates/predicate.hpp"
#include <components/expressions/function_expression.hpp>

namespace components::operators {

    operator_match_t::operator_match_t(std::pmr::memory_resource* resource,
                                       log_t log,
                                       const expressions::expression_ptr& expression,
                                       logical_plan::limit_t limit)
        : read_only_operator_t(resource, log, operator_type::match)
        , expression_(std::move(expression))
        , limit_(limit) {}

    void operator_match_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        int64_t total = 0; // total matching rows seen (including skipped)
        if (!limit_.check(total)) {
            return; // limit = 0 with no offset
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
                        for (size_t j = 0; j < chunk.column_count(); j++) {
                            out_chunk.set_value(j, static_cast<uint64_t>(out_count), chunk.data[j].value(i));
                        }
                        out_chunk.row_ids.data<int64_t>()[out_count] = chunk.row_ids.data<int64_t>()[i];
                        ++out_count;
                    }
                    ++total;
                    if (!limit_.check(total)) {
                        out_chunk.set_cardinality(static_cast<uint64_t>(out_count));
                        return;
                    }
                }
            }
            out_chunk.set_cardinality(static_cast<uint64_t>(out_count));
        }
    }

} // namespace components::operators
