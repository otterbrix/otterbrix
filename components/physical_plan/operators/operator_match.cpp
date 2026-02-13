#include "operator_match.hpp"

#include "expressions/function_expression.hpp"
#include "predicates/predicate.hpp"
#include "services/collection/collection.hpp"

namespace components::operators {

    operator_match_t::operator_match_t(services::collection::context_collection_t* context,
                                       const expressions::expression_ptr& expression,
                                       logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , expression_(std::move(expression))
        , limit_(limit) {}

    void operator_match_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        size_t count = 0;
        if (!limit_.check(static_cast<int>(count))) {
            return; //limit = 0
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
            for (size_t i = 0; i < chunk.size(); i++) {
                if (predicate->check(chunk, i)) {
                    for (size_t j = 0; j < chunk.column_count(); j++) {
                        out_chunk.set_value(j, count, chunk.data[j].value(i));
                    }
                    out_chunk.row_ids.data<int64_t>()[count] = chunk.row_ids.data<int64_t>()[i];
                    ++count;
                    if (!limit_.check(static_cast<int>(count))) {
                        out_chunk.set_cardinality(count);
                        return;
                    }
                }
            }
            out_chunk.set_cardinality(count);
        }
    }

} // namespace components::operators
