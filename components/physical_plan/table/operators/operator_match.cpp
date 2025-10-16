#include "operator_match.hpp"
#include "check_expr.hpp"

namespace components::table::operators {

    operator_match_t::operator_match_t(services::collection::context_collection_t* context,
                                       const expressions::compare_expression_ptr& expression,
                                       logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , expression_(std::move(expression))
        , limit_(limit) {}

    void operator_match_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }
        if (!left_) {
            return;
        }
        if (left_->output()) {
            const auto& chunk = left_->output()->data_chunk();
            auto types = chunk.types();
            std::unordered_map<std::string, size_t> name_index_map;
            for (size_t i = 0; i < types.size(); i++) {
                name_index_map.emplace(types[i].alias(), i);
            }
            output_ = base::operators::make_operator_data(left_->output()->resource(), types, chunk.size());
            auto& out_chunk = output_->data_chunk();
            for (size_t i = 0; i < chunk.size(); i++) {
                if (check_expr_general(expression_, &pipeline_context->parameters, chunk, name_index_map, i)) {
                    for (size_t j = 0; j < chunk.column_count(); j++) {
                        out_chunk.set_value(j, count, chunk.data[j].value(i));
                    }
                    ++count;
                    if (!limit_.check(count)) {
                        out_chunk.set_cardinality(count);
                        return;
                    }
                }
            }
            out_chunk.set_cardinality(count);
        }
    }

} // namespace components::table::operators
