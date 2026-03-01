#include "operator_func.hpp"

#include <components/compute/function.hpp>

namespace components::operators::aggregate {

    operator_func_t::operator_func_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     compute::function* func,
                                     std::pmr::vector<expressions::param_storage> args,
                                     bool distinct)
        : operator_aggregate_t(resource, std::move(log))
        , args_(std::move(args))
        , func_(func)
        , distinct_(distinct) {
        assert(func);
    }

    types::logical_value_t operator_func_t::aggregate_impl(pipeline::context_t* pipeline_context) {
        auto result = types::logical_value_t(std::pmr::null_memory_resource(), types::logical_type::NA);
        if (left_ && left_->output()) {
            const auto& chunk = left_->output()->data_chunk();
            using column_it = decltype(vector::data_chunk_t::data)::const_iterator;
            using columns_var = std::variant<column_it, types::logical_value_t>;
            std::pmr::vector<columns_var> columns(left_->output()->resource());
            columns.reserve(args_.size());
            for (const auto& arg : args_) {
                if (std::holds_alternative<expressions::key_t>(arg)) {
                    const auto& key = std::get<expressions::key_t>(arg);
                    auto it = std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& v) {
                        return v.type().alias() == key.as_string();
                    });
                    if (it != chunk.data.end()) {
                        columns.emplace_back(it);
                    } else {
                        break;
                    }
                } else {
                    const auto& id = std::get<core::parameter_id_t>(arg);
                    columns.emplace_back(pipeline_context->parameters.parameters.at(id));
                }
            }
            if (columns.size() == args_.size()) {
                std::pmr::vector<types::complex_logical_type> types(left_->output()->resource());
                types.reserve(columns.size());
                for (const auto& it : columns) {
                    if (std::holds_alternative<column_it>(it)) {
                        types.emplace_back(std::get<column_it>(it)->type());
                    } else {
                        types.emplace_back(std::get<types::logical_value_t>(it).type());
                    }
                }
                vector::data_chunk_t c(left_->output()->resource(), types, chunk.size());
                c.set_cardinality(chunk.size());
                for (size_t i = 0; i < c.column_count(); i++) {
                    if (std::holds_alternative<column_it>(columns.at(i))) {
                        c.data[i].reference(*std::get<column_it>(columns.at(i)));
                    } else {
                        c.data[i].reference(std::get<types::logical_value_t>(columns.at(i)));
                        c.data[i].flatten(vector::indexing_vector_t(left_->output()->resource(), chunk.size()),
                                          chunk.size());
                    }
                }
                if (distinct_ && c.size() > 0) {
                    std::vector<uint64_t> unique_indices;
                    unique_indices.reserve(c.size());
                    for (uint64_t row = 0; row < c.size(); row++) {
                        bool dup = false;
                        for (auto idx : unique_indices) {
                            if (c.data[0].value(row) == c.data[0].value(idx)) {
                                dup = true;
                                break;
                            }
                        }
                        if (!dup)
                            unique_indices.push_back(row);
                    }
                    vector::data_chunk_t unique_c(left_->output()->resource(), types, unique_indices.size());
                    unique_c.set_cardinality(unique_indices.size());
                    for (size_t col = 0; col < c.column_count(); col++) {
                        for (size_t ui = 0; ui < unique_indices.size(); ui++) {
                            unique_c.data[col].set_value(ui, c.data[col].value(unique_indices[ui]));
                        }
                    }
                    c = std::move(unique_c);
                }
                auto res = func_->execute(c, c.size());
                if (res.status() == compute::compute_status::ok()) {
                    result = std::get<std::pmr::vector<types::logical_value_t>>(res.value())[0];
                }
            }
        }
        result.set_alias(func_->name());
        return result;
    }

    std::string operator_func_t::key_impl() const { return func_->name(); }

} // namespace components::operators::aggregate
