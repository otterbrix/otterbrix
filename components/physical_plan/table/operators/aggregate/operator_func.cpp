#include "operator_func.hpp"

#include <components/compute/function.hpp>
#include <services/collection/collection.hpp>

namespace components::table::operators::aggregate {

    operator_func_t::operator_func_t(services::collection::context_collection_t* context,
                                     compute::function* func,
                                     std::pmr::vector<expressions::key_t> keys)
        : operator_aggregate_t(context)
        , keys_(std::move(keys))
        , func_(func) {}

    types::logical_value_t operator_func_t::aggregate_impl() {
        auto result = types::logical_value_t(nullptr);
        if (left_ && left_->output()) {
            const auto& chunk = left_->output()->data_chunk();
            std::pmr::vector<decltype(vector::data_chunk_t::data)::const_iterator> columns(left_->output()->resource());
            columns.reserve(keys_.size());
            for (const auto& key : keys_) {
                auto it = std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& v) {
                    return v.type().alias() == key.as_string();
                });
                if (it != chunk.data.end()) {
                    columns.emplace_back(it);
                } else {
                    break;
                }
            }
            if (columns.size() == keys_.size()) {
                std::pmr::vector<types::complex_logical_type> types(left_->output()->resource());
                types.reserve(columns.size());
                for (const auto& it : columns) {
                    types.emplace_back(it->type());
                }
                vector::data_chunk_t c(left_->output()->resource(), types, chunk.size());
                c.set_cardinality(chunk.size());
                for (size_t i = 0; i < c.column_count(); i++) {
                    c.data[i].reference(*columns.at(i));
                }
                auto res = func_->execute(c, c.size());
                if (res.status() == compute::compute_status::ok()) {
                    result = res.value().value(0, 0);
                }
            }
        }
        result.set_alias(func_->name());
        return result;
    }

    std::string operator_func_t::key_impl() const { return func_->name(); }

} // namespace components::table::operators::aggregate
