#include "operator_func.hpp"

#include <components/compute/function.hpp>
#include <services/collection/collection.hpp>

namespace components::table::operators::aggregate {

    operator_func_t::operator_func_t(services::collection::context_collection_t* context,
                                     compute::function* func,
                                     expressions::key_t key)
        : operator_aggregate_t(context)
        , key_(std::move(key))
        , func_(func) {}

    types::logical_value_t operator_func_t::aggregate_impl() {
        auto result = types::logical_value_t(nullptr);
        if (left_ && left_->output()) {
            const auto& chunk = left_->output()->data_chunk();
            auto it = std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& v) {
                return v.type().alias() == key_.as_string();
            });
            if (it != chunk.data.end()) {
                vector::data_chunk_t c(left_->output()->resource(), {it->type()}, chunk.size());
                c.set_cardinality(chunk.size());
                c.data[0].reference(*it);
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
