#pragma once

#include "operator_aggregate.hpp"
#include <components/expressions/key.hpp>

namespace components::compute {
    class function;
}

namespace components::table::operators::aggregate {

    class operator_func_t final : public operator_aggregate_t {
    public:
        explicit operator_func_t(services::collection::context_collection_t* collection,
                                 compute::function*,
                                 std::pmr::vector<expressions::key_t> keys);

    private:
        std::pmr::vector<expressions::key_t> keys_;
        compute::function* func_;

        types::logical_value_t aggregate_impl() override;
        std::string key_impl() const override;
    };

} // namespace components::table::operators::aggregate