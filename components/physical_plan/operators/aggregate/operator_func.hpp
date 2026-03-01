#pragma once

#include "operator_aggregate.hpp"
#include <components/expressions/expression.hpp>

namespace components::compute {
    class function;
}

namespace components::operators::aggregate {

    class operator_func_t final : public operator_aggregate_t {
    public:
        explicit operator_func_t(std::pmr::memory_resource* resource,
                                 log_t log,
                                 compute::function*,
                                 std::pmr::vector<expressions::param_storage> keys,
                                 bool distinct = false);

    private:
        std::pmr::vector<expressions::param_storage> args_;
        compute::function* func_;
        bool distinct_{false};

        types::logical_value_t aggregate_impl(pipeline::context_t* pipeline_context) override;
        std::string key_impl() const override;
    };

} // namespace components::operators::aggregate