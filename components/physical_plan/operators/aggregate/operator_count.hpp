#pragma once

#include "operator_aggregate.hpp"

namespace components::operators::aggregate {

    class operator_count_t final : public operator_aggregate_t {
    public:
        explicit operator_count_t(std::pmr::memory_resource* resource, log_t* log);

    private:
        types::logical_value_t aggregate_impl() override;
        std::string key_impl() const override;
    };

} // namespace components::operators::aggregate