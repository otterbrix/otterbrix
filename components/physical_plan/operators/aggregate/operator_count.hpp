#pragma once

#include "operator_aggregate.hpp"
#include <expressions/key.hpp>

namespace components::operators::aggregate {

    class operator_count_t final : public operator_aggregate_t {
    public:
        explicit operator_count_t(std::pmr::memory_resource* resource, log_t log);
        operator_count_t(std::pmr::memory_resource* resource, log_t log, bool distinct,
                         expressions::key_t field);

    private:
        bool distinct_;
        expressions::key_t field_;

        types::logical_value_t aggregate_impl() override;
        std::string key_impl() const override;
    };

} // namespace components::operators::aggregate