#pragma once

#include "operator_aggregate.hpp"
#include <components/expressions/key.hpp>

namespace components::operators::aggregate {

    class operator_max_t final : public operator_aggregate_t {
    public:
        operator_max_t(std::pmr::memory_resource* resource, log_t* log, collection_full_name_t name,
                       expressions::key_t key);

    private:
        expressions::key_t key_;

        types::logical_value_t aggregate_impl() override;
        std::string key_impl() const override;
    };

} // namespace components::operators::aggregate